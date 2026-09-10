# Authoritative-gap ownership view: exact settlement, severe stalls remain

Recorded: 2026-09-10. **Not performance-accepted.** The first ordinary view
candidate settles323,158,016B exactly in56.678022s, but maximum confirmation
and local-write gaps remain7.470709and14.915527s. Pre-cut and restored service
remain materially below the preserved `b0` control. Passing the work-count
regression and completing this smaller transfer do not justify promotion or
advancement to unaffected performance gates.

## Mechanism, proof and fixed comparison

The [recovery model](INDEPENDENT_QOS_RECOVERY_MODEL_20260910.md) retains the
authoritative-gap service rule and replaces repeated full-horizon ownership
queries with one transient, exact-instance coverage view per evaluation.
Queries use the current eligible-instance mask. The second scored-quantum
query, assignment-clock updates, target/native admission and final Apply remain
unchanged. No durable index, new timer, quantum, controller or queue limit is
introduced; shared response recovery is not migrated.

The prior [ordinary failure and separate work profile](AUTHORITATIVE_GAP_SERVICE_ORDINARY_20260910.md)
motivated this correction: repeated synchronous model work occupied most of a
long target-flat/reply-held interval. The forecast was to remove a material
portion of that work, not to convert its elapsed total into a promised speed
gain. Remaining scored-prefix queries, allocation/copy cost and native service
were explicitly not eliminated by the view.

The actual evaluator work RED reports5290sweep visits against a2442bound, after
blocked-capacity and released-capacity semantic controls pass. The new candidate
passes101unique focused tests in1.25s, including the work control, five view-oracle
tests and preserved request/response opposite controls. The test compile takes98s;
ordinary release build takes84s with the existing unused-helper warning only.
This proves the tested query/work contract, not latency or completion behavior.

The measured source is the then-uncommitted six-file view change atop `b958179`:
request sender/multipath, request stream/flight, and their flight/client tests.
The frozen ordinary executable is
`./.tmp/reflection/bin/authoritative-gap-view-20260910/mptunnel`. No feature
observer or diagnostic overrides run with this traffic. Source/build/proof logs
use `./.tmp/reflection/authoritative-gap-view-{red,focused,build,qos}-0910.log`.

Fixed result directories under `./.tmp/reflection/results/` are:

- `aggregate-combined-up-native-refill-independent-qos-0910`: preserved `b0`;
- `aggregate-combined-up-authoritative-gap-service-qos-0910`: failed request
  pilot `79ddb41`, not the later timing-only profile;
- `aggregate-combined-up-authoritative-gap-view-qos-0910`: this ordinary view.

The prior [independent-link report/archive](NATIVE_REFILL_INDEPENDENT_20260910.md)
also preserves the healthy single-link47negative control. These are fixed
earlier captures, not a simultaneous or packet-identical paired trial. Complete
different amounts of offered work and retain differing native/allocation histories.

The [ordinary view archive](AUTHORITATIVE_GAP_VIEW_ORDINARY_20260910.raw.tar.gz)
contains exactly11regular files and no directory entries: all five result files,
four RED/focused/build/driver logs, `run.py` and `shape.sh`, preserving relative
paths. Archive size is315,771B. Gzip integrity, member listing, tar comparison
and an independent byte-for-byte comparison of every member against its source
all pass. It contains no binary or source patch; the candidate's source remains
the separately preserved checkpoint/worktree described above.

## Exact outcome and complete confirmation history

| Outcome | Aggregate `b0` | Failed request pilot | Ownership view |
|---|---:|---:|---:|
| Locally accepted bytes | 1,294,925,824 | 442,040,320 | 323,158,016 |
| Confirmed bytes | 1,294,925,824 | 368,664,530 | 323,158,016 |
| Complete / valid exact accounting | 1/1 / yes | 0/1 / no | 1/1 / yes |
| Elapsed, s | 41.538802 | 85.480357, censored | 56.678022 |
| Completed whole Mbps | 249.391 | Not available | 45.613 |
| First write / confirmation, s | .105451 /.409741 | .105411 /.410068 | .108528 /.412616 |
| Maximum confirmation gap, s | .634001 | 26.665989 | 7.470709 |
| Maximum local-write gap, s | .545897 | 3.444041 | 14.915527 |

The view has no probe errors, `status=ok`, accepted=confirmed and valid terminal
accounting. The runner exits0after57.006115s. Its elapsed time includes a
16.678022s tail beyond the nominal40s offered-load window; this is not a new
timeout policy. The duration probe's blocked writes and queued socket work mean
that40s is not an exact last-byte-acceptance boundary. The failed pilot's34.503Mbps
is a censored lower bound, not a completed comparator; this view also confirms
fewer bytes than that failed run. Do not call the whole-rate difference a gain.

All57raw one-second confirmation bins (Mbps) follow, including35zeros. Each row
begins at the printed zero-based second. The final bin is partial and not
renormalized. The trimmed array removes three bins at each end and is not the
wall-clock series. There is no concurrent echo workload in this upload probe.

```text
0:  9.511,217.580,248.417,106.955,57.147,0,0,10.153,0,0
10: 0,0,0,63.203,0,0,61.246,46.994,0,0
20: 347.987,41.341,46.592,40.038,0,0,0,0,0,12.559
30: 0,0,0,0,0,0,0.584,0,473.544,46.093
40: 162.822,0,0,0,0,0,0,0,31.598,0
50: 0,0,0,0,74.183,147.945,338.773
```

| Raw confirmation window | Aggregate `b0`, Mbps | View, Mbps | View zero bins |
|---|---:|---:|---:|
| 0–5s | 262.228 | 127.922 | 0/5 |
| 5–15s, before QoS | 308.068 | 7.336 | 8/10 |
| 15–25s | 34.658 | 58.420 | 4/10 |
| Interior bins16–24 inclusive | 16.203 | 64.911 | 3/9 |
| 25–40s, restored | 328.092 | 35.519 | 11/15 |
| 40–50s | Already settled | 19.442 | 8/10 |
| 50–57s, partial last bin | Already settled | 80.129 | 4/7 |

The cut-phase confirmation gain is real but does not supply sustained healthy47
service; the earlier one-link control maintains185.887Mbps in bins16–24. Bursts
above the instantaneous physical rate can release previously buffered/confirmed
work and are not capacity estimates. Exact gap endpoints are absent, so zero-bin
runs identify coarse holds without inventing a subsecond critical timeline.

## Forward target and reverse confirmation holds both remain

Server management `traffic.total.from_peer_bytes` counts successful target-socket
writes; client `to_peer_bytes` counts source reads. Neither is the probe's sink
confirmation counter. Server `to_peer_bytes` reads sink replies, while client
`from_peer_bytes` writes those replies to the local socket. Management producer
timestamps are separate from probe elapsed; role/socket/class sampling is
sequential. The following rates use counter differences and actual sample elapsed,
not reconstructed confirmation bins.

| Target-socket sample window | `b0`, Mbps | Failed pilot, Mbps | View, Mbps |
|---|---:|---:|---:|
| 0→5s | 273.573 | 69.850 | 206.916 |
| 5→15s | 321.800 | 67.617 | 17.783 |
| 15→25s | 15.642 | 69.308 | 9.753 |
| Strict16→24s | 17.028 | 72.555 | 12.133 |
| 25→40s | 340.910 | 110.545 | 37.919 |
| 40→50s | Settled | Unsettled | 65.350 |
| 50→56s | Settled | Unsettled | 8.794 |

The view target is flat at151,501,236B over samples7–13, before the cut, then
151,566,772B over14–21: only65,536B intervening progress. During restored
samples30–39 it is flat at224,081,468B, while client source reads remain
230,809,532B over25–39. Thus6,728,064B are outstanding at that forward stage;
this is not the previous profile's source=target plateau. Server reply reads
are576B by30; client delivery is478B at30,492at37and576at39. Both forward
ordered service and reverse confirmation holds need preservation.

At55and56the target/source counters both reach323,158,016B. At56the server has
read1051reply bytes and the client has delivered814; exact terminal confirmation
follows before the56.678022s probe closes. This is genuine eventual settlement,
not a continuous useful-service claim. The windows/stages also explain why the
cut-phase confirmation burst is not the same as contemporaneous target progress.

## Matched physical profile, native progress and resource cost

All57view service rows verify the same two independent200Mbps links, with
client eth0/eth1=46/47and server eth1/eth0=46/47. TCP+QUIC carriers share their
own link's cut. Only46UPis10Mbps at samples15–24;47and both return directions
remain200Mbps. First restriction/restoration reporting is15.001634/25.002654s.
DOWN/UPdelay30/70ms, zero jitter/loss/outage, rate=ceil, burst/cburst65536B and
netem limit8192all match. Class/netem drop deltas are0. No capacity or profile
change rescues the trial.

| UP class service, Mbps | `b0`46 /47 | Failed pilot46 /47 | View46 /47 |
|---|---:|---:|---:|
| 0→15s | 185.977 /187.386 | 86.899 /78.957 | 68.051 /68.743 |
| Strict16→24s | 9.991 /31.016 | 9.997 /165.060 | 10.058 /96.718 |
| 25→40s | 185.632 /193.366 | 102.378 /95.117 | 57.456 /28.490 |

All sampled paths are active; eight are present from10s onward. Over the restored
target-flat30→39s, exact native identities/epochs remain unchanged and all eight
producer timestamps advance. TCP ACK bytes add17,993,008on46and21,021,294on47;
QUIC adds45,990and181,116. At39QUIC RTTs are approximately101.96ms, with native
flight0/503,798B and Product flight14,921,640/5,318,978B. These are different
domains: active native delivery does not prove missing-prefix or reply service,
nor do those totals identify necessary or unnecessary copies.

| Whole sampled cost | `b0` | Failed pilot | View |
|---|---:|---:|---:|
| Accounting window, s | 41.011453 | 85.010732 | 56.005962 |
| UP46 /47bytes | 745,256,344 /801,341,232 | 428,663,871 /549,475,737 | 336,022,023 /400,620,890 |
| DOWN46 /47bytes | 15,482,120 /15,889,276 | 7,493,589 /8,344,598 | 5,258,507 /5,264,669 |
| Client RSS peak / final, KiB | 391,104 /385,112 | 370,772 /344,060 | 380,736 /380,736 |
| Server RSS peak / final, KiB | 125,516 /125,516 | 110,296 /110,296 | 106,724 /106,724 |
| Client lifetime CPU peak / final, % | 188 /162 | 125 /111 | 135 /112 |
| Server lifetime CPU peak / final, % | 90.4 /77.8 | 42.8 /18 | 69.3 /19.4 |
| Summed UP backlog max, B | 15,059,114 | 63,217,268 | 27,190,388 |
| Summed UP backlog median over0–40, B | 5,697,518 | 4,052,450 | 858,520 |

The view UP peak occurs at sample40, after restoration. Its final summed UP
backlog is6,831,910B; DOWN backlog maximum/final is47,353/990B. These are sampled
physical queue scopes, not the exact missing byte's position. Lower whole wire
volume or lifetime CPU with substantially less completed work is not an efficiency
win. CPU is the process's lifetime percentage, not critical-region occupancy;
there is no evaluator timing observer in this ordinary capture.

Client log/probe stderr are empty. The server log contains two
`ApplicationClose: H3_NO_ERROR` warnings at10:16:16.696UTC after settlement during
teardown, not two failed transfers. There are no hidden probe errors or censored
bytes in this candidate; the earlier pilot's incomplete result remains separate.

## Disposition

The query-view contract passes its actual work and equivalence tests, but the
ordinary practical forecast is not satisfied: exact settlement coexists with
severe pre-cut/restored shortfalls and a new longer observed local-write gap.
The view has not established that remaining model queries are cheap, and native
progress alone does not attribute the unresolved ordered/confirmation holds.
Preserve this result and choose the next exact owner/work discriminator before
another runtime or acceptance attempt. No favorable rerun, parameter change,
public performance update or release is supported by this capture.

## Separate stage profile: residual work does not dominate every held interval

The subsequent timing-only capture
`aggregate-combined-up-authoritative-gap-view-profile-0910` uses ordinary source
`a747bda` with the saved two-file temporary overlay
`./.tmp/reflection/authoritative-gap-view-profile-0910.patch`. Root freezes
`./.tmp/reflection/bin/authoritative-gap-view-profile-20260910/mptunnel` after
the83s feature build, then fully reverses the overlay before traffic.
`MPTUNNEL_LAB_PERF=1`, per-call samples off. No decisions, profile or resource
parameters change. This diagnostic is not another ordinary speed comparison.

The [stage-profile archive](AUTHORITATIVE_GAP_VIEW_PROFILE_20260910.raw.tar.gz)
contains11regular files, no directory entries: five results, build/driver logs,
the two-file overlay patch, wrapper, `run.py` and `shape.sh`. Its size is793,634B.
Gzip integrity, listing, tar comparison and independent member-by-member byte
comparison against source files all pass. Relative paths are preserved; no
binary or credentials are included.

### Incomplete outcome and own progress timeline

The unchanged85s guard ends an incomplete transfer:276,216,058B confirmed of
312,672,256B locally accepted in85.513042s, leaving36,456,198B unconfirmed.
`complete=false`,0/1streams complete, invalid exact accounting and lower-bound
status. Reported25.841Mbps is **censored confirmed throughput**, not completed
goodput. First local write/confirmation is.105895/.410683s; maximum gaps are
1.395808s local-write and6.851551s confirmation. The driver exits1with
`probe failed to settle`; teardown produces the terminal
`upload sink closed before terminal acknowledgement` error. It is not a separately
demonstrated spontaneous connection failure. Raw/trimmed confirmation bins are
empty and maximum-gap endpoints absent; none are reconstructed from other domains.

This run differs from the preceding ordinary view and earlier profile, so use
its own state to interpret timings:

- Target-socket acceptance is exactly149,532,592B over samples16–27. Source
  reads grow154,370,256→174,931,024B, leaving4,837,664→25,398,432B not yet
  accepted at the target socket. This difference is outstanding stage volume,
  not the exact extent of one missing DSN. Server reply reads remain557B;
  client reply writes advance252→347B. Forward and reverse progress diverge.
- Over48→57, target writes continue266,938,032→276,347,130B,8.363Mbps, while
  source remains312,672,256B. Over58→68, target adds34,773,662B,27.815Mbps.
  This middle settlement segment is slow but not target-flat.
- At68the target and source both equal312,672,256B and remain equal through85.
  Server has already read2068sink-reply bytes. Client reply delivery advances
  only1229→1607B, and the probe remains36,456,198B short of terminal confirmation.
  The final target-flat state is therefore a held-confirmation interval, not
  proof that upload bytes still need to reach the target socket.

The86sampled rows end85.009199s. Their management producer clocks and the
probe's elapsed clock are separate; the coarse progress joins do not reconstruct
the exact user-visible maximum-gap interval or a blocking reply's byte identity.

### Nested stage totals, not additive CPU or a new bottleneck assumption

All56rows for each of11new labels reconcile count/query/time deltas with
cumulative totals:616rows, one client PID. Every label has50,268completed
outer evaluations; generic `bytes` means queries, not traffic. The table reports
elapsed time summed within those evaluations. Maxima are per-evaluation aggregate
stage times, **not per-query maxima**.

| Region after `request.gap_service` | Queries | Aggregate elapsed, s | Maximum per evaluation, ms |
|---|---:|---:|---:|
| Outer service | 9,349,268 model queries | 15.420367 | 9.664 |
| `.owner_model` | 9,349,268 | 11.953549 | 9.420 |
| `.owner_mask` | 9,349,268 | 3.644743 | 4.422 |
| `.ownership_frontier` | 9,349,268 | .817712 | 2.302 |
| `.scored_frontier` | 484,387 | .898272 | 4.277 |
| `.cache_frames` | 484,387 | .257351 | .903 |
| `.lower_model` | 484,387 | 5.356034 | 6.365 |
| `.lower_owner_lookup` | 484,387 | 1.154682 | 3.520 |
| `.lower_native_observe` | 484,387 | 3.012307 | 4.811 |
| `.lower_target_selection` | 484,387 | 1.130411 | 4.251 |
| `.assignment_clock` | 195,739 | .363050 | 1.062 |

Owner mask/frontiers/cache/lower-model are inside owner-model; lower lookup,
native observation and target selection are nested inside lower-model. Assignment
clock is outside owner-model but inside service. Do not sum those overlapping
rows. Each recorder floors elapsed to1us, even when the stage has zero queries.
Timers include instrumentation/descheduling and synchronous lock-held elapsed,
not CPU instruction time, lock acquisition or another task's blocked duration.
View construction and other surrounding service work are included only in outer
time. The final new-label record is Unix1789041307036ms; other counters continue
through1789041336203ms. Without a clean terminal flush, totals cover completed
recorded evaluations, not a categorical measurement of every unfinished action.

Actual cumulative flush stamps give materially different occupancy by phase.
`interval_ms=1000` is nominal configuration, not actual spacing. The following
Unix millisecond stamps are exact logged boundaries inside the stated management
windows, not shared process-monotonic or probe origins:

| Own management window | Flush stamps / outer sequence | Wall s | Completed evaluations | Outer / owner-model elapsed, s |
|---|---|---:|---:|---:|
| 5–15s | 1789041255832→1789041264882 /112→318 | 9.050 | 7,941 | 2.021161 /1.502353 |
| Target-flat16–27s | 1789041266892→1789041276930 /364→594 | 10.038 | 4,081 | 2.102305 /1.447980 |
| Restored25–40s | 1789041275929→1789041289956 /569→914 | 14.027 | 19,607 | 4.101653 /2.954955 |
| Target-flat42–45s | 1789041292970→1789041294975 /985→1027 | 2.005 | 525 | 1.021838 /.928556 |
| Slow drain48–57s | 1789041298986→1789041307036 /1116→1289 | 8.050 | 1,896 | .170459 /.108921 |

The windows overlap and must not be added. Sequential counter recording can
split one evaluation across a flush: nested counts/query deltas occasionally
differ at boundaries, although final totals agree. In16–27s, outer occupancy
is20.94%, not the earlier profile's84.48%regime. The3,222,829nested model queries
spend.934869s in owner eligibility and.211218s in view membership. **None reaches
scored-frontier, cache, lower-model or assignment-clock service** in that window;
each such stage's displayed.004081s is exactly4081×1us recording floor, not
executed queries. The source returns before scored service when the view has
no frontier or not exactly one eligible owner; these counters do not distinguish
which condition occurred for each exact range. Mere lower-model optimization
cannot remove work that this held interval never executes.

The short42–45s plateau does contain substantial model work:1.021838s outer
in2.005s wall, including.518116s lower-model, .251959s nested native observation,
.214746s owner-mask and.107441s scored-frontier. But48–57outer falls to.170459s
over8.050s while completion is still slow. There is no single remaining stage
whose aggregate explains every critical interval.

During the final68–85confirmation-held state, no new completed evaluator rows
appear. Other owner operations do continue: exact flush bounds
1789041319129→1789041335200contain181mux ACK applications releasing9,056,864B,
and51receive/local-write operations. Local writes deliver336B in1.610ms aggregate,
maximum238us. This contradicts an uninterrupted request-Product lock hold across
that entire tail; it does not rule out all intermittent contention. TCP reader
queue-send/stream-route elapsed totals in that same16.071s band are96.562/95.948s
across concurrent carriers, with maxima131.033/130.862ms. Those are asynchronous
residences/backpressure, not additive CPU or an exact reply's critical delay.

### Same physical profile; different native utilization and collection cost

All86rows retain two independent200Mbps links and the same46UP10Mbps restriction
at15–25s;47and returns remain200Mbps, DOWN/UP30/70ms, zero loss/jitter/outage,
burst/cburst65536B and netem limit8192. Class/netem drop deltas are0. First
restriction/restoration reporting occurs15.001617/25.002677s. These samples verify
configuration, not identical native histories or timing-neutral instrumentation.

The important negative result is physical underuse of healthy47 during the own
target-flat cut: strict16→24 UP classes average9.996Mbps on46but only.034584Mbps
on47. Native TCP ACKs advance6,151,690/15,272B and QUIC15,760/15,714B, with
unchanged exact epochs and fresh producer stamps. All paths are sampled active,
eight from9s onward. At27healthy47QUIC RTT is106.882ms, native flight0but Product
flight36,193,814B. Active/native labels do not supply the missing logical owner
or prove capacity-admissible recovery for the blocked range.

Later68→85, TCP ACKs advance66,358,148/45,306,642B on46/47and QUIC99,667/130,803B,
with stable exact identities and advancing producer stamps. UP classes average
32.410/22.595Mbps despite no new target acceptance. These bytes are not unique
useful payload; no repair-winner or duplication claim follows from totals.

| Own whole sampled cost | Client UP46 /47 | Server DOWN46 /47 |
|---|---:|---:|
| Class bytes over85.009199s | 635,918,102 /421,040,618 | 9,362,042 /7,779,521 |
| Summed backlog median / maximum / final, B | 547,516 /16,221,486 /355,838 | 660 /52,593 /544 |
| Process RSS peak / final, KiB | 344,744 /334,684 | 65,336 /65,336 |
| Lifetime CPU peak / final, % | 117 /109 | 55.3 /19.6 |

CPU/RSS do not locate a critical lock or prove a leak; substantially different
work/completion and observation windows forbid a cost-efficiency comparison.
Client/server logs total1,140,410B (1660/1044lines), no per-call samples, no
product warning/error lines and empty probe stderr. The instrumented run changes
execution overhead and ends censored; it cannot replace the ordinary57bin result.

**Information disposition:** view-specific remaining synchronous work is real,
but its whole15.420s total does not justify assuming it dominates the remaining
failure. The long cut plateau exits before scored service, and the late
confirmation-only tail continues after completed evaluator activity stops.
Preserve the held prefix/eligible-owner/reply-service question before selecting
another equivalent-work correction. No timer or throughput-only rescue,
performance promotion, public update or release follows this diagnostic.

## Separate exact events: request staleness and a delayed healthy-link reply

Capture `aggregate-combined-up-authoritative-gap-view-events-0910` uses ordinary
`a747bda` with a temporary22-line, two-file observer. The saved overlay is
`./.tmp/reflection/authoritative-gap-view-events-0910.patch`; it adds exact
prepared-response Original identity, every response arrival/frontier and gap
context on existing request-stale transitions. Root builds82s, freezes
`authoritative-gap-view-events-20260910`, and fully reverses the overlay before
traffic. The preceding88s timing-free feature build had no traffic. Perf is off;
diagnostics filter stream0and15event kinds, not bulk upload DATA/ACK logs.

The [exact-event archive](AUTHORITATIVE_GAP_VIEW_EVENTS_20260910.raw.tar.gz)
contains11regular files, no directory entries: five results, base/joined build
and driver logs, the two-file overlay patch, `run.py` and `shape.sh`. Relative
paths are preserved. Gzip integrity, listing, tar comparison and all-member
byte comparison against sources pass; no binary or configuration keys are included.

This event capture settles287,113,216B exactly in59.641606s,38.512Mbps,
1/1complete, no probe errors. First write/confirmation is.107010/.414907s;
maximum local-write/confirmation gaps are4.107995/10.716683s. These are this
diagnostic's outcomes, not an improved ordinary comparison. All60raw one-second
confirmation bins follow (36zeros); the final partial bin is not renormalized:

```text
0:  8.271,99.942,346.318,50.236,0,0,35.649,202.045,0,0
10: 0,26.835,0,97.754,331.164,62.244,206.384,19.443,27.589,0
20: 0,0,0,0,0,0,0,0,0,37.845
30: 0,0,67.211,0,0,0,0,59.813,319.675,0
40: 0,0,0.428,0.524,0,0,0,0,0,0
50: 0,0.589,0,0,20.050,0.953,3.293,0,0,272.649
```

Means over5–15/interior16–24inclusive/25–40/40–60s are
69.345/28.157/32.303/14.924Mbps. Exact completion does not erase the holds.

### Request direction: stale transitions precede the imposed cut

The eight recorded transitions are per-request exact-output decisions, not
carrier failure or proof of native loss. `gap_owners` is the candidate-owner
set across all retained authoritative gaps, not necessarily ownership of only
the printed first gap. Times below are the client's diagnostic monotonic clock;
its origin is Unix1789041835416ms, not the probe start.

| Client time, s / sequence | Marked output | First gap at decision |
|---|---|---|
| 3.372 /19 | QUIC47, physical1, attachment3 | [64,513,653,64,632,725) |
| 4.427 /20 | QUIC46, physical8, attachment2 | [64,775,797,65,615,765) |
| 9.065 /42 | QUIC46, physical8, attachment2 | [96,150,005,96,215,541) |
| 10.719 /43 | QUIC47, physical1, attachment3 | [96,162,005,96,215,541) |
| 11.196 /45 | TCP47 index1, physical4, attachment1 | [96,164,605,96,215,541) |
| 55.142 /231 | QUIC47, physical1, attachment3 | [252,966,581,252,972,117) |
| 56.558 /233 | QUIC46, physical8, attachment2 | [253,539,691,253,670,763) |
| 58.387 /234 | TCP47 index1, physical4, attachment1 | [254,578,989,254,739,589) |

The first five are before46UPQoS. Moreover server management at
1789041845987ms already reports132,027,733B accepted by the ordered raw-upload
target socket, preceding the96.16MB stale decisions at1846136/1846612ms
(same178904prefix). Thus those decisions can concern bytes already beyond the
target-write frontier. This supplies concrete lagging Product-receipt context,
not proof of forward packet loss or, alone, an incorrect stale timer. It does not
identify the precise missing ACK's publication/native/consumer stage. Repeated
stale events must not be interpreted as continuous exclusion without intervening
eligibility evidence; no response-stale event appears in this selected capture.

### Response direction: complete identity/range accounting

One session,2919368028768285897, owns stream0. All88prepared Original ranges
exactly partition[0,1202), with no gap or overlap. There are157accepted repairs:
141tail and16completion-tail,2172payload bytes. Client receives235frames/3236B.
Every receipt maps to exactly one preceding accepted Original or repair with
the same extent and mapped output; there are no ambiguous or unmatched receipts.
All88Originals and147repairs arrive;10accepted repairs/138B have no recorded
receipt before closure, not necessarily a loss. Unique/duplicate input is
1202/2034B. First receipts are671BOriginal (648QUIC,23TCP) and531BQUIC repairs;
no TCP repair wins a first receipt in this capture.

Cross-role IDs are mapped, never presumed interchangeable. Unique startup
ranges and the remaining exact joins establish these four observed output pairs:

| Server underlay / wire / physical / incarnation | Client underlay / index / physical / attachment |
|---|---|
| TCP /2 /5 /1 | TCP /0 /6 /0 |
| TCP /5 /3 /2 | TCP /1 /4 /1 |
| QUIC /0 /8 /3 | QUIC /0 /8 /2 |
| QUIC /1 /1 /4 | QUIC /1 /1 /3 |

For example[70,83)has its only TCP acceptance on server wire5and its TCP
receipt on client index1; its separate QUIC repair has its own later QUIC
receipt. The QUIC1pair is configured physical link47, not impaired46.
Four used response outputs do not prove that every other output is ineligible.

### Exact longest hold: [699,713) on healthy47

Unix times below are milliseconds after1789041800000. Acceptance is Product/
carrier queue admission, **not measured native write completion**.

| Time / role sequence | Recorded event |
|---|---|
| 52602 /server255 | Original[699,713)accepted on QUIC1/physical1/incarnation4 |
| 53910 /client120 | Previous receipt advances frontier685→699 |
| 54337 /server269 | Same-range repair dispatched on QUIC0, queue delay0ms |
| 62192 /server290,292 | Accepted-copy wake1181us late; repair dispatched TCP5, queue delay0ms |
| 62393 /server297,299 | Accepted-copy wake1089us late; repair dispatched TCP2, queue delay0ms |
| 64627 /client122 | QUIC1Original receipt advances699→713, reorder0 |
| 66969 /client126 | QUIC0copy arrives, all14B duplicate |
| 89914 /client152 | TCP2mapped copy arrives, all14B duplicate |
| 89917 /client161 | TCP5mapped copy arrives, all14B duplicate |

The frontier hold is10,717ms, matching the probe's10.716683s maximum to the
logs' millisecond precision. Its winning Original was accepted12,025ms before
receipt, already1308ms before the hold began. No same-range repair was accepted
on QUIC1, so the winner cannot be confused with those alternate copies.
The first repair precedes release by10,290ms; TCP repairs precede it by2435and
2234ms. Their eventual acceptance-to-receipt delays are27,725/27,521ms on TCP5/2.
The observation therefore localizes this hold after already accepted work;
neither late server queue dispatch nor absence of a repair alone explains it.
It does not yet distinguish active writer, native transport, client reader/
mailbox or logical-owner service inside that post-acceptance interval.

The native/physical evidence must retain its field semantics. During the strict
held interior1789041854987→1789041863987ms (management20→29), healthy47DOWN
class sends92,654B, maximum sampled backlog2009B, zero drops, at200Mbps.
47UPsends16,357,914B with maximum sampled backlog598,166B;47is never QoSed.
Server QUIC1`native_delivery` advances527,680→542,446ACKed bytes, with the same
epoch and producer stamp20,757,825→29,709,416us. These aggregate ACKs do not
identify the14critical bytes. Server displayed RTT308.569ms/flight3937B/queue0
is explicitly `local_sender` projection, unchanged and not fresh packet-level
RTT/flight evidence. Client QUIC47native RTT recovers to approximately100–107ms
while the reply is still held. No exact native sent-byte/write-handoff event is
present. Direct attribution of12seconds to the46UPcut or sampled47DOWNqueue
is unsupported; a hidden earlier queue position is not reconstructed either.

### Physical verification, cost and next attribution boundary

All60management rows verify the unchanged two200Mbps topology,46UP10Mbps only
at15–25s,30/70ms delays, zero jitter/loss/outage, burst/cburst65536B and netem
limit8192; class/netem drops remain0. First restriction/restoration reports are
15.007231/25.008314s. Whole accounting spans59.012116s, not exact probe duration.
UP46/47class bytes are266,660,235/335,903,733; DOWN46/47are3,326,411/3,781,107.
Summed UP/DOWN backlog maxima are22,072,946/43,156B. Client RSS peak/final is
292,888/291,912KiB and lifetime CPU117/108%; server95,768/95,768KiB and52/14.2%.
These whole-process snapshots are not critical-stage CPU or leakage measurements.

Logs contain786lines/244,333B (client248/70,158B, server538/174,175B), with no
bulk upload-frame or ACK event flood. The complete ordinary outcomes and the
earlier stage profile remain separate. The useful next boundary is exact
post-acceptance native write/read/owner service for the held reply, alongside
the independently identified request receipt/eligibility question—not another
timer or copy-budget adjustment. No performance promotion follows this capture.

## Separate boundary capture: material local reply delay, not one universal cause

The next diagnostic uses ordinary `a747bda` plus the frozen eight-file
`./.tmp/reflection/authoritative-gap-view-boundary-0910.patch`. Its feature build
completed in92s; the complete temporary overlay was reversed before traffic.
The frozen `authoritative-gap-view-boundary-20260910` executable, not a modified
ordinary control, produced
`./.tmp/reflection/results/aggregate-combined-up-authoritative-gap-view-boundary-0910/`.
The [boundary raw archive](AUTHORITATIVE_GAP_VIEW_BOUNDARY_20260910.raw.tar.gz)
contains10regular files: five results, build/driver logs, the eight-source-file
overlay patch, and unchanged `run.py`/`shape.sh`. Its308,147B gzip passes integrity,
tar comparison and every member's byte comparison; no binary or credentials are
included. The two driver files also match their previous event-capture archive.
No PERF or bulk upload-frame logging was enabled. The information question was
whether an already committed reply waits before native submission, during the
write future, before decoding, or after the decoded frame enters the reader
queue. This observation changes no scheduler, deadline or recovery policy.

### Own complete outcome and accounting

Runner34271 exits0 after48.006s. The probe exactly confirms all489,160,704B
locally accepted in47.360233s (82.628Mbps), one complete stream, no probe errors.
First write/confirmation is.105386/.410646s; maximum write/confirmation gap is
4.759941/5.239099s. This remains poor practical service, not a performance gain
over another diagnostic. There are48raw confirmation bins, including16zeros;
the probe's separately trimmed vector is not a wall-clock series. Raw means
for5–15,16–24inclusive,25–40 and40–48 are74.031,11.926,119.106 and103.299Mbps.
The final partial bin is not rescaled. This upload has no echo workload.

```text
raw bin start (s): receiver-confirmed Mbps
 0: 9.259,116.682,61.105,0,173.776,86.463,0,0,92.088,0
10: 149.180,115.248,14.872,48.707,233.753,91.837,38.222,25.890,3.582,8.189
20: 2.333,4.770,21.156,3.190,0,0,0,0,244.134,536.871
30: 0,380.589,196.844,0,2.834,0,0,401.200,24.117,0
40: 0,25.594,0,0,46.233,391.255,237.136,126.177
```

Session8196219730563367013/stream0 is stable in all48management rows.
142Original extents partition[0,1959) without gaps/overlaps. There are252accepted
repairs/3490B:237tail,13stale-path, one persistent-gap and one completion-tail
operation. All392received extents have exactly one preceding acceptance with
the same extent and mapped carrier:1959unique/3462duplicate B. Two accepted
14B copies have no receipt by closure, not proof of loss. One uses a newly
attached TCP4/incarnation5; reply membership is therefore not claimed constant.
First receipts are1015B from74QUIC Originals,139B from10TCP Originals,791B from
57QUIC copies and14B from one TCP copy. Five of those QUIC Original first
receipts buffer behind an older hole; the other69 advance the frontier.

Cross-role output mapping is established by unique extents, not equal integers:
server TCP3/physical4/incarnation1 maps to client TCP0/physical3/attachment0;
TCP2/physical5/incarnation2 maps to TCP1/physical5/attachment1. Server
QUIC0/physical1/incarnation4 maps to client QUIC0/physical1/attachment2;
QUIC1/physical8/incarnation3 maps to QUIC1/physical8/attachment3. QUIC1 is
healthy physical47, while only46UP is temporarily restricted.

### Exact stage evidence and the coverage boundary

All128ordinary-channel QUIC Originals join uniquely through acceptance,
write-begin, successful write-end, decoded frame, successful reader enqueue and
Product receipt. All writes use the interlocked route; none reports an error.
The write-end is successful completion of the existing batch write helper,
not a physical transmission timestamp. The decoder event follows frame decoding;
reader-queue follows successful channel send. Its downstream interval includes
carrier-actor routing, Product-owner service and scheduling, not an identified
single lock or actor. Role-local `t_mono_ms` origins are never subtracted.

| Stage, ms | First-winning QUIC Originals n74: minimum / median / p95 / maximum |
|---|---|
| Acceptance → write-begin | 0 /0 /1 /2 |
| Write-begin → write-end | 0 /0 /1 /2 |
| Write-end → decoded | 30 /889 /3846 /4489 |
| Decoded → reader-queue | 0 /0 /8 /11 |
| Reader-queue → Product receipt | 0 /505 /1854 /4391 |

These are per-frame first-winner observations, not independent samples of all
user gaps or unbiased native latency. Percentiles select the sorted index
`round((n-1)*rank)`. The predecode interval includes native transport AND client
reader scheduling/decoding; it is not exclusively network delay.

The decisive local example is[187,200) on QUIC1. Unix timestamps below are
milliseconds after1789042500000; sequences are local to their logged role.

| Time / sequence | Actual event |
|---|---|
| 12741 /client58 | Prior receipt advances frontier to187 |
| 12804 /server102,103,104 | Original accepted; write-begin and successful write-end |
| 12892 /client59,60 | Decoded and successfully enqueued to ordinary reader channel |
| 15564 /client69 | Same Original advances frontier187→200 |

The2,823ms advancing gap contains2,672ms AFTER successful reader enqueue,
versus88ms from write-end to decode. Thus a material local downstream service
delay is directly measured; a server Pending write or late native arrival
cannot explain this particular2,672ms interval. The trace does not distinguish
carrier-actor service from Product-actor service, or prove a proposed fairness
rule removes all of it. The same distinction applies to[213,226) on QUIC0:
accept/write-begin13481, write-end13483, decode/enqueue13515, Product17906.
Its4391ms local residence overlaps earlier prefix delays; only1687ms follows
the preceding frontier advance. These residences cannot be added as a gain.

Conversely, first-winning[448,462) has4489ms write-end→decode and only155ms
enqueue→Product. The longest observed ordinary Original residence is9986ms
for[1554,1568) on QUIC0, but that arrival is a losing duplicate. It is not the
critical user delay. A local-only explanation for every reply is falsified.

The absolute longest advancing gap,5,239ms, remains differently scoped:
frontier1218 at1789042530791 advances to1232 at1789042536030. TCP2Original is
accepted32478; the winning QUIC1copy is accepted32758 (Unix suffix within
1789042500000), giving3272ms accepted-copy→receipt. TCP3copy and QUIC0copy
are accepted33798/35358 and arrive later. There is NO write/decode/reader-queue
event for that winning repair. Source `quic/repair.rs` writes and reads its
separate native repair stream directly, bypassing the instrumented ordinary
writer and `spawn_quic_path_reader`. Its successful route feeds the shared
Product input. Do not assign this5.239s gap to any unobserved boundary or infer
that the repair traversed the same ordinary-channel wait.

### Matched physical scope, costs and bounded disposition

All48rows verify two200Mbps links, only46UP10Mbps during15–25s,30/70ms delays,
zero configured loss/jitter/blackhole, burst/cburst65536B and netem limit8192.
First restriction/restoration reports are15.002060/25.003329s. Every class and
netem drop counter stays0. Whole sampled accounting spans47.005804s, not the
probe's complete settlement window. UP46/47 class deltas are469,424,872/
533,166,725B; DOWN46/47 are6,009,269/5,992,313B. Summed UP/DOWN backlog maxima
are26,255,619/30,746B. Client RSS peak/final is347,620/328,440KiB with lifetime
CPU121/120%; server138,544/138,544KiB with52.7/36.3%. These are not stage CPU
or post-teardown retention measurements.

During the exact local[187,200) hold, management samples at1789042513231–
1789042515231 show server target writes71,353,132→104,683,234B and fresh
QUIC1ACK-counter progress131,156→250,208B. Client local reply writes remain
187B at the corresponding three samples. This corroborates continuing other
work while the decoded reply waits, without identifying the responsible actor.
Server displayed RTT/flight here is `local_sender` projection; independently
advancing native counters must not turn those projections into fresh native
latency measurements. Queue snapshots do not supersede the exact decode event.

Logs contain2045lines/656,048B. The two server H3_NO_ERROR warnings occur during
client shutdown; probe stderr is empty and exact completion succeeds. Nine
request-stale transitions and one late response-stale transition are recorded,
not a new independent defect inventory. The practical decision is to investigate
the now-proven local post-enqueue service premise with a real caller control,
while preserving predecode delay and uninstrumented repair-route uncertainty.
This diagnostic neither promotes the request pilot nor justifies timer tuning.

## Separate local-stage capture: shared-input service and propagated backpressure

The next same-profile diagnostic extends the selected-reply observer at carrier
routing, attachment forwarding and Product dequeue/pre-apply boundaries. The
complete13-source-file temporary overlay is
`./.tmp/reflection/authoritative-gap-view-local-stage-0910.patch`; ordinary
`a747bda` policy is unchanged. Build73093 completes in3m37s with only the existing
unused-helper warning. The frozen `authoritative-gap-view-local-stage-20260910`
executable is used after the complete overlay is reversed, without overlapping
compilation. The [local-stage raw archive](AUTHORITATIVE_GAP_VIEW_LOCAL_STAGE_20260910.raw.tar.gz)
retains10regular files: five results, build/driver logs, complete overlay and
unchanged `run.py`/`shape.sh`. Its925,269B gzip passes integrity, tar comparison
and every member's byte comparison. There is no binary or credential material.

### Failed completion includes a censored26-second reply stall

Runner60654 exits1 at the existing85s settlement guard. The final probe reports
479,504,771B confirmed of547,815,424B locally accepted in85.400254s:68,310,653B
remain unconfirmed. Its close-before-terminal error follows runner cleanup,
not a separately observed native disconnect. This is **not** an exact44.918Mbps
completed result. First write/confirmation is.105463/.412543s; reported maximum
write/confirmation gap is1.303973/4.569714s. Both raw/trimmed confirmation-bin
arrays are empty because accounting is incomplete; no bins are reconstructed
from target I/O. There is no echo workload.

The reported maximum excludes an unresolved final silence. Client local reply
writes stay1663B at every management sample from1789045579469 through
1789045605469:26.000s without progress. The last advancing Product event was
1789045579360,26.109s before that final sample; later received copies do not
advance the frontier. During the same sampled26s, server target-socket writes
continue524,173,811→547,815,424B, reaching all locally accepted bytes. These are
successful socket writes, **not** proof the sink consumed/confirmed every byte.
Server reads from the sink's response stream remain2139B, already observed at
the earlier sample. The retained response prefix remains a demonstrated problem
even while forward target service continues.

Sampled target-socket service5→15,16→24 and25→40 is51.643,81.601 and78.584Mbps;
these are coarse I/O-counter slopes, not confirmed timing bins or an ordinary
candidate comparison. No exact-settlement acceptance follows this observation.

### Exact local owner split on winning frames

Session16038167878315865865/stream0 has274applied frames, each uniquely matched
to one preceding accepted extent and mapped carrier.143Original extents
partition[0,2139);364repair admissions contain5899B. Received input is
3768B=1747unique+2021duplicate;1663B becomes ordered and84B remains buffered.
Among127first-received extents,78win on QUIC Original,38on QUIC repair,9on TCP
Original and2on TCP repair. First-winning buffered arrivals are included in the
stage population, not mislabelled as independently advancing user gaps.

All130ordinary QUIC Originals have successful write-begin/end records, maximum
write-helper duration1ms.115reach decoded/reader-queue events;112reach normal
carrier handling/enqueue.72repair frames have decoded/send and enqueue records.
No normal/deferred/interlock attempt is silently substituted for another: all
observed ordinary handling is `quic_normal_handle`; no interlock attempt or
Full/closed outcome is emitted.274frames have dequeue/selected/pre-apply/apply
events,277have attachment-receive and276have successful shared-send events.
One received attachment frame remains pending shared send; two shared-enqueued
frames remain unapplied. There are no repeated extent/physical/phase keys in
these joins, and no negative post-send→dequeue interval in this capture.

| Local interval, ms | First-winning sample count | Median / p95 / maximum |
|---|---:|---|
| Ordinary reader-queue → carrier handle | 78 | 16 /2662 /3229 |
| Ordinary carrier handle → enqueue | 78 | 0 /52 /92 |
| Ordinary carrier enqueue → attachment receive | 78 | 37 /2007 /6426 |
| Repair decoded/send-begin → enqueue | 38 | 2 /117 /192 |
| Repair enqueue → attachment receive | 38 | 582 /3610 /3783 |
| Attachment receive → shared-input send completion | 127 | 3 /45 /328 |
| Shared-input send completion → Product dequeue | 127 | 103 /1825 /3474 |
| Product dequeue → selected | 127 | 0 /0 /1 |
| Selected → pre-apply geometry/lock finished | 127 | 0 /1 /1 |
| Pre-apply finished → applied receipt | 127 | 0 /0 /1 |

The next table uses Unix milliseconds after1789045500000. Both frames are
unique Original winners; QUIC0 is46 and QUIC1 is47. Client QUIC0physical8/
attachment3 maps to server QUIC0physical8/incarnation4; client QUIC1physical1/
attachment2 maps to server QUIC1physical1/incarnation3. Cross-role IDs are
mapped rather than assumed interchangeable; only Unix time is compared.

| Actual boundary | [1229,1243), QUIC0 | [1355,1369), QUIC1 |
|---|---:|---:|
| Prior frontier advance | 52574 | 60781 |
| Server acceptance/write-begin/write-end | 51438 | 58009 |
| Decoded / reader enqueue | 51475 /51475 | 60751 /60751 |
| Carrier handle / enqueue | 51630 /51630 | 61847 /61932 |
| Attachment receive / shared send complete | 53247 /53555 | 64940 /64980 |
| Product dequeue / selected / pre-apply / receipt | 57029 /57029 /57029 /57029 | 65351 /65351 /65351 /65351 |

For[1229,1243),3474ms AFTER successful shared admission lies inside its4455ms
advancing gap. That frame had left carrier handling long beforehand; a
carrier-only priority fix cannot explain this interval. For the maximum
completed4570ms gap[1355,1369), the local split is1096ms to carrier handling,
85ms to carrier enqueue,3008ms to attachment receive,40ms shared send and371ms
shared-input residence. Those4600ms start30ms before the prior frontier advance;
they are not an additional4570ms gap. All selected→apply work fits the same millisecond.
Neither gap is materially explained by the measured per-frame pre-apply lookup.
The earlier predecode intervals remain37ms and2742ms respectively; those precede
the displayed frontier gaps and must not be added as independent gain.

The shared input has capacity131 in this configuration.234/277attachment
arrivals observe131occupied/reserved slots;234/276post-send observations do too.
220/274Product dequeues leave129or130visible FIFO items. The successful send
stamp can trail a concurrent consumer; occupied/reserved capacity is not
identical to FIFO length. Nevertheless the positive3474ms post-send interval
and actual queued items establish a material shared-input service deficit.
Late attachment receive can result from that forwarder's earlier blocked send,
not only from carrier-loop starvation. The observations support propagated
backpressure but do not time each preceding ACK/control handler or prove one
continuous lock hold. Per-frame elapsed intervals overlap and are not CPU sums.

### The final blocking[1663,1677) remains explicitly censored

Server QUIC0acceptance/write-begin/write-end occurs at1789045564777. Client
decode is1789045592532:27,755ms later; then reader enqueue completes.
normal handle/enqueue at1789045601649 is8940ms after reader enqueue
1789045592709. Attachment receive finally appears at1789045605879,4230ms
after carrier enqueue, with131slots occupied/reserved.
There is no successful shared-send, dequeue or applied receipt for this head
before cleanup. Five TCP copies admitted1789045577265–1789045578072 also
have no head receipt.

This unfinished head contains both a long predecode interval and subsequent
local queue residence. It must not be assigned a successful completion time,
folded into the completed-gap percentile, or attributed solely to carrier
priority, a missing repair or the physical46cut. The cut was restored long
before this final tail. Native ACK counters on both QUIC carriers keep advancing
with producer stamps and stable epochs; they do not identify the critical
14bytes. Server displayed RTT/flight remains `local_sender` projection, not
fresh native latency evidence for that head.

### Physical/cost context and next decision

All86profiles match the prior two200Mbps setup: only46UP10Mbps during15–25s,
30/70ms delays, zero loss/jitter/outage, burst/cburst65536B and netem8192.
First restriction/restoration reports are15.001981/25.003067s. All class/netem
drop counters remain0. Whole sampled accounting spans85.011966s: UP46/47
class deltas643,958,947/753,976,833B; DOWN46/47 are8,974,076/10,015,978B.
Summed UP/DOWN backlog maxima are33,761,415/49,565B. Client RSS peak/final is
320,456/320,000KiB and lifetime CPU129/114%; server127,048/127,048KiB and
56.9/24.2%. No per-handler CPU or post-teardown leak claim follows these values.

Logs contain16,129lines/7,414,475B, including12,234existing
`server_stale_output_recovery` events. That observer overhead is retained,
not called a negligible or ordinary-performance-equivalent capture. No
runtime WARN/ERROR or probe-stderr output is recorded before guarded cleanup.
The information forecast succeeds at locating shared-input service and
backpressure; it rejects an exclusive carrier-priority or measured pre-apply
lookup explanation. The next bounded question is which intervening owner work
limits FIFO service, using exact callsite timing rather than a larger queue,
new timer or presumed speed gain. Ordinary promotion remains stopped.

## Separate owner-profile capture: repeated serialized work dominates held intervals

This diagnostic retains selected-reply stages and measures completed request
Product guard acquisition/hold durations by static instrumented caller. Source
policy remains ordinary `a747bda`; the complete temporary overlay is
`./.tmp/reflection/authoritative-gap-view-owner-profile-0910.patch`. Build47244
completes in3m39s, then the frozen `authoritative-gap-view-owner-profile-20260910`
executable runs after overlay reversal. PERF uses periodic aggregation without
samples. The previously noisy `server_stale_output_recovery` event is disabled
in the observer filter, not in runtime behavior. No threshold, native controller
or physical profile changes.

[Verified raw evidence](AUTHORITATIVE_GAP_VIEW_OWNER_PROFILE_20260910.raw.tar.gz)
contains11regular files: five result files, build/driver logs, the complete
observer patch, its text wrapper, and unchanged `run.py`/`shape.sh`. The archive
is457,326B; gzip integrity, tar comparison and every decompressed file's byte
comparison pass. No configs, credentials, binaries, symlinks or directory
members are included. The runner/profile match the preceding local-stage
archive byte-for-byte.

### Own outcome, not an ordinary improvement comparison

Runner17084 exits0 after48.005502s. All456,720,384B locally accepted are exactly
confirmed in47.794866s (76.447Mbps), one complete stream and no probe errors.
First write/confirmation is.105929/.409460s; maximum local-write/confirmation
gaps are8.650791/4.050847s. Thus eventual settlement does not make this trial
fluent or replace the previous censored result. All48raw bins are retained,
including18zeros; there is no echo workload. Raw means5–15,16–24inclusive,
25–40 and40–48 are36.123,8.212,112.656 and90.913Mbps.

```text
raw bin start (s): receiver-confirmed Mbps
 0: 8.556,191.173,300.085,156.762,0,0,0,26.391,0,0
10: 59.260,0,58.624,98.041,118.918,144.893,36.466,0,17.855,0
20: 14.539,0,5.051,0,0,0,1.240,13.632,69.317,467.413
30: 143.273,0,0,367.835,27.413,0,2.019,548.044,10.141,39.519
40: 21.623,69.748,245.860,0,0,0,29.349,360.720
```

### Measurement truth and dominant source callers

2076owner rows cover32static callsites,64wait/hold components. Every component's
count, byte and elapsed deltas reconcile with its cumulative totals; paired
wait/hold counts match. Owner byte fields are all0, not traffic. There are
513,800completed actor guard observations and54,451successful writer guards.
Actor hold/wait totals are43.688533/1.524999s; writer hold/wait1.297782/.054467s.
Successful writer `try_lock` time excludes all prior Busy results and retries.
No inference of low writer retry residence follows its small measured wait.

The hold clock starts after acquisition and is read immediately after actual
mutex release; recording occurs after unlock and notification. It is an elapsed
critical-section estimate with descheduling/timestamp-boundary uncertainty,
not measured CPU. Wait can overlap another guard's hold and is not added to it.
Only completed guards appear; final inactive sites retain their last recorded
totals rather than manufacturing a simultaneous end-of-run sample. Existing
inner timers are not added to these enclosing holds. Each event has a1us floor.

Caller lines below are reconstructed in memory from the complete frozen patch
and checked against its base hunks, **not** read from reverted ordinary line
numbers. A caller spans its entire guard scope, not just the first function.

| Instrumented caller | Observations | Total hold | Largest hold | Scope |
|---|---:|---:|---:|---|
| `control.rs:1562` | 76,491 | 25.589383s | 27.207ms | Preselect source admission, retained-frontier maintenance, authoritative-gap evaluation and service/deadline geometry |
| `control.rs:3032` | 29,479 | 11.886316s | 13.765ms | Request recovery/repair dispatch and resulting attachment decisions |
| `control.rs:3954` | 26,477 | 2.914869s | 19.341ms | Full received-ACK application, release and path/queue consequences |
| `control.rs:3013` | 29,479 | 1.582490s | 3.632ms | Recovery-batch collection and service-wait arming |
| `control.rs:1114` | 76,491 | 1.405470s | 3.417ms | Preselect topology/recovery observation |
| `prepared.rs:231` (writer) | 17,989 | .666278s | 4.328ms | Successful prepared writer claim scope |

The25.589s scope includes `reliable_stream_source_admission`, retained-tail
selection/discard and `evaluate_client_data_ack_reinjection`; this capture does
not isolate their individual contributions. The11.886s scope includes
`dispatch_next_request_path_recovery` and `dispatch_client_repair_work`.
It is not justified to rename either entire caller as one specific inner query
or assume all its work is unnecessary. The next correction needs that source
work counterexample, not merely a large elapsed total.

### Same-capture critical windows, with complete flush boundaries

Session2060930880107225816/stream0 has a clear local winner[201,214) on QUIC0.
It decodes/enqueues at1789046468971, reaches ordinary carrier handling/enqueue
6468976 and attachment receive/shared-send6469271/6469278, then Product
dequeue/selected/pre-apply/receipt6472675. Its3,397ms AFTER shared admission
lies inside a3,861ms advancing-frontier gap. As before, it is not explained by
a late carrier read or one slow selected-frame pre-apply operation. Abbreviated
timestamps in this paragraph have the common178904prefix.

The maximum completed gap[689,703) lasts4050ms in event timestamps:
1789046487563→1789046491613. Its winning QUIC0repair is decoded at6490080,
carrier-enqueued6490213, attachment-received6491458, shared-enqueued6491463
and dequeued/applied6491613. The decode→receipt local contribution is1533ms;
the earlier part of this user gap is not silently relabelled local service.

Each periodic flush emits rows over several milliseconds. The analysis groups
the complete sorted-component flush, carries unchanged counters forward and
subtracts snapshots only after the full group. Selecting individual timestamps
mid-flush would incorrectly include a preceding interval for later-listed sites.
The following bands are strictly inside the stated held intervals; their Unix
times are complete flush-end timestamps. Largest completed calls can straddle
a band edge, so these are aligned cumulative observations, not per-call start
and stop traces.

| Held interval | Interior flush band (Unix ms) | Band | All hold estimates | Actor/writer wait (separate) | Main hold contributions |
|---|---|---:|---:|---:|---|
| [201,214) shared-input wait | 1789046470072→1789046472079 | 2.007s | 1.981563s | .002975s | 1562:1.083387s/354calls;3032:.797347s/94;ACK:.062347s/75 |
| [689,703) maximum frontier gap | 1789046488128→1789046491138 | 3.010s | 2.986625s | .007754s | 1562:2.315304s/905;3032:.346946s/378;ACK:.235723s/375 |
| [1781,1795) repair decoded→receipt | 1789046509203→1789046511210 | 2.007s | 1.981229s | .016903s | 1562:1.087692s/1354;3032:.652437s/518;ACK:.142174s/517 |

The bands contain2975/6161/9127completed hold observations. Their maximum1us
floor contributions are therefore2.975/6.161/9.127ms, not the approximately
1.98/2.99/1.98seconds measured. The repeated serialized work is material in
these actual stalled windows; there is no single multi-second guard, and
writer contention is not their dominant measured cost. These sums still are
not CPU utilization or promised removable delay. They justify investigating
the dominant preselect/dispatch work before a queue-capacity or carrier-only
scheduling change. The full FIFO's intervening ACK/control contents and exact
inner-call costs are not reconstructed from reply-only events.

### Physical and resource context

All48profiles match two200Mbps links,46UP10Mbps only15–25s,30/70ms delays,
zero loss/jitter/outage, burst/cburst65536B and netem8192. First restriction/
restoration reports are15.001738/25.002787s; every class/netem drop counter
remains0. The accounting window is47.005359s. UP46/47class deltas are
334,726,118/591,726,317B; DOWN46/47 are6,190,689/7,662,486B. Summed UP/DOWN
backlog peaks20,524,093/32,762B. Client RSS peak/final is321,288/314,576KiB with
lifetime CPU124/121%; server112,988/112,988KiB with64.4/32.6%. These are not
per-call CPU or leak measurements.

Logs contain7207lines/2,446,272B, no samples, no noisy server-stale-output
events, and no runtime WARN/ERROR or probe-stderr output. Observation has its
own aggregation/formatting cost after guard release; it is not performance-
equivalent to an ordinary build. The information forecast succeeds: repeated
actor work dominates the observed serialized service, with concrete caller
ownership and small acquisition waits. It does not yet prove which inner
algorithm should change or waive any ordinary practical failure.

## Ordinary recovery-target observation reuse: late confirmation still fails the gate

This separate ordinary run tests one lazy all-path observation per recovery
target selector invocation, projected through the unchanged target helper.
Regular/Backup order, exact copy debt, eligibility and fresh native-fenced Apply
remain unchanged; there is no observation cached across decisions or awaits.
The forecast removes repeated whole-path capture from part of the measured
dispatch scope, not all planning cost or a promised number of stalled seconds.

The real selector RED passes its selection/debt controls then reports(2,4,4)
captures instead of(1,1,1), .03s runtime after7m47s compilation. The corrected
selector and zero-candidate control pass with all73request-multipath checks
in.22s after7m25s compilation. Ordinary build takes3m32s with the existing
unused-helper warning. The frozen binary is
`./.tmp/reflection/bin/recovery-target-observation-20260910/mptunnel`; measured
source is the two-file request-multipath implementation/test change atop
`78875e8`. No feature observer or CPU wrapper runs in this cell.

Result directory is
`./.tmp/reflection/results/aggregate-combined-up-recovery-target-observation-qos-0910/`.
The [verified raw archive](RECOVERY_TARGET_OBSERVATION_ORDINARY_20260910.raw.tar.gz)
contains11regular files: five results, RED/focused/build/driver logs, `run.py`
and `shape.sh`. It is451,103B; gzip integrity, tar comparison and every member's
decompressed-byte comparison pass. No configs, credentials, binaries, links or
directory entries are included. Runner/profile match the preceding owner-profile
archive byte-for-byte. Earlier `b0` and view controls remain in their own archives;
these are preserved comparisons, not simultaneous or identical-work trials.

### Exact settlement and complete ordinary history

| Outcome | Preserved `b0` | Earlier view | Observation reuse |
|---|---:|---:|---:|
| Accepted = confirmed bytes | 1,294,925,824 | 323,158,016 | 419,954,688 |
| Exact completed streams | 1/1 | 1/1 | 1/1 |
| Elapsed, s | 41.538802 | 56.678022 | 64.942209 |
| Completed whole Mbps | 249.391 | 45.613 | 51.733 |
| First write / confirmation, s | .105451 /.409741 | .108528 /.412616 | .105864 /.408088 |
| Maximum write gap, s | .545897 | 14.915527 | 5.172138 |
| Maximum confirmation gap, s | .634001 | 7.470709 | 9.215061 |

Runner22211 exits0after66.008063s. Probe status is `ok`, exact terminal
accounting is valid and no stream/probe error occurs. Confirmation extends
24.942209s beyond the nominal40s offered-load interval; blocked writes mean
that interval is not an exact final-byte acceptance timestamp. The longer
settlement and worse confirmation gap remain adverse despite the improved
write gap and somewhat higher mean. There is no echo workload or censoring
in this run. All65raw confirmation bins follow, including32zeros. The last
bin is partial; the trimmed array is not the wall-clock series.

```text
raw bin start (s): receiver-confirmed Mbps
 0: 7.981,139.634,62.138,0,167.328,9.009,30.933,0,0,18.778
10: 501.411,1.049,22.296,274.494,18.729,0,0,11.002,181.545,99.809
20: 28.932,77.689,56.893,98.098,0,11.943,0,159.478,0,0
30: 64.583,32.13,0,92.538,0,59.22,0,33.859,134.222,28.738
40: 0,0,255.998,447.283,0,0,0,0,0,0
50: 0,7.571,0,0,0,0,0,0,0,0
60: 172.824,0,0,0,51.5
```

| Raw confirmation phase | `b0`, Mbps | View, Mbps | Reuse, Mbps | Reuse zero bins |
|---|---:|---:|---:|---:|
| 0–5s | 262.228 | 127.922 | 75.416 | 1/5 |
| 5–15s, pre-cut | 308.068 | 7.336 | 87.670 | 2/10 |
| 15–25s | 34.658 | 58.420 | 55.397 | 3/10 |
| Interior16–24 inclusive | 16.203 | 64.911 | 61.552 | 2/9 |
| 25–40s, restored | 328.092 | 35.519 | 41.114 | 6/15 |
| Own post40s bins, last partial | Already settled shortly afterward | 44.431 over40–57 | 37.407 over40–65 | 20/25 |

The better pre-cut/restored means versus the view do not approach the preserved
`b0` healthy service; cut confirmation also falls modestly versus the view.
Bursts above physical capacity can release earlier buffered work. They do not
establish either instantaneous link capacity or continuous target progress.

### Forward progress improves, but replies remain held after all target writes

Management counters retain their producer domains: server `from_peer_bytes`
is successful target-socket write, not sink confirmation; server `to_peer_bytes`
is sink-reply read, and client `from_peer_bytes` is local reply write. Rates
below use actual row elapsed differences; management and probe clocks are not
substituted for one another.

| Target-socket sample window | `b0`, Mbps | View, Mbps | Reuse, Mbps |
|---|---:|---:|---:|
| 0→5s | 273.573 | 206.916 | 87.037 |
| 5→15s | 321.800 | 17.783 | 94.412 |
| Strict16→24s | 17.028 | 12.133 | 59.496 |
| 25→40s | 340.910 | 37.919 | 62.647 |

By sample43the client has read all419,954,688upload bytes. By server sample46
(Unix1789048723340) all are successfully written to the target, and1720reply
bytes have been read. Client sample46(1789048723339) has delivered only1609reply
bytes. Target/source and server-reply counters remain unchanged thereafter,
while client reply delivery is still1609at sample51,1623at52through60,
1665at61through64, and finally1720at65(1789048742342). These successive
late reply holds persist after full forward target acceptance; they are not
explained by remaining upload bytes at that stage. Probe gap endpoints are
not stored, so these coarse plateaus are not labelled the exact9.215061s gap.

All eight physical output identities remain active in one session; initialized
native epochs do not change. Over samples46→64all eight native producer stamps
advance on both roles. Client native ACK counters add47,585,563TCP and236,522
QUIC bytes; server return counters add13,590TCP and5,719QUIC bytes. Return
classes carry214,517B combined during that18.002001s sampled band. This is
progressing-carrier evidence, not identification of the missing reply byte,
a necessary-copy count or proof that native/read/FIFO service caused this hold.
Ordinary logs contain no per-frame boundary or owner-cost instrumentation.

### Physical and resource cost, with the adverse result retained

All66profiles verify two independent200Mbps links, only46UP10Mbps15–25s,
30msDOWN/70msUPdelay, zero configured loss/jitter/outage,65536Bbursts and8192
netem limit. First restriction/restoration rows are15.002257/25.003517s.
Class/netem drops remain0. Client eth0/eth1 map to46/47; server eth1/eth0
map to46/47. The accounting window is65.007846s, not the probe's64.942209s.

| UP class service, Mbps | `b0`46 /47 | View46 /47 | Reuse46 /47 |
|---|---:|---:|---:|
| 0→15s | 185.977 /187.386 | 68.051 /68.743 | 100.203 /145.756 |
| Strict16→24s | 9.991 /31.016 | 10.058 /96.718 | 10.019 /137.332 |
| 25→40s | 185.632 /193.366 | 57.456 /28.490 | 72.552 /94.617 |

| Whole sampled cost | View | Reuse |
|---|---:|---:|
| UP46 /47bytes | 336,022,023 /400,620,890 | 423,938,161 /705,498,359 |
| DOWN46 /47bytes | 5,258,507 /5,264,669 | 6,673,253 /8,278,284 |
| Summed UP backlog peak / final, B | 27,190,388 /6,831,910 | 31,303,275 /424,756 |
| Summed UP backlog median0–40, B | 858,520 | 6,392,053 |
| Summed DOWN backlog peak / final, B | 47,353 /990 | 38,033 /810 |
| Client RSS peak / final, KiB | 380,736 /380,736 | 410,640 /410,640 |
| Server RSS peak / final, KiB | 106,724 /106,724 | 123,852 /123,852 |
| Client lifetime CPU peak / final, % | 135 /112 | 124 /116 |
| Server lifetime CPU peak / final, % | 69.3 /19.4 | 45.9 /26.8 |

Different completed work and observation durations prevent declaring cheaper
CPU or wire service from selected aggregates. CPU is the existing process-
lifetime percentage, not one-second execution or measured planning savings;
the separately prepared20%loss CPU observation has not run in this cell.
Client/server logs and probe stderr are empty. The driver retains its existing
HTB quantum notices; no shaping parameter is changed to suppress them.

**Disposition: no performance promotion.** The narrow repeated-observation
counterexample is corrected and semantic controls pass, but practical service
still has multi-second confirmation stalls, severe restored goodput loss
against `b0`, and higher RSS/whole wire cost. The result does not prove the
reuse caused the later tail or quantify how much serialized work it removed.
It falsifies sufficient-service improvement from this isolated correction;
neither another parameter adjustment nor the unrelated deployed loss/CPU
report is justified as its attribution or acceptance waiver.

## Preselect partition diagnostic: long holds move to dispatch, not stale-only queries

This is an information result, not a performance correction or acceptance.
The unchanged `d44ca8e` runtime plus a temporary feature-only observation
overlay reproduces a 17.859206s confirmation gap despite eventual exact
settlement. Whole-run preselect time is dominated by authoritative-gap work,
but the longest local reply holds coincide principally with the separate
dispatch owner. The proposed stable-absent query pruning is therefore deferred:
its measured scope is too small to explain or remove the dominant held service.

The optimized diagnostic build takes 3m38s with only the existing unused
`apply_and_write_ready_stream_data_batch` warning. The complete 17-source-file
overlay is frozen in the saved patch and reversed before traffic; the frozen
`preselect-owner-profile-20260910` executable is used through its wrapper with
periodic performance summaries enabled and samples disabled. This includes
the preceding selected reply-stage and owner-guard observations, not a new
ordinary runtime policy. The [verified raw archive](PRESELECT_OWNER_PROFILE_20260910.raw.tar.gz)
contains 11 regular files: five results, build/driver logs, the complete patch,
wrapper, `run.py` and `shape.sh`. It is 818,226B; gzip integrity, tar comparison
and every decompressed member/source byte comparison pass. No binary, config,
link or directory entry is included.

### Own outcome and complete confirmation history

Runner89074 exits0 after 77.008481s. The probe confirms exactly 378,339,328B
accepted, one complete stream, no errors and no censoring, over 76.528672s
(39.550Mbps). First write/confirmation are .105781/.411549s; maximum
write/confirmation gaps are 11.889302/17.859206s. There is no echo workload.
Eventual settlement does not make these multi-second service failures acceptable,
nor does this instrumented run replace either preceding ordinary control.

All 77 raw confirmation bins follow, including 49 zeros. Values are Mbps;
the final bin is partial. The trimmed array must not substitute for this
wall-clock history.

```text
raw bin start (s): receiver-confirmed Mbps
 0: 8.214,179.436,73.024,0,87.12,0,66.393,49.999,102.62,30.645
10: 216.625,4.084,68.89,0,55.242,84.263,147.14,132.322,16.733,126.064
20: 0,0,0,150.995,0,0,0,0,0,0
30: 0,0,293.27,0,0,343.354,36.176,0,0,0
40: 0,311.268,0,0,0,0,0,0,0,0
50: 0,0,4.55,0,0,.908,0,0,0,0
60: 0,0,0,0,0,0,0,0,0,0
70: 0,0,0,28.755,67.083,170.784,170.758
```

| Raw phase | Mean Mbps | Zero bins |
|---|---:|---:|
| 0–5s | 69.559 | 1/5 |
| 5–15s, before cut | 59.450 | 2/10 |
| 15–25s | 65.752 | 4/10 |
| Interior16–24 inclusive | 63.695 | 4/9 |
| 25–40s, restored | 44.853 | 12/15 |
| 40–77s, last partial | 20.381 | 30/37 |

### Reconciled cost scopes and the forecast boundary

All interval count/byte/time deltas reconcile to cumulative totals in all
4,075 client and 960 server performance rows, with one PID per role. The four
preselect phases each record 82,296 evaluations. They partition the synchronous
preselect body, not lock acquisition, later dispatch or asynchronous waiting.

| Nonoverlapping preselect phase | Cumulative elapsed, s | Largest phase evaluation, ms |
|---|---:|---:|
| Authoritative-gap service | 22.346425 | 9.736 |
| Source geometry | .543246 | 3.155 |
| Retained cleanup | .494605 | 3.807 |
| Residual/deadline | .115695 | 1.526 |
| Phase sum | 23.499971 | Not additive |

Authoritative-gap service is 95.09% of this partition. It includes setup,
coverage/queued-union subtraction, ownership-view construction, scored prefixes,
cache/target models, clocks and observation overhead. Its full residual has
not been isolated by the following narrower query timers.

| Owner-mask plus uniform-frontier query result | Queries | Cumulative timed query work, s | Nonempty class emissions |
|---|---:|---:|---:|
| Owner present under current dynamic eligibility | 1,788,439 | 1.489723 | 36,798 |
| Absent also under stable attached non-stale set | 2,699,580 | 1.231554 | 29,730 |
| Absent dynamically but owner present under stable set | 0 observed | 0 observed | No component emitted |

For these three components the generic byte fields are **query counts**, not
traffic; record counts are nonempty per-evaluation class batches. Their maximum
times are batch sums, not individual-query maxima. Production query timing
stops before the stable-set classifier. Separately recorded observer work is
.169392s over 56,517 emissions; it covers stable-set construction/classification,
not all logging overhead. It and the query timers are nested within enclosing
work and must not be added to the phase total. Stable-absent work is 60.15% of
queries but only 5.51% of the complete gap-service elapsed time. Even eliminating
all that measured query work would not by itself remove the dominant cost;
classification counts do not establish a safe pruning rule or total savings.

All client actor guards total 72.850840s of exclusive elapsed hold over 556,153
acquisitions; acquisition wait totals 1.178401s. Writer guards total .915945s
hold and .046504s successful-try-lock time over 46,372 acquisitions. Instrumented
`control.rs:1567` preselect holds 23.579505s; `:3095` dispatch holds 44.081456s;
`:4017` ACK application holds 1.887422s. These line numbers refer to the archived
instrumented source, not the current source tree. The dispatch guard contains
both direct structural recovery and, if appropriate, queued repair dispatch;
44.081456s cannot be assigned to queued copies alone. Initial recovery collection
is in an earlier guard. Largest actor hold is 47.377ms, so these totals represent
repeated ownership, not one continuous 44s call. Timings measure elapsed lock
ownership, including possible descheduling, not CPU execution. Every emitted
duration is floored to 1us; nested timers and thread/owner totals are not additive.

### Exact reply ownership during the held intervals

All following times use Unix milliseconds, with millisecond resolution; neither
role's `t_mono` origin nor the probe clock is substituted. Session
14082984577445849136/stream0 has 334 Product receipt events and 92 advancing
receipts. Exact offset/length and carrier joins distinguish Original and copy;
cross-role attachment/incarnation numbers are not assumed interchangeable.

The longest advancing gap is 1789050916241→1789050934101 (17.860s), agreeing
with the probe maximum within timestamp resolution. Its final missing interval
is [1106,1120), a unique Q1 Original on the healthy47 link. Previous Product
advance reaches 1106; this receipt advances to 1120. Its server Original and
normal QUIC write-begin/end are all 1789050902383. The same Q1 frame is decoded
at 1789050913483 and accepted by the reader queue at 1789050913739. Normal
carrier handling starts only at 1789050927926, enqueues at 1789050928059,
and the attachment receives it at 1789050933068. Shared Product queue admission
succeeds at 1789050933132 with recorded depth131; dequeue occurs at
1789050934100, then input selection, preapply-lock completion and Product
application at 1789050934101. No Q1 copy overlaps this extent; other-carrier
copies do not make this Original join ambiguous.

Thus its write-end→decode is 11.100s, reader-queue→carrier handling 14.187s,
carrier enqueue→attachment 5.009s and shared-queue→dequeue .968s. The whole
31.718s write-end→Product residence overlaps earlier frontier advances and is
**not** the 17.860s critical gap or an additive delay budget for it. In particular,
the already-decoded local delays are directly observed; the separate predecode
delay is not assigned to the physical network from these events alone.

A second critical winner, Q0 repair [1078,1092), ends an 11.701s advancing
gap at 1789050913952. Repair decode/enqueue occur at 1789050902922/2943;
attachment delivery is 1789050909613, and accepted shared-queue admission is
1789050909828 at depth131. Product dequeue/application is 1789050913952:
6.670s repair-reader→attachment and 4.124s admitted shared-FIFO residence.
There is no Q0 Original for this extent. These are actual local service holds,
not a failed queue-send attempt or a later duplicate chosen as the winner.

Counter joins use first and last **complete periodic flushes wholly inside**
each stated held interval, carry inactive cumulative components forward, and
difference states only after each entire flush. Completed calls can cross the
chosen boundaries; these are interval aggregates, not per-call CPU traces.

| Held interval and interior full-flush endpoints, Unix ms | Interior wall, s | Actor hold, s | Dispatch hold, s | Preselect hold, s | Stable-absent query work |
|---|---:|---:|---:|---:|---:|
| Maximum frontier gap; 1789050916324→1789050933488 | 17.164 | 17.034518 | 16.487691 /1,086 calls | .208979 | 78,636 queries /.056172s |
| Q0 shared-FIFO wait; 1789050910246→1789050913277 | 3.031 | 3.010913 | 2.888610 /101 calls | .085612 | 50,593 queries /.046558s |
| Q1 reader hold; 1789050914298→1789050927438 | 13.140 | 13.042140 | 12.623933 /620 calls | .209116 | 101,882 queries /.076649s |

These intervals overlap and must not be summed. In the maximum-gap interior,
authoritative-gap phase time is only .148319s, actor acquisition wait .036080s
and writer hold .018205s. This localizes the dominant repeated ownership to
dispatch during that hold, without splitting direct structural recovery from
queued work or claiming every millisecond would disappear under a correction.
It specifically weakens the stale-only-query proposal as a material remedy.

### Target, physical and resource context

The client reads all upload bytes by sample42. Target-socket acceptance continues
from 323,496,620B at40 to 369,557,241B at56 and all 378,339,328B at66
(Unix1789050926965). The server has read all 1,945 reply bytes by67, while
client local reply delivery remains at1,106 through73, then1,120/1,134/1,204
at74/75/76. Therefore the maximum reply hold spans continuing forward work
and a later period after all target writes; it is not wholly an upload-debt
plateau. These management counters are not final sink-confirmed accounting.
Closure records receive frontier1945, reorder0 and empty sender queues.

Across samples56→73 all eight native outputs on each role retain the same
epochs, advance their producer sampling stamps and increase ACK counters.
This proves ongoing carrier-level progress, not delivery of the critical reply
byte. It does not make management cache timestamps exact critical-frame clocks.

All 77 physical samples verify the declared two independent200Mbps links,
only46UP10Mbps during rows15–24, DOWN30ms/UP70ms, zero loss/jitter/outage,
65536B bursts and netem limit8192. Restriction/restoration rows are at
15.001659/25.002734s. All class/netem drop deltas are0. Client eth0/eth1 are
46/47; server eth1/eth0 are46/47. Cost accounting spans76.008329s, distinct
from probe settlement and performance flush windows.

| Sampled class service | UP46 /47, Mbps |
|---|---:|
| 0→15s | 130.040 /108.277 |
| Strict16→24s | 10.146 /116.428 |
| 25→40s | 44.248 /35.580 |
| 55→73s | 23.361 /11.781 |

| Whole sampled cost | Value |
|---|---:|
| UP46 /47 bytes | 508,075,265 /553,865,687 |
| DOWN46 /47 bytes | 6,063,693 /7,964,900 |
| Summed UP backlog peak / final, B | 28,153,280 /1,345,268 |
| Summed DOWN backlog peak / final, B | 46,573 /2,064 |
| Client RSS peak / final, KiB | 309,540 /257,448 |
| Server RSS peak / final, KiB | 111,576 /111,576 |
| Client lifetime CPU peak / final, % | 122 /112 |
| Server lifetime CPU peak / final, % | 57.3 /19.4 |

CPU here is the existing process-lifetime `ps` percentage, not the separate
20%loss task-tick measurement. Logs contain 8,811 lines/3,140,973B; observation
overhead is not fully measured by the .169392s classifier timer. The noisy
stale-output event is disabled. Probe stderr is empty; two server H3_NO_ERROR
closure warnings occur at teardown, not independent failed probe attempts.

**Information disposition:** preserve the observed local FIFO/owner deficit and
the ordinary rejection. This capture supports splitting the remaining complete
gap and dispatch cost scopes before selecting a work-model correction; it does
not support a stale-only pruning patch, native timer adjustment, an unavoidable
physical-stall explanation or a new performance claim.

## Dispatch-inner diagnostic: bound replanning, with substantial gap-model work

The next information capture separates direct structural recovery from queued
repair and the expensive bound-send plan from successful publication. It does
not change scheduling authority or supply an ordinary speed comparison. Exact
settlement again coexists with a material confirmation stall: 480,378,880B in
48.576457s, 79.113Mbps, maximum confirmation gap9.000672s. The earlier
17.859206s diagnostic gap remains preserved, not replaced by this outcome.

Source is ordinary `d44ca8e` plus the frozen 18-source-file temporary overlay.
Build8383 completes in3m35s; warnings are the existing unused batch-write helper
and the ordinary dispatch wrappers unused by this feature-only adapter. The
complete overlay is reversed before traffic. Frozen
`dispatch-inner-profile-20260910` runs with periodic PERF, no performance
samples, and the existing selected reply-stage events. Runner16610 exits0
after49.006014s. The [verified raw archive](DISPATCH_INNER_PROFILE_20260910.raw.tar.gz)
is569,079B with11regular files: five results, build/driver logs, complete patch,
wrapper, `run.py` and `shape.sh`. Gzip integrity, tar comparison and every member's
decompressed-byte comparison pass. Runner/profile match the preceding archive;
no configs, binaries, links or directory entries are included.

### Exact outcome and full timing series

Accepted and confirmed bytes are equal, one stream completes, and there are
no probe errors or censored bins. First write/confirmation are .105755/.242271s;
maximum write gap is1.872985s. There is no echo workload. The49raw confirmation
bins below contain20zeros; the final bin is partial. Completion is8.576457s
beyond nominal40s load, not proof that every source write ended at40s.

```text
raw bin start (s): receiver-confirmed Mbps
 0: 14.754,205.137,208.334,50.332,30.217,234.408,44.276,28.836,44.126,195.857
10: 65.536,164.242,0,142.114,301.823,0,0,0,27.411,0
20: 86.879,0,0,0,42.851,36.981,23.497,63.343,570.067,0
30: 373.625,38.37,174.114,0,0,0,0,0,0,0
40: 0,3.382,0,0,0,32.987,66.363,175.695,397.472
```

| Raw confirmation phase | Mean Mbps | Zero bins |
|---|---:|---:|
| 0–5s | 101.755 | 0/5 |
| 5–15s | 122.122 | 1/10 |
| 15–25s | 15.714 | 7/10 |
| Interior16–24 inclusive | 17.460 | 6/9 |
| 25–40s | 85.333 | 8/15 |
| 40–49s, last partial | 75.100 | 4/9 |

### Complete counter reconciliation and timer scope

Every interval count/byte/time delta matches its cumulative counter across
4,234client and677server performance rows, with one PID per role. For dispatch
timing labels, generic `bytes` means **phase calls**; for event-count labels it
means **events**. Only `request.dispatch.payload_bytes` is actual committed
payload. Generic record counts count emitted nonempty batches, not attempts.
Maximum elapsed values are per-guard accumulated phase batches, not individual
attempt maxima. Event-only durations are synthetic1us floors, not measured cost.

Dispatch direct/queued are disjoint children of its owner body. Prepare/select/
send are children of direct; send-total occurs in direct and queued; plan,
authority, reservation and fenced Apply nest inside send-total. Product commit
nests in fenced Apply. Do not add parents and children. Timings measure elapsed
work, including descheduling, not CPU execution; local accumulation and logging
overhead remain within some enclosing timers.

| Dispatch scope | Cumulative elapsed, s | Actual calls/events |
|---|---:|---:|
| Direct structural recovery | 13.409747 | 22,553 calls |
| Queued repair | .218317 | 24,748 calls |
| Structural candidate preparation | .392297 | 1,471,424 region visits |
| Structural target selection | 1.485480 | 1,471,051 calls |
| Structural bound send | 11.296259 | 463,360 calls |
| Shared bound/unbound send-total | 11.460190 | 472,741 calls |
| Send-plan construction | 11.122946 | 472,741 calls |
| Avoid-history lookup | .069813 | 472,741 calls |
| Target authority | .023274 | 14,971 calls |
| Reservation and eligibility before native fence | .027714 | 14,971 calls |
| Fenced Apply | .119824 | 12,558 calls |
| Product commit, inside Apply | .072298 | 12,558 calls |

Selection returns no target1,007,691times; structural bound attempts return
blocked/migratable456,279times. Across all observed sends,459,573return
`SenderServiceBlocked`,610return other errors and12,558commit, exactly
accounting for472,741send calls. Committed payload is450,316,407B of recovery
traffic; this is neither unique repaired bytes nor proof that copies were
unnecessary. No `native_stale` component is emitted: no such recorded outcome
occurs, rather than an unmeasured value treated as zero. Other error attempts
are internal publication outcomes, not610independent failed user transfers.
No-target count does not independently time failed selections. Source and
these measurements distinguish costly replanning from successful flight
recording, but do not yet identify the exact repeated plan predicate to change.

The four preselect phases total24.060029s: gap23.313297, source.329930,
retained.321184 and residual.095618. The following helper counters cover
**all gap-service calls**, including ACK-triggered calls in `relay/client.rs`,
not only the preselect caller. They are not an exclusive partition of that
23.313297s; subtracting them from it would mix caller scopes.

| Inner gap-service scope, all callers | Elapsed, s | Calls |
|---|---:|---:|
| Coverage/subtraction | .400861 | 31,279 |
| Boundaries | .553453 | 27,699 |
| Ownership view | .671884 | 27,699 |
| Scored metadata | 2.625237 | 1,768,251 |
| Exact cache preview | 1.162965 | 1,768,251 |
| Owner/target model | 13.746046 | 1,768,251 |
| Assignment clocks | 1.905566 | 1,580,224 |

Owner queries separately record present1,768,251/.910376s and stable-absent
4,469,451/1.734640s; dynamic-absent has no emitted component. Classification
observer time is.242135s. These nested all-call domains likewise cannot be
summed into an exact preselect budget. Actor guards total44.699875s hold and
1.470677s acquisition wait over452,079acquisitions; writer guards total
1.269273s hold and.046542s successful-try-lock time over46,383acquisitions.
Largest individual actor hold is20.673ms. Instrumented owner sites are
preselect `control.rs:1569`24.086979s, dispatch `:3101`13.627873s and ACK
application `:4047`3.058333s, using the archived source line map.

### Winning repair and complete-flush critical intervals

Session10028713324498556993/stream0 has383Product receipts and158advancing
receipts. The maximum advancing gap is Unix1789051946189→1789051955189,
9.000s at millisecond resolution, consistent with the probe maximum. Its
winning missing interval [1708,1722) is a Q1 repair, physical1/client attachment3,
not the Q0 Original or a later duplicate. Server repair acceptance at
1789051946778 (Q1/incarnation4) matches client repair decode at1789051946808.
Repair queue admission finishes1789051947145; attachment delivery occurs only
1789051953447. Shared Product queue admission succeeds1789051953500 at
recorded depth131; dequeue, input selection, preapply-lock completion and
Product application occur1789051955189, advancing1708→1722.

Thus acceptance→decode is30ms, decode→repair enqueue337ms,
repair enqueue→attachment6.302s, and admitted shared-FIFO→dequeue1.689s.
Decode→Product is8.381s of directly observed local residence. Admission is not
native write completion, and the30ms stage is not a measured wire-only delay.
The same-range Q0 Original is decoded only at1789051960597 and later applied
as a duplicate. It must not replace the winning copy in this causal join.

Using whole periodic flushes strictly inside the held intervals, carrying
inactive cumulative counters forward and differencing after each full flush:

| Held interval; interior flush endpoints, Unix ms | Wall, s | Actor hold, s | Preselect hold, s | Dispatch hold, s | ACK hold, s |
|---|---:|---:|---:|---:|---:|
| Maximum gap;1789051946675→1789051954708 | 8.033 | 7.931464 | 4.638369 | 2.497864 | .498973 |
| Repair queue→attachment;1789051947678→1789051952700 | 5.022 | 4.949824 | 2.699114 | 1.734398 | .314247 |
| Shared FIFO;1789051953704→1789051954708 | 1.004 | .995539 | .631806 | .256898 | .071332 |

Intervals overlap; do not sum them. In the maximum-gap interior, direct
dispatch is2.495686s versus queued.002182s. Plan work is2.086050s over88,727
calls, with88,071blocked and656committed sends (36,525,800payload bytes).
Structural selection is.271628s/189,623calls with100,975no-target results;
fenced Apply is.015761s and Product commit.011521s. No native-stale outcome
is recorded there. Gap phase is4.613013s; all-call owner/target model is
1.718160s, scored metadata.681257s and assignment clocks.531905s, some of
which may belong to ACK-triggered evaluations. Actor acquisition wait is
.024581s and writer hold.018749s. Completed calls can cross flush boundaries;
these are measured aggregate ownership within a held interval, not an exact
CPU or removable-delay budget.

Both preselect and repeated bound planning therefore matter in this capture,
unlike the preceding observer's dispatch-dominated long interval. An earlier
3.554s advancing gap ending with Q1 Original[966,980) has a2.008s full-flush
interior containing1.982901s actor hold,1.781324s preselect and only about1ms
direct dispatch; all-call owner/target model is1.273920s there. Its decode
arrives late in that interval, so the whole earlier gap is not labelled local
postdecode delay. This difference is retained rather than forcing every stall
into one owner or selecting only the favorable stage breakdown.

### Forward progress, physical service and cost

During the maximum reply hold, target writes continue: sample33 records
396,316,796B, sample35 448,639,696B and sample41 456,831,696B. Client source
read is463,411,060B at33–40,463,425,660B at41, and finally480,378,880B at48.
The final management row still has only472,250,240target-written bytes and
2,226server-read/1,820client-written reply bytes; exact settlement follows
that sample. Unlike the preceding capture, this is not a hold wholly after
full forward target acceptance. At closure Product receive frontier is2253,
reorder0 and sender/reinjection queues0. That finite close is not a latency pass.

All eight native outputs in samples33→41 retain epochs and advance producer
sample stamps and ACK bytes on each role, including the winning Q1 carrier.
Native progress does not identify the critical bytes, invalidate the exact
local residence, or classify the differing forward debt as useful versus
duplicate copies.

All49profiles match two independent200Mbps links, only46UP10Mbps during
rows15–24, DOWN30ms/UP70ms, zero loss/jitter/outage,65536Bbursts and limit8192.
Actual restriction/restoration rows are15.002002/25.003163s. All class and
netem drop deltas are0. Cost accounting spans48.005810s, not the probe or
complete-flush interval. Physical46/47 mappings are unchanged.

| Sampled phase | UP46 /47 class Mbps | Target-socket Mbps |
|---|---:|---:|
| 0→15s | 175.836 /68.528 | Separate startup below |
| Strict16→24s | 9.979 /7.456 | 6.018 |
| 25→40s | 91.096 /115.759 | 117.585 |
| 33→41s | 45.602 /55.452 | 60.507 |

Target-socket service is176.403Mbps at0→5s and94.820Mbps at5→15s.
These are successful target writes, not receiver confirmation bins. Link47
remains physically200Mbps during the cut, but is lightly used in this particular
capture. The decoded winning reply's local delay is stronger attribution than
inferring a cause from that aggregate utilization or comparing diagnostic means.

| Whole sampled cost | Value |
|---|---:|
| UP46 /47 bytes | 538,504,313 /434,666,729 |
| DOWN46 /47 bytes | 8,056,057 /6,607,685 |
| Summed UP backlog peak / final, B | 33,850,496 /4,325,690 |
| Summed DOWN backlog peak / final, B | 42,447 /6,874 |
| Client RSS peak / final, KiB | 352,052 /340,928 |
| Server RSS peak / final, KiB | 121,992 /114,036 |
| Client lifetime CPU peak / final, % | 129 /123 |
| Server lifetime CPU peak / final, % | 64.2 /33.4 |

CPU remains process-lifetime `ps`, not interval execution. Logs contain
9,187lines/3,211,386B, including observer summaries; total measurement overhead
is not known. Probe stderr is empty. Two H3_NO_ERROR warnings occur at teardown,
not two additional failed streams. **No performance promotion:** the information
forecast identifies repeated direct bound planning and all-call gap-model work
as material owners, while successful commit/native-stale replan and queued
dispatch are small in the decisive interval. The next correction requires a
reachable invariant in that measured planning scope; neither a new parameter
nor another isolated minor query optimization follows from these totals.

## Ordinary lazy bound-repair observation: rejected for performance promotion

The minimal chooser-level reuse does not pass the ordinary gate. Run51554
reaches the unchanged85s settlement guard with141,370,950B confirmed of
201,261,056B locally accepted: **59,890,106B remain unconfirmed**. The maximum
confirmation gap is19.827287s. This practical failure is retained despite the
actual work counterexample and74focused checks passing. It does not establish
that the small source change caused every difference from the earlier runs.

The candidate is ordinary `d44ca8e` plus one lazy authority-only observation
inside the existing repair chooser, shared across its target/tier passes.
Measured-evidence predicates, live cause-specific admission, the separate outer
eligibility capture and fresh final Apply remain unchanged. This is neither
cross-region caching nor a new timing/quantum/controller policy. The declared
ceiling was the affected portion of the diagnostic2.086s critical/11.123s
whole bound-plan work, not a promised cure for the remaining gap-model work.

The real bound-plan test first passes its semantic assertions, then fails its
capture-count assertion with(6,3,7,3,10,6) instead of(2,2,2,2,2,2). RED43266
compiles in1m11s and fails at the intended assertion. Focused43172 subsequently
passes74tests in.24s. Ordinary release build10700 completes in3m33s with the
existing unused batch-write helper warning; no diagnostic flags are used in
the run. The [verified raw archive](BOUND_REPAIR_OBSERVATION_ORDINARY_20260910.raw.tar.gz)
contains12regular files: five results, RED/focused/build/driver logs, the exact
two-file source/test patch, `run.py` and `shape.sh`. It is569,392B; gzip integrity,
tar comparison and every decompressed member/source-byte comparison pass.
Runner/profile match the preceding archive. No configs, binaries, links or
directory entries are included. The patch records source identity, not acceptance.

### Exact versus censored accounting, with every preceding ordinary result kept

These are chronological single realizations with different completed/offered
work, not a newly paired or packet-identical causal comparison. `b0` is the
original native-refill independent-cut result, `view` is ordinary10116, and
`d44` is ordinary22211. Their complete raw histories remain in their preceding
reports/archives; no favorable control or diagnostic replaces them.

| Outcome | `b0` | View | `d44` | Lazy bound chooser |
|---|---:|---:|---:|---:|
| Locally accepted B | 1,294,925,824 | 323,158,016 | 419,954,688 | 201,261,056 |
| Confirmed B | 1,294,925,824 | 323,158,016 | 419,954,688 | 141,370,950 lower bound |
| Exact completion | 1/1 | 1/1 | 1/1 | 0/1; censored |
| Observed elapsed, s | 41.538802 | 56.678022 | 64.942209 | 85.422621, cutoff |
| Completed whole Mbps | 249.391 | 45.613 | 51.733 | Not a completed rate |
| First write / confirmation, s | .105451 /.409741 | .108528 /.412616 | .105864 /.408088 | .105454 /.410085 |
| Maximum write gap, s | .545897 | 14.915527 | 5.172138 | 11.731949 |
| Maximum confirmation gap, s | .634001 | 7.470709 | 9.215061 | 19.827287 |
| Raw confirmation bins / zeros | 42 /0 | 57 /35 | 65 /32 | Not emitted |

Root runner exits1 with `probe failed to settle`. Runner cleanup closes the
client first; the probe's final “sink closed before terminal acknowledgement”
is a **guard-triggered censored ending**, not evidence of autonomous Product
shutdown. Probe JSON reports status`loss`, incomplete/lower-bound accounting,
one failed stream and no valid terminal ACK. Its13.240Mbps confirmed and
18.849Mbps accepted rates are cutoff summaries, not exact completed goodput.
There is no echo workload. The incomplete-result schema emits empty raw and
trimmed arrays: no confirmation series or gap endpoints are reconstructed
from target counters, native bytes or selected management samples.

| Preserved raw confirmation phase, Mbps | `b0` | View | `d44` | Candidate |
|---|---:|---:|---:|---:|
| 5–15s | 308.068 | 7.336 | 87.670 | Unavailable |
| Interior16–24 inclusive | 16.203 | 64.911 | 61.552 | Unavailable |
| 25–40s | 328.092 | 35.519 | 41.114 | Unavailable |

### Ordinary failure localization: growing source debt and joint target/reply plateaus

Management supplies a separate target-socket write history, not missing
confirmation bins. The failure is not confined to the physical restriction:
at sample10 client reply delivery is309B and remains309B through sample15,
while target writes advance135,096,552→172,737,766B. This pre-cut hold is not
proof of postdecode delay because this ordinary capture has no exact reply
stages. During and long after the restriction, forward target service also
collapses; restored physical capacity does not restore useful ordered service.

| Successful target-socket phase, Mbps | `b0` | View | `d44` | Candidate |
|---|---:|---:|---:|---:|
| 0→5s | 273.573 | 206.916 | 87.037 | 148.761 |
| 5→15s | 321.800 | 17.783 | 94.412 | 63.791 |
| Strict16→24s | 17.028 | 12.133 | 59.496 | .317 |
| 25→40s | 340.910 | 37.919 | 62.647 | .140 |

In samples47→55, target writes stay173,775,270B, server-read reply729B and
client-written reply337B, while source read grows187,019,078→191,147,846B.
Another joint-flat band is samples60→67, with client producer timestamps
Unix1789053193380→1789053200380: target173,775,270B, server reply729B and
client reply365B are unchanged, while source195,669,830→198,291,270B grows
2,621,440B. The increasing source/target difference is outstanding logical
work, not a measured count of accepted repair copies. These sampled plateaus
are not assigned as the exact19.827287s probe gap, whose endpoints are absent.

Native traffic is small but not universally frozen in that latter band.
ClientQ0 ACKs advance192,530B with a6.931086s producer-stamp advance; Q1 adds
66B with7.028526s. The two bulk-used TCP outputs have unchanged ACKed bytes
but producer stamps advance about6.916/6.941s. Other TCP outputs add36B each.
Server return native counters also add small amounts. These control-sized
increments are not bulk progress or identification of the missing reply byte.
Some inactive carrier stamps remain unchanged in other bands, so a management
row alone is not treated as a fresh native poll for every output.

At sample85, source read is201,261,056B, target writes174,294,164B, server
reply read799B and client reply delivery435B. The target still accepts173,678B
over80→85s (.278Mbps), so the final state is severely slow and incomplete,
not a proved fully dead actor. Both target debt and unread replies remain;
this is not solely the earlier “all target bytes done, replies held” case.

### What the ordinary CPU and queue state can—and cannot—distinguish

Client process-lifetime `ps` CPU at samples60–67 is103,103,103,103,103,103,
102,102%; server falls9.2→8.3%. Client RSS grows309,812→310,280KiB; server
stays61,040KiB. Sustaining that lifetime percentage does not resemble a
long completely idle client, but it is not interval CPU, per-call cost or
proof of a particular synchronous owner. No observer timers are present.

Client summary `queue_bytes` is0 at these eight samples; reported native flight
is0 except15,010B at62. Product `data_level_bytes_in_flight` falls39,107,916→
35,864,516B. At60/67, Q47's Product flight is35,697,444/33,124,004B while its
native flight is0 and reported native window is about5.45MB; Q46's Product
flight is2,891,720/2,221,760B with native flight0 and window about5.10MB.
The two bulk-used TCP Product-flight values remain453,216/65,536B, versus
native flight0 and windows about7.36/7.33MB. These fields do not expose exact
queued-repair ownership, accepted-copy debt, command-lane occupancy, retained
intervals or the cause-specific eligibility predicate. Zero summary queue
does not prove no pending Product work; a native window is not recovery authority.

Independent `ss` samples60/67 show the two used client TCP sockets with unread
Recv-Q129,592→126,800B and127,392→124,204B, while their Send-Q and NOTSENT
are0. Their native ACKed counters stay fixed, advertised send windows are
25,164,800B and cwnds5,085/5,059segments. These are pending local incoming
bytes and available native context, not proof that they contain the critical
response prefix or that Product can currently admit a new repair. The evidence
does not yet separate repeated local work from cause-specific recovery debt/
authority blocking, and does not justify declaring either unavoidable.

### Matched physical conditions and complete cost context

All86candidate samples verify two independent200Mbps links, only46UP10Mbps
during rows15–24, DOWN30ms/UP70ms, zero loss/jitter/outage,65536Bbursts and
limit8192. Actual restriction/restoration rows are15.002121/25.003275s.
All class/netem drop deltas are0. The same checks pass for all three controls.
Client eth0/eth1 remain46/47; server eth1/eth0 remain46/47. Candidate cost
accounting spans85.009736s, not the probe's85.422621s or a flush interval.

| UP class service, Mbps | `b0`46 /47 | View46 /47 | `d44`46 /47 | Candidate46 /47 |
|---|---:|---:|---:|---:|
| 0→15s | 185.977 /187.386 | 68.051 /68.743 | 100.203 /145.756 | 73.358 /109.263 |
| Strict16→24s | 9.991 /31.016 | 10.058 /96.718 | 10.019 /137.332 | 3.184 /2.383 |
| 25→40s | 185.632 /193.366 | 57.456 /28.490 | 72.552 /94.617 | .414 /.284 |

| Whole sampled cost | `b0` | View | `d44` | Candidate |
|---|---:|---:|---:|---:|
| UP46 /47B | 745,256,344 /801,341,232 | 336,022,023 /400,620,890 | 423,938,161 /705,498,359 | 144,486,456 /209,833,695 |
| DOWN46 /47B | 15,482,120 /15,889,276 | 5,258,507 /5,264,669 | 6,673,253 /8,278,284 | 2,447,475 /2,682,143 |
| Summed UP backlog peak / final, B | 15,059,114 /5,554,524 | 27,190,388 /6,831,910 | 31,303,275 /424,756 | 22,541,694 /0 |
| Summed DOWN backlog peak / final, B | 53,045 /47,115 | 47,353 /990 | 38,033 /810 | 39,847 /0 |
| Client RSS peak / final, KiB | 391,104 /385,112 | 380,736 /380,736 | 410,640 /410,640 | 310,500 /310,500 |
| Server RSS peak / final, KiB | 125,516 /125,516 | 106,724 /106,724 | 123,852 /123,852 | 61,040 /61,040 |
| Client lifetime CPU peak / final, % | 188 /162 | 135 /112 | 124 /116 | 112 /102 |
| Server lifetime CPU peak / final, % | 90.4 /77.8 | 69.3 /19.4 | 45.9 /26.8 | 50.6 /6.7 |

Lower traffic, queues or RSS under far less completed work and a longer
censored interval are not efficiency improvements. Candidate client/server
logs and probe stderr are empty; the runner's guard traceback and existing
HTB notices are preserved. No lab parameter is changed to suppress them.

**Disposition: reject performance promotion.** The local work invariant is
corrected, but this ordinary capture supplies no practical benefit and exposes
severe incomplete healthy/restored service. Retaining a source checkpoint for
exact diagnosis does not upgrade it to acceptance. The next decision must
distinguish the remaining actual work/authority boundary using this ordinary
state; neither the earlier diagnostic timer reduction nor component GREEN
waives failed settlement or authorizes another tuning adjustment.

## ACK invalidation diagnostic: actual re-dirtying overlaps held reply service

The next information capture distinguishes an already-subsumed successful ACK
from an actual false-to-true recovery-dirty transition. It finds both substantial
re-dirtying and a directly observed 6.246s post-decode reply hold. This establishes
a reached invalidation mechanism worth testing, **not** the fraction of dispatch
time removable by suppressing it. Independent capacity, model and deadline wakes
can request the same work. The largest 7.746007s confirmation gap has a different
winning-frame boundary and must not be relabelled as a 7.746s post-decode hold.

Source is ordinary `d44ca8e` plus the saved 18-source-file observation overlay;
the failed bound-chooser candidate and rejected full-gap shared view are absent.
Build33516 completes in 3m37s with the existing unused batch helper and ordinary
dispatch wrappers unused by the diagnostic adapter. Parent verifies exact source
comparison and fully reverses the overlay before traffic. The frozen
`ack-invalidation-profile-20260910` binary runs with periodic PERF, samples0 and
the same selected events as dispatch-inner; no scheduling decision changes.
Runner6884 exits0 in 61.007750s. The [verified raw archive](ACK_INVALIDATION_PROFILE_20260910.raw.tar.gz)
is 752,474B: 11 regular files, comprising five results, build/driver logs, complete
patch, wrapper, `run.py` and `shape.sh`. Gzip, tar comparison and every decompressed
member's bytes pass; runner/profile are identical to the preceding dispatch-inner
archive. No configs, binaries, symlinks or directory entries are included.

### Exact completion and complete raw series

All 504,627,200 locally accepted bytes are target-confirmed in 60.621599s,
66.594Mbps, one completed stream, no failed streams or probe errors. First
write/confirmation are .105291/.409849s; maximum write/confirmation gaps are
4.058593/7.746007s. This is a duration-upload workload, not an echo test. Settlement
extends 20.621599s past nominal 40s load; the actual source-read counter reaches its
final value only by management sample43. These 61 raw confirmation bins contain
26 zeros; the last bin is partial. No trimmed-series time origin is substituted.

```text
raw bin start (s): receiver-confirmed Mbps
 0: 9.551,135.983,54.43,0,70.447,0,133.169,98.566,106.098,10.486
10: 37.653,27.219,109.344,90.619,64.182,156.886,85.475,120.562,180.144,119.252
20: 81.122,0,0,0,.524,0,0,917.735,63.439,6.728
30: 0,0,14.384,0,93.323,36.892,49.903,38.273,275.923,70.618
40: 11.01,36.286,0,0,0,0,0,0,71.259,0
50: 0,0,250.862,0,0,0,0,0,0,0
60: 408.672
```

| Raw confirmation phase | Mean Mbps | Zero bins |
|---|---:|---:|
| 0–5s | 54.082 | 1/5 |
| 5–15s | 67.734 | 1/10 |
| 15–25s | 74.397 | 3/10 |
| Interior16–24 inclusive | 65.231 | 3/9 |
| 25–40s | 104.481 | 5/15 |
| 40–61s, last partial | 37.052 | 16/21 |

### Successful ACK partition and actual work

The feature flag records the existing validated subsumption fast path, not
`released_bytes == 0`. Its caller records only successful ACK applications and
captures recovery-dirty immediately before the ordinary assignment to true.
The three `request.ack_facts.*_count` labels use **record count**, with byte
fields0 and synthetic1us/event. Those artificial durations are not processing
cost. Through the closing flush at Unix1789054681194:

| Successful caller outcome | Count |
|---|---:|
| New facts | 6,094 |
| Already subsumed | 22,774 |
| Total successful ACKs | 28,868 |
| Subsumed, actual false→true dirty transition | 17,870 |
| Subsumed while already dirty | 4,904 |

Subsumed ACKs are 78.89% of successes; actual re-dirtying is 61.90% of successes
and 78.47% of subsumed ACKs. These are event frequencies, not saved-work forecasts.
Every interval count/byte/time delta and its accumulated sum reconciles with the
cumulative value across 5,433 client/763 server performance rows, single PID per
role. Inactive components are omitted from later flushes: their last emission
need not share the close timestamp. All three ACK labels do reach the close.

Client actor hold totals 56.815001s over 583,129 guards; actor acquisition wait
totals 1.290763s. Writer hold is 1.102658s/42,259 guards, with .042510s successful
acquisition-call time, not prior Busy/retry residence. Largest actor guard is
23.663ms at the dispatch callsite, so the multi-second hold is repeated work,
not a single observed multi-second lock acquisition or guard.

| Observed owner/dispatch scope | Elapsed, s | Actual calls/events |
|---|---:|---:|
| Dispatch actor guard, instrumented `control.rs:3103` | 35.783692 | 33,881 guards |
| Preselect actor guard, `control.rs:1571` | 14.958354 | 87,051 guards |
| Collection actor guard, `control.rs:3080` | 2.124544 | 33,881 guards |
| ACK actor guard, `control.rs:4051` | 1.861476 | 28,868 guards |
| Direct structural recovery, inside dispatch | 35.588522 | 37,292 calls |
| Queued repair, inside dispatch | .197589 | 30,911 calls |
| Structural target selection | 3.996898 | 1,752,551 calls |
| Shared send-plan construction | 30.264546 | 989,922 calls |
| Fenced Apply | .112075 | 18,964 commits |

Of 989,922 sends, 970,904 return blocked, 54 other errors and 18,964 commit, an exact
partition. Structural no-target events number 771,384; no `native_stale`
component is emitted. Recovery payload committed is 855,744,520B, not unique
repaired bytes or proof that copies were unnecessary. Internal send errors are
not 54 failed user transfers. The four preselect phase timers total 14.883196s:
gap 13.848814, source .470534, retained .452075 and residual .111773. All-caller
inner gap timers, including ACK calls, retain their separate scope; owner/target
model time 4.739885s is not an exclusive child of preselect alone.

Timers measure elapsed work, including descheduling. Owner, direct, send-plan
and Apply timers nest and must not be added. Generic dispatch `bytes` means calls
or events except the explicitly named payload counter; maxima of accumulated
dispatch phases describe per-guard batches. Logging and1us floors remain
observation costs, not a CPU profile or an ordinary performance improvement.

### Exact local winner and complete-flush overlap

One session3544478606622885740/stream0 carries this upload. For the following
unique QUIC joins, serverQ0/path0/incarnation3 maps to clientQ0/physical8/
attachment3; serverQ1/path1/incarnation4 maps to clientQ1/physical1/attachment2.
Incarnation and attachment numbers are not interchangeable. Wall timestamps
below have millisecond resolution; independently zeroed monotonic clocks and
probe offsets are not treated as a common origin.

The 6.991s advancing-reply gap ends when Q0 repair `[1405,1419)` advances the
frontier1405→1419. The previous advance is Unix1789054661990. This carrier has
one matching repair acceptance and decode; the Original is on Q1 and later
arrives as a duplicate. All times in the next table add Unix1789054660000ms.

| Exact event | Offset, ms | Local sequence |
|---|---:|---:|
| Server accepts Q0 stale-path repair | 2705 | server951 |
| Q0 repair decoded/send begin | 2735 | client2191 |
| Repair enqueued | 2752 | client2192 |
| Attachment receives, shared depth131 | 7083 | client2257 |
| Shared Product send accepted, depth131 | 7194 | client2258 |
| Product dequeued / selected / preapply lock finished | 8981 | client2277–2279 |
| Applied; frontier1405→1419 | 8981 | client2280 |

Acceptance→decode is 30ms; decode→Product is 6.246s, including 4.331s from repair
enqueue to attachment and 1.787s from accepted shared send to dequeue. These
exact boundaries establish a material local service delay without requiring a
native-network latency explanation. They do not by themselves assign every
intervening scheduling turn to a particular ACK.

For periodic counters, reconstruct complete sorted flush groups and subtract
cumulative states at fully contained group ends. The strict post-decode interval
Unix1789054663468→1789054668500 spans 5.032s, entirely before application:

| Counter/time in that complete-flush interior | Value |
|---|---:|
| Subsumed / new-fact successful ACKs | 414 /63 |
| Actual subsumed re-dirty / already-dirty duplicates | 136 /278 |
| Actor hold / acquisition wait | 4.904340 / .016365s |
| Dispatch guard / preselect guard | 4.758717 / .053776s |
| Nested direct structural work | 4.755105s;1,527 calls |
| Nested send plans | 4.103400s;98,243 calls |
| Blocked / committed sends | 97,268 /975 |

An even narrower 1.012s full-flush band inside the shared-queue wait
(Unix1789054667488→1789054668500) contains 62 subsumed/9 new-fact ACKs,
16 re-dirties and 46 already-dirty duplicates, 1.000625s actor hold and .979037s
dispatch guard. The .004360s preselect guard cannot explain that band's work
by itself. These nested windows must not be summed. Completed calls can straddle
counter boundaries, and after-unlock emission/flush ordering adds small skew;
there is no per-ACK trigger-to-dispatch trace. Actual re-dirtying is present
during the proven local hold, but its frequency is not the removable dispatch
fraction: independent wakes and useful committed recovery remain active.

### Preserve the different largest-gap outcome

The 7.746s logged frontier gap is Unix1789054672990→1789054680736,
frontier1545 waiting for `[1545,1559)`. Its winning Q1 completion-tail copy is
accepted only at1789054680627 (server1311), decoded0658 (client2493), enqueued0661,
attached0705 and sent to the shared queue0707; Product applies it0736
(client2572). That winner spends 31ms acceptance→decode and 78ms decode→Product.
The earlier Q0 Original, accepted/written at1789054661749, is only observed
decoded at1789054680890, after the winner. Earlier TCP copies likewise cannot
be assigned unseen intermediate boundaries. Thus the largest gap is **not**
proved to be an already-decoded local FIFO hold, even though recovery work is
busy throughout it.

Its strict 7.041s complete-flush interior (Unix1789054673540→1789054680581)
contains 1,502 subsumed/209 new-fact ACKs, 784 re-dirties/718 already-dirty duplicates,
6.883117s actor hold,6.299739s dispatch guard and .097216s preselect guard;
nested send-plan time is5.424103s. Those counters do not resolve the earlier
Original/copies' missing native/reader stages.

### Actual target, native and physical/resource context

All 61 management samples keep the same eight physical outputs and session.
Native telemetry initializes during startup, then all eight epochs remain stable
from client sample10/server sample9 onward. The sampled target has accepted all
504,627,200B by server Unix1789054667252 (about runner47s). Server reply production
reaches 1838B by sample50, while client local reply delivery is 1405B at samples42–48,
1433B at49–52 and1545B at53–60. The final long gap is therefore settlement-return
delay after target writes have completed, not absence of target bulk progress.

Inside the earlier local hold, server samples42→48 show 38,912,896B further target
writes over 6s, while the client reply counter stays 1405B. Inside samples53→60,
source and target counters stay final, yet the six client TCP native ACK counters
increase 137,282,902B in their stable epochs. Native timestamps advance; this is
not new useful application progress and does not identify which copy won.
Client lifetime `ps` CPU is 123→121% across42–48 and 119→118% across53–60;
it is not an interval CPU measurement or proof of continuous saturation.

The sampled physical profile is 200Mbps per independent link, DOWN30ms/UP70ms,
zero jitter/configured loss/outage, netem limit8192, HTB burst/cburst65536.
Only client eth0/link46 UP is first observed at 10Mbps at runner15.002552s and
restored to 200Mbps at25.003620s; link47 remains 200Mbps. All 61 rows satisfy
rate/ceil and delay checks; class and qdisc drop deltas are0. Management target-write rates over
its own5→15,16→24 and25→40 sample intervals are86.340,25.949 and85.166Mbps,
respectively, not substitutes for the differently timed raw confirmation bins.

| Sampled cost, first→last over60.007556s | Client / UP | Server / DOWN |
|---|---:|---:|
| Link46 class bytes | 679,246,055 | 9,492,698 |
| Link47 class bytes | 739,826,221 | 10,100,745 |
| Sum-class peak / final backlog, B | 31,518,748 /4,423,461 | 50,764 /1,686 |
| RSS peak / final, KiB | 346,596 /302,668 | 129,684 /129,684 |
| Lifetime process CPU peak / final, % | 125 /118 | 56.4 /32.2 |

Class bytes are not useful payload; nested qdisc backlogs are not added again.
Client/server logs contain 8,658/2,096 lines and 3,097,063/742,458B, including the
periodic observers. Their overhead is not measured away. Probe stderr is empty;
two server `H3_NO_ERROR` remote-close warnings occur after final recorded
settlement/close, not as independent failed transfers. There is no ordinary
promotion or guarantee that correcting the observed no-op invalidation will
remove either measured long gap. The next bounded correctness/performance
transaction must preserve independent availability wakes and measure its own
whole outcome; this capture supplies attribution, not a new policy or speed claim.

## Ordinary ACK recovery invalidation: useful partial gain, write-gap gate still fails

This separate ordinary run tests only the exact subsumed-ACK no-op correction.
`ClientStreamAckOutcome` carries buffer-release bytes and `has_new_facts` to the
real caller. False is returned only by the existing validated subsumption fast
path; every successful full ACK application returns true, including novel
negative evidence that releases zero bytes. The caller preserves prior dirty
work with `request_recovery_dirty |= has_new_facts`, unchanged buffer release,
idle progress and independent availability wakes. No ACK cadence, publication,
controller, timer or recovery geometry changes. Runtime is `d44ca8e` plus the
saved four-file candidate patch (client/control runtime and both test files).

Forecast: remove recovery scans caused solely by exact no-op ACK invalidation,
while independent model/capacity/deadline wakes and other work remain. The prior
diagnostic establishes reachability and material overlap, not removable seconds.
This ordinary result improves completed bytes, settlement and confirmation gaps
against preserved `d44`, but the maximum write gap worsens and restored service
still stalls. **The exact mechanism is corrected; performance promotion remains
held.** Neither crossing a bandwidth number nor component GREEN clears the gate.

The initial test compilation fails on `Vec` versus required `SmallVec` in the
new fixture, before any assertion. After the fixture correction, all 12 client
checks pass in .41s after 2m13s compilation, including exact positive replay and
new omission with zero released bytes; all 32 affected control checks pass in
.19s, 44 total. The ordinary build55998 takes 3m34s with the existing unused
batch-write helper warning. No observer runs in this capture. Runner23406 exits0
in 46.006965s; result tag remains `ack-recovery-invalidation-0910` although local
completion/report date is September11. The [verified raw archive](ACK_RECOVERY_INVALIDATION_ORDINARY_20260911.raw.tar.gz)
is 257,672B with 13 regular files: five results, build, initial compile failure,
client retry and control-test logs, driver, exact patch, `run.py` and `shape.sh`.
Gzip integrity, tar comparison and every decompressed member's bytes pass.
Runner/profile match the preserved `d44` ordinary archive byte-for-byte. No
configs, credentials, binaries, links or directory entries are included.

### Complete ordinary outcome, not a diagnostic-speed comparison

The comparators below are previously preserved ordinary captures in the same
declared topology/profile, not simultaneous, equal-work or packet-identical
trials. `b0` retains the earlier one-head recovery model; `d44` is this candidate's
direct runtime baseline. The diagnostic6884 result is not a performance control.

| Outcome | Preserved `b0` | Ordinary `d44` | No-op invalidation candidate |
|---|---:|---:|---:|
| Accepted = confirmed bytes | 1,294,925,824 | 419,954,688 | 526,385,152 |
| Exact completed streams | 1/1 | 1/1 | 1/1 |
| Elapsed, s | 41.538802 | 64.942209 | 45.465808 |
| Completed whole Mbps | 249.391 | 51.733 | 92.621 |
| First write / confirmation, s | .105451 /.409741 | .105864 /.408088 | .105228 /.408993 |
| Maximum write gap, s | .545897 | 5.172138 | **7.235997** |
| Maximum confirmation gap, s | .634001 | 9.215061 | 4.145083 |

The candidate completes 25.34% more bytes than `d44` in 29.99% less elapsed time;
the reported whole rate rises 79.04%. Maximum confirmation gap falls 55.02%,
but maximum write gap grows 39.90%. This is a useful partial ordinary outcome,
not a causal estimate from one chronological comparison. Probe status is `ok`,
terminal accounting exact, errors empty, and there is no echo workload or
censoring. Nominal offered duration is 40s; source backpressure means the last
local acceptance need not occur at 40s. Final settlement extends 5.465808s beyond
that nominal endpoint. All 46 raw confirmation bins follow, with 11 zeros;
the final bin is partial. Trimmed bins are not substituted for wall-clock history.

```text
raw bin start (s): receiver-confirmed Mbps
 0: 6.856,93.492,167.868,43.42,16.777,0,184.217,62.204,74.54,77.166
10: 245.559,91.607,185.218,43.324,0,49.047,193.922,150.22,118.473,96.484
20: 156.991,118.307,81.7,0,22.493,206.909,73.708,0,222.78,143.2
30: 236.072,36.988,0,0,0,6.12,0,0,5.243,319.121
40: 0,0,128.768,206.967,221.731,123.592
```

| Raw confirmation phase | `b0`, Mbps | `d44`, Mbps | Candidate, Mbps | Candidate zeros |
|---|---:|---:|---:|---:|
| 0–5s | 262.228 | 75.416 | 65.683 | 0/5 |
| 5–15s, pre-cut | 308.068 | 87.670 | 96.384 | 2/10 |
| 15–25s | 34.658 | 55.397 | 98.764 | 1/10 |
| Interior16–24 inclusive | 16.203 | 61.552 | 104.288 | 1/9 |
| 25–40s, restored | 328.092 | 41.114 | 83.343 | 6/15 |
| Own post40s bins, last partial | 349.816 over40–42 | 37.407 over40–65 | 113.510 over40–46 | 2/6 |

Cut service improves materially versus both preserved comparators, while healthy
and restored performance remains far below `b0`. Startup0–5s is worse than `d44`.
The zero bins at32–34 and36–37 preserve the restored failure that the whole mean
would hide; no claim of continuous service follows from the faster bursts.

### Own stalled phases: target and reply holds coexist

The ordinary probe saves maximum gap magnitudes but not their exact endpoints.
The following sampled plateaus are therefore not labelled the exact7.235997s
write or4.145083s confirmation interval. Management's source read, successful
target-socket write, sink-reply read and client local-reply write are separate
counters, and their producer timestamps are not a saved probe wall-clock origin.

| Management sample | Source read, B | Target write, B | Server reply read, B | Client reply write, B |
|---|---:|---:|---:|---:|
| 32 | 467,801,688 | 401,298,232 | 1,496 | 1,482 |
| 34 | 467,801,688 | 437,113,884 | 1,580 | 1,482 |
| 35 | 467,801,688 | 437,113,884 | 1,580 | 1,482 |
| 37 | 467,801,688 | 437,113,884 | 1,580 | 1,496 |
| 39 | 468,407,096 | 437,113,884 | 1,580 | 1,510 |
| 40 | 510,941,700 | 444,541,732 | 1,622 | 1,622 |
| 42 | 511,650,596 | 473,995,758 | 1,664 | 1,622 |
| 43 | 526,385,152 | 479,799,638 | 1,720 | 1,650 |
| 45, last sampled | 526,385,152 | 513,633,572 | 1,860 | 1,860 |

Source is fixed over samples32–37, then adds only605,408B through39. Target is
fixed at437,113,884B from server Unix1789056153140 through1789056158140
(samples34–39, exactly5s). The return prefix also advances slowly. This is not
solely a final reply hold after all target bytes arrived: forward target service
is still incomplete. Sample45 ends before the target's final12,751,580B are
observed; the exact probe, not that last management row, proves full settlement.

During target-flat34→39, the two used TCP outputs' native forward ACK counters
increase14,458,224/15,246,028B; both QUIC counters increase30,502B each. Epochs
are unchanged and used producer timestamps advance. The small idle TCP counters
sometimes retain old stamps and are not described as freshly polled continuous
progress. Native bytes while target is flat are not useful ordered bulk bytes
or a proof of any particular necessary/wasteful copy.

Client Product flight falls29,591,012→20,985,914B in that band. QUIC46/47 retains
11,581,922/17,047,778B initially and11,064,114/9,448,112B finally despite sampled
native QUIC flight0 at both endpoints of the band. Native QUIC flight limits
are approximately5.65/6.15→5.68/5.82MB; they are not free Product recovery credit.
Client total queued bytes range0–102,244B, not a measurement of command-lane
permits or readiness. Management exports no command-slot occupancy/full predicate,
so neither large Product debt nor a zero total queue proves a blocked/full lane
or a feasible next repair. Native TCP sender windows remain large and bytes
advance, while the two used client TCP receive queues fall45,126/37,942→
23,149/15,690B. Those unread bytes are not identified as the critical reply.

Client lifetime `ps` CPU moves125→123% in34–39; RSS stays352,956KiB. These are not
interval CPU or owner-hold timers. Without diagnostic boundaries or exact queued
recovery ownership, the ordinary capture cannot assign the remaining stall to
the old repeated-work mechanism, a new lost wake, or native/read service.
It also cannot establish that the small source correction caused the larger
maximum write gap. The adverse user-visible result remains a gate failure.

### Matched physical profile and complete sampled costs

All 46 samples verify the same direct independent links:200Mbps each,
DOWN30ms/UP70ms, zero configured loss/jitter/outage, netem limit8192 and HTB
burst/cburst65536. Client eth0/eth1 are links46/47; server eth1/eth0 are46/47.
Only46UP is first observed at10Mbps at runner15.001773s and restored at25.002925s;
47 stays200Mbps. Every class rate equals its ceiling, and all class/qdisc drop
deltas are0. One session5226093404294122106 keeps eight active physical outputs,
none suspect/failed. All eight native epochs are initialized and stable from
sample9 onward. No compiler overlaps the ordinary run.

| Actual target-socket sample window | `b0`, Mbps | `d44`, Mbps | Candidate, Mbps |
|---|---:|---:|---:|
| 0→5s | 273.573 | 87.037 | 155.060 |
| 5→15s | 321.800 | 94.412 | **75.944** |
| Strict16→24s | 17.028 | 59.496 | 119.326 |
| 25→40s | 340.910 | 62.647 | 68.915 |

Target rates use actual management producer time differences. The pre-cut
target rate worsens versus `d44` despite better raw confirmation in the nominal
phase: differently timed counters and previously buffered replies must not be
equated. Restored target gain is much smaller than the raw-confirmation gain.

| UP class service, Mbps | `d44`46 /47 | Candidate46 /47 |
|---|---:|---:|
| 0→15s | 100.203 /145.756 | 89.136 /146.906 |
| Strict16→24s | 10.019 /137.332 | 10.023 /195.684 |
| 25→40s | 72.552 /94.617 | 112.396 /91.589 |

| Whole sampled cost | `d44` | Candidate |
|---|---:|---:|
| Sample window, s | 65.007846 | 45.006732 |
| UP46 /47 class bytes | 423,938,161 /705,498,359 | 467,368,412 /749,700,687 |
| DOWN46 /47 class bytes | 6,673,253 /8,278,284 | 7,015,954 /8,612,499 |
| Summed UP backlog peak / final, B | 31,303,275 /424,756 | 28,677,318 /12,763,272 |
| Summed DOWN backlog peak / final, B | 38,033 /810 | 46,620 /13,425 |
| Client RSS peak / final, KiB | 410,640 /410,640 | 355,388 /342,712 |
| Server RSS peak / final, KiB | 123,852 /123,852 | 75,776 /75,776 |
| Client lifetime CPU peak / final, % | 124 /116 | 126 /123 |
| Server lifetime CPU peak / final, % | 45.9 /26.8 | 66.2 /37.8 |

More completed work coexists with more wire bytes and higher lifetime CPU values,
but lower peak RSS. Different durations and final in-flight phases prevent
treating final backlog/RSS as a post-teardown leak or a matched settled-state
cost comparison. Class traffic is not unique payload, and hierarchical qdisc
backlogs are not summed twice. Candidate client log and probe stderr are empty;
two server `H3_NO_ERROR` remote-close warnings occur during normal teardown,
with no probe error or incomplete transfer. Existing HTB notices are retained.

Disposition: the forecast receives **partial practical support**, including
better independent-cut service and shorter settlement with more delivered work.
It does not prove the eliminated ACK transitions caused the full gain, and the
remaining4.145s confirmation gap, worsened7.236s write gap and poor restored
service prevent performance promotion. Preserve this result and the narrow
correctness checkpoint separately; no threshold rescue, diagnostic-as-baseline
comparison or favorable rerun is justified by this one outcome.

## Repair-plan refusal diagnostic: early queue-negative work, absent late bound-plan activity

This small diagnostic resolves the actual refusal branch but **does not select
a queue-readiness filter**. Of 308,051 recorded bound plans, 299,219 (97.133%)
fail in the chooser after existing enqueue checks observe false and never true;
8,832 succeed and reach authority. Yet 93.217% of those refusals have accumulated
by about20.1s, while a strictly contained4.042s interval in the later target
plateau contains **no recorded bound-plan attempt**. Early count dominance
cannot be assigned to that late stall or turned into removable CPU time.

Source is ordinary `a16b404` plus a temporary three-file count-only overlay;
parent freezes it and fully restores ordinary source before traffic. Build takes
3m35s with the existing unused-helper warning. There is no full owner-timing or
per-reply-stage overlay. Runner61411 exits0 in75.010718s. The [verified raw archive](REPAIR_PLAN_REFUSAL_PROFILE_20260911.raw.tar.gz)
contains11regular files/595,627B: five results, build/driver, exact patch, wrapper,
`run.py` and `shape.sh`. Gzip, tar and every decompressed member's bytes pass;
runner/profile match the preceding ordinary archive. No configs/binaries/links
are included. The wrapper enables PERF and literally sets `MPTUNNEL_LAB_SAMPLES=0`;
source recognizes `MPTUNNEL_LAB_PERF_SAMPLES`, not that variable. No sample records
actually occur, so the capture is periodic-only without attributing that fact
to the misspelled variable.

### Exact partition and its temporal falsifier

Count `total_bytes`/`interval_bytes` as **attempts**, not payload. `total_count`
counts emitted batches; synthetic1us per record is not measured plan cost.
Scope is bound `ClientStalePathReinjection`/`ClientPathFailureReinjection` inside
the synchronous dispatch guard, not every recovery path. TLS accumulation emits
after Product unlock without rereading the original predicates.

| Flushed outcome | Attempts | Emitted batches |
|---|---:|---:|
| `chooser_blocked_queue_false` | 299,219 | 5,366 |
| `success` | 8,832 | 2,211 |
| `authority_reached`, subset of success | 8,832 | 2,211 |

The other ten exclusive plan exits and three post-plan refusals have no emitted
component, including unclassified, captured eligibility and target/proof/load
refusal. Thus the twelve-way plan partition is308,051, and success equals
post-target + post-proof + post-load + authority-reached at the final observed
counters. Queue-negative means at least one evaluated enqueue predicate was
false and none true in that failed attempt; it does not distinguish full permits
from closed/inactive output, continuous fullness, or exact free slot count.

All interval count/byte/time deltas and sums reconcile for1,046client and914server
performance rows. Per-component recording/flush is not atomic: one group at
Unix1789057441388–1389 has success5,183 versus authority5,049, then the next
group at7442389–2390 reconciles8,814each. Do not interpret that transient134
split as a missing authority outcome. Client logs have periodic reasons only:
last bound counters are at7443393, with subsequent periodic activity through
7446397 and no newer bound values, not a separately recorded close flush.

By Unix1789057391731 (20.109s after first management timestamp),278,923 refusals
have accumulated; by7396811,280,555 (93.762%). The following differences use
cumulative states at complete periodic-group ends strictly inside each coarse
management band; boundary groups are excluded, so rows do not sum to the whole.
Completed scopes can straddle a flush; counts supply no exclusive time partition.

| Management band | Actual full-flush span, s | Queue-negative / success | Target-write increase across management band, B |
|---|---:|---:|---:|
| 5–15s | 9.029 | 110,787 /1,908 | 193,365,032 |
| 16–24s | 7.043 | 100,288 /1,743 | 18,917,936 |
| 25–40s | 14.197 | 3,071 /146 | 56,067,224 |
| 40–74s | 33.370 | 15,409 /4,468 | 28,449,256 |

Own reply delivery stays1115B at samples37–45 while target writes increase
362,834,384→365,312,768B. Its7.094s full-flush interior contains966negative/
94successful plans. Later, local replies stay1157B at53–61: the7.076s interior
contains only29negative/10successful plans. Most decisively, target writes stay
367,459,576B at57–62 while source grows375,701,312→384,290,876B. The fully
contained Unix1789057429233→1789057433275 interval spans4.042s and contains0
plans of any recorded kind. This counterevidence blocks treating a filter for
these plans as the immediate late-stall correction. Other unobserved work,
unavailable recovery authority and return service are not distinguished here.
There are no exact reply-frame stages or saved probe gap endpoints to import
from the prior capture.

### Own complete service and costs, not an ordinary performance comparison

All392,364,032B settle exactly in74.605760s,42.073Mbps,1/1complete, no probe errors.
First write/confirmation are .105813/.412161s; maxima are5.791800/9.391853s.
No echo workload is present. All75raw confirmation bins below contain42zeros;
last bin is partial. Settlement extends34.605760s beyond nominal40s offering,
and actual source acceptance reaches its final value only by sample65.

```text
raw bin start (s): receiver-confirmed Mbps
 0: 13.317,143.419,327.156,48.234,0,129.359,87.543,89.378,216.103,79.455
10: 274.439,117.81,195.75,39.545,0,0,45.959,295.599,150.652,78.058
20: 2.112,0,4.075,0,2.352,0,0,0,2.193,0
30: 2.591,0,0,0,0,0,40.533,0,0,0
40: 0,0,0,0,0,44.835,0,0,0,0
50: 0,0,32.41,0,0,0,0,0,0,0
60: 0,5.863,0,0,0,0,37.845,37.623,241.289,168.664
70: 0,.62,52.359,0,131.773
```

Raw means for0–5 /5–15 /15–25 /16–24inclusive /25–40 /40–75 are
106.425 /122.938 /57.881 /64.312 /3.021 /21.522Mbps. The restored25–40
interval has12/15zero bins. Actual target-write rates over its own5→15,
16→24,25→40 and40→65 sample intervals are154.677/18.916/29.903/1.218Mbps.
They are not the raw confirmation clock or evidence that earlier queued bytes
were useful. By sample71 all target bytes and2150reply bytes exist; client
delivery remains2095B at71,2109B at72 and2137B at73–74 before final settlement.

In the target-flat57–62 band, client total queue is0 at every sample; Product
flight remains52,819,916→50,539,708B, including QUIC46 debt44,059,172→43,326,276B
despite its native flight0 and fresh native stamps with no new native ACK bytes.
QUIC47 adds45,030native ACK bytes and the two used TCP outputs add58,668/14,692B:
small progress is not ordered bulk service. Idle TCP stamps sometimes do not
advance. Client lifetime CPU stays112%, RSS328,776→326,408KiB; these are not
interval CPU or held-lock measurements. No command-slot or exact critical-copy
ownership is exported, so zero total queue is not proof of admission readiness.

All75profiles verify the unchanged two200Mbps links, DOWN30ms/UP70ms, zero
configured loss/jitter/outage, burst/cburst65536 and netem limit8192. Only46UP
is observed at10Mbps from runner15.004350s, restored at25.005402s;47 stays200.
Class/qdisc drops remain0. Eight physical outputs remain stable; all native
epochs are initialized and stable from sample10. Cost window is74.010520s:

| Sampled cost | Client / UP | Server / DOWN |
|---|---:|---:|
| Link46 /47 class bytes | 345,119,213 /404,303,713 | 6,380,915 /5,872,876 |
| Summed class backlog peak / final, B | 25,782,577 /300 | 37,954 /0 |
| RSS peak / final, KiB | 328,776 /321,156 | 69,868 /69,868 |
| Lifetime process CPU peak / final, % | 132 /111 | 53.2 /17.9 |

Logs contain only periodic/counter output:435,733client/383,021server bytes;
probe stderr is empty. Observation overhead is not measured away, and these
results do not replace the ordinary ACK-correction capture. Information outcome:
the refusal branch is identified, but its timing contradicts attributing the
largest late stalls to continued execution of those plans. No queue-filter
implementation or performance promotion is selected by this result.

## Current late-owner diagnostic: preselect and direct recovery dominate different intervals

On current `a16b404`, local Product ownership remains material, but no single
whole-run caller explains every interval. A restored slow-service band spends
4.394542s of a5.024s measured interior in preselect, predominantly its gap
evaluator; queued dispatch consumes only .000747s. A later reply plateau instead
has1.250684s direct-dispatch-owner time and .558662s preselect in2.014s.
This locates current work, not an exact critical reply's residence or an
ordinary performance improvement. It does not revive the rejected queue filter
or make the preceding classifier's different late plateau a dispatch stall.

Build28234 completes in3m36s; the saved seven-file periodic observer is frozen
and fully reversed before traffic. It includes caller-keyed owner timing,
preselect and inner recovery phases, but **no per-reply-stage trace**. Correct
`MPTUNNEL_LAB_PERF_SAMPLES=0` is used; no sample rows occur. Warnings are the
existing unused batch helper and ordinary wrappers unused by this diagnostic.
Runner90510 exits0 in51.014213s. The [verified raw archive](LATE_OWNER_PROFILE_20260911.raw.tar.gz)
is500,562B/11regular files: five results, build/driver, exact patch, wrapper,
`run.py` and `shape.sh`. Gzip, tar comparison and every decompressed member's
bytes pass; runner/profile match the preceding classifier archive. No configs,
binaries or links are included.

### Actual intervals and dominant owner

All source/target/reply counters below use their own management producer time.
The probe does not retain exact maximum-gap endpoints. Periodic counter windows
subtract cumulative states at complete groups wholly contained within each
management band; boundaries and nesting remain explicit. The static callsite
map is reconstructed in memory from `a16b404` plus the saved patch, not guessed
from today's source lines: instrumented control1569preselect,3101dispatch,
3078collection and4032ACK application.

| Own sampled band | Complete-flush interior, Unix ms | Span, s | Actor hold, s | Preselect guard, s | Dispatch guard, s | Queued child, s |
|---|---|---:|---:|---:|---:|---:|
| Target flat17–25 | 1789058811218→1789058818253 | 7.035 | 6.904566 | 1.303594 | 5.030220 | .001163 |
| Restored slow29–35 | 1789058823274→1789058828298 | 5.024 | 4.960852 | 4.394542 | .035209 | .000747 |
| Target flat39–41 | 1789058833320→1789058834323 | 1.003 | .977568 | .695865 | .134804 | .000207 |
| Reply flat43–46 | 1789058837335→1789058839349 | 2.014 | 1.945711 | .558662 | 1.250684 | .000659 |
| Target flat45–47 | 1789058839349→1789058840352 | 1.003 | .979794 | .307918 | .599702 | .000160 |

Target writes are231,822,908B at17–25,393,940,374B at39–41, and454,116,882B
at45–47. The restored29–35 band is slow rather than flat: source grows
326,953,294→364,711,198B, target306,333,454→318,899,718B (16.755Mbps over6s),
and local replies745→829B. At43–46, source is already final459,276,288B;
target412,172,598→454,116,882B and server replies1403→1501B advance, while local
reply delivery stays1403B. These different producer states must not be merged.

In the restored5.024s interior, preselect's directly timed gap phase is4.355900s;
source/retained/residual total only .034484s. Actor acquisition wait is .022489s,
writer hold .009402s, and ACK guard .447779s. Inner gap counters cover **all
callers**, including that ACK guard, so they are not an exclusive preselect
partition: owner/target model3.179285s, scored metadata .411250s, cache .251782s,
assignment clock .335958s, boundaries .028315s and ownership view .037609s.
There are435,745 scored/present-owner queries; their mask/frontier time is
.241829s. The97,544 stable-absent queries consume only .046474s, with no dynamic-
absent increase. Small stale-only pruning and queued retry are not the dominant
work in this band. The separate observation-classification timer is .014756s.

During the later2.014s reply plateau, direct work is1.247611s and send plans
1.042342s/41,337attempts (40,553blocked,770committed,14other errors). During the
7.035s cut plateau, direct work is5.035808s and plans4.265653s/187,805attempts,
187,153blocked/652committed. These are materially different from the restored
preselect-dominant band. Parent/child timings overlap; small boundary skew can
make an emitted child total slightly exceed its corresponding owner delta.
Do not sum overlapping windows or call these elapsed durations CPU execution.

### Whole counter accounting and scope

All interval count/byte/time deltas and sums reconcile across4,509client and
660server perf rows, one PID per role. Client closes with a
`multipath_stream_close` flush through Unix1789058844522, server `stream_close`
through8844589; inactive components retain older last timestamps. Dispatch
timing `bytes` counts calls, count-label `bytes` counts events, and only explicit
payload bytes represent data. Generic record counts are emitted batches;
synthetic1us floors are not event costs or per-attempt maxima.

| Whole scope | Elapsed, s | Calls/guards or events |
|---|---:|---:|
| Actor hold / acquisition wait | 46.672479 /1.298395 | 401,855 guards |
| Writer hold / successful acquisition-call time | 1.210977 /.046769 | 46,676 guards |
| Preselect guard / gap child | 22.012764 /21.224716 | 62,811 guards |
| Dispatch guard / direct child / queued child | 18.946815 /18.795869 /.152581 | 15,652 guards;21,996direct/17,860queued calls |
| ACK guard / collection guard | 2.978602 /1.126054 | 25,789 /15,652 guards |
| Shared send plans | 15.764799 | 668,540 attempts |
| Authority / reservation / fenced Apply | .027788 /.035540 /.125994 | 19,014 /19,014 /15,370 |
| Product commit, inside Apply | .072546 | 15,370 |

All sends partition into652,828blocked +342other errors +15,370commits.
Recorded recovery payload is623,797,774B, not unique useful repair or proof of
unnecessary copies. No native-stale component occurs. Largest dispatch/preselect
guard is19.022/13.136ms: observed seconds accumulate across many short guards,
not one multi-second lock acquisition. The four preselect phase timers total
21.951128s; their source/retained/residual values are .315575/.327231/.083606s.
All-caller gap owner/target model totals10.359173s/1,297,003calls; it cannot be
subtracted from preselect alone. Writer wait omits prior Busy/retry residence,
and observer recording/classification remains within some enclosing timers.

### Complete own outcome and raw history

The probe completes exactly459,276,288B in50.410371s,72.886Mbps,1/1stream,
no errors. First write/confirmation are .105804/.410417s; maximum write and
confirmation gaps are9.271768/4.711260s. No echo workload or censoring occurs.
The cut includes source unchanged at237,146,714B over samples16–24, but absent
gap endpoints prevent assigning the exact9.271768s maximum to those rows.
Source reaches its final count by42; at last sample50 target writes are still
237,490B short of final, so the probe supplies final settlement, not management.
All51raw bins, including22zeros and the partial final bin, follow:

```text
raw bin start (s): receiver-confirmed Mbps
 0: 5.115,125.502,0,219.817,0,0,71.591,241.46,217.956,169.987
10: 0,35.837,209.571,0,28.215,0,0,0,0,35.775
20: 0,0,0,0,478.744,64.957,0,123.704,37.845,0
30: 239.992,0,72.749,53.654,0,119.775,0,76.015,84.583,247.12
40: 131.11,123.137,41.559,0,0,0,42.135,0,35.792,10.558
50: 329.955
```

Raw0–5 /5–15 /15–25 /16–24inclusive /25–40 /40–51 means are
70.087 /97.462 /51.452 /57.169 /74.693 /64.931Mbps; zero-bin counts are
2/5,3/10,8/10,7/9,5/15,4/11. These are not native capacity estimates or
ordinary comparisons against the smaller classifier or previous candidate.

All51profiles preserve independent200+200Mbps, DOWN30ms/UP70ms, zero configured
loss/jitter/outage,65536Bbursts and8192netem limit. Only46UP is observed at10Mbps
at15.004332s, restored at25.006070s;47 stays200. All class/qdisc drops are0.
Eight physical outputs stay stable; all native epochs are initialized and
stable from sample8. In restored29–35, native forward ACKs add6,092,428TCP and
14,010,685QUIC bytes while Product flight falls32,169,412→21,297,384B. At35,
native flight and total queue are0 but Product ownership remains substantial;
that is not exact copy/command authority or evidence of continuous readiness.
Client lifetime CPU121→118% in that band is not interval CPU; the directly
observed owner durations, not `ps`, locate the repeated synchronous work.

| Sampled cost,50.013981s window | Client / UP | Server / DOWN |
|---|---:|---:|
| Link46 /47 class bytes | 547,712,017 /591,614,863 | 7,166,317 /7,214,463 |
| Sum-class backlog peak / final, B | 22,515,830 /352,272 | 43,116 /3,200 |
| RSS peak / final, KiB | 371,612 /366,628 | 113,972 /113,972 |
| Lifetime CPU peak / final, % | 124 /121 | 45.5 /32.3 |

Client/server logs are1,919,810/277,890B; probe stderr is empty. Two server
`H3_NO_ERROR` warnings occur during post-completion teardown, not as failed
transfers. No post-teardown retention inference follows from the last sample.
Information forecast is met: current repeated owner work is material, with
preselect gap scoring dominant in one restored band and direct planning in
others; queued work and mutex acquisition are small there. Exact per-reply
causality, a safe work correction and ordinary practical acceptance remain
separate obligations. No runtime change or performance promotion follows from
these nested totals alone.

## Ordinary recovery-attached observation: practical gate fails

The six-file candidate atop `a16b404` removes unused global Original-admission
projection from the fresh lower recovery observation. It introduces internal
Attached/Recovery/BulkAdmission scopes: Recovery preserves the old bulk scope's
health maintenance, measured attachment evidence and proof qualification without
building global candidates or latency pressure. Existing boolean callers keep
their previous mapping; only the lower recovery model changes scope, including
its completion-tail consumer. No clock, rate, recovery geometry, native policy
or public knob changes. The final source patch is preserved with the capture.

Forecast: remove genuinely unused work, but do not equate the earlier model
timer with removable time or promise a throughput gain. The sole ordinary
comparator is the preserved `a16` run23406, not any diagnostic capture. This
chronological same-profile comparison is not packet-identical or equal-work.
**Reject performance promotion:** exact completion survives, but delivered work,
settlement, pre-cut/cut/restored service and both maximum gaps are materially
adverse. Startup confirmation improves; it does not clear these failures.
This result does not establish that deleting the projection caused every stall.

### Source proof and reproducibility

The initial actual-producer RED compiles in1m11s and reaches the intended final
assertion: first/successor global builds `(1,1)`, expected `(0,0)`, after the
semantic and ordinary-positive controls. The first GREEN compilation takes
2m16s; all12client checks pass, but an added equivalence fixture fails before
scope comparison because its attachment retained proof generation0 after the
physical install advanced health generation1. The fixture is corrected through
real proof retry/admission/ACK, with mismatch-to-match checks, not weaker runtime
evidence. Retry compilation takes27.86s; remaining checks pass. There are28
distinct focused passes, not an additional count for the repeated negative test.

Ordinary build47114 completes in3m31s with the existing unused batch-helper
warning. The frozen executable is `bin/recovery-attached-observation-20260911/mptunnel`;
there are no diagnostic flags or compiler overlap. Runner44791 exits0 in
75.009488s. The [verified raw archive](RECOVERY_ATTACHED_OBSERVATION_ORDINARY_20260911.raw.tar.gz)
contains15regular files/467,166B: five results, RED/initial GREEN/retry/build/driver
logs, RED/initial/final patches, `run.py` and `shape.sh`. Final patch has six
source files. Gzip, tar comparison and every decompressed member's bytes pass;
runner and shape match the ordinary `a16` archive byte-for-byte. No configs,
credentials, binaries, symlinks or directory entries are included.

### Complete outcome and all raw confirmation bins

| Outcome | Ordinary `a16`, run23406 | Recovery-attached candidate |
|---|---:|---:|
| Accepted = confirmed bytes | 526,385,152 | 298,516,480 |
| Exact completed streams / errors | 1/1 /0 | 1/1 /0 |
| Elapsed, s | 45.465808 | 74.611477 |
| Whole confirmed Mbps | 92.621 | 32.008 |
| First write / confirmation, s | .105228 /.408993 | .105916 /.409137 |
| Maximum write gap, s | 7.235997 | **26.570694** |
| Maximum confirmation gap, s | 4.145083 | **22.436904** |
| Nominal40s endpoint to final settlement, s | 5.465808 | 34.611477 |
| Raw bins / zero bins | 46 /11 | 75 /55 |

The candidate completes43.29% fewer bytes in64.10% more time; whole goodput
falls65.44%. Write/confirmation maxima grow3.67/5.41times. Both probes are exact,
status `ok`, without censoring or echo workload. Status and eventual completion
are not practical acceptance. Source backpressure can extend the last local
acceptance beyond the nominal40s offered duration.

| Raw confirmation phase | `a16`, Mbps | Candidate, Mbps | Candidate zeros |
|---|---:|---:|---:|
| 0–5s | 65.683 | 114.204 | 0/5 |
| 5–15s, pre-cut | 96.384 | 21.158 | 8/10 |
| 15–25s | 98.764 | **0** | 10/10 |
| Interior16–24 inclusive | 104.288 | **0** | 9/9 |
| 25–40s, restored | 83.343 | 52.364 | 9/15 |
| Own post40 bins, last partial | 113.510 over40–46 | 23.431 over40–75 | 28/35 |

```text
raw bin start (s): receiver-confirmed Mbps
 0: 8.693,206.333,269.741,58.623,27.628,163.053,48.523,0,0,0
10: 0,0,0,0,0,0,0,0,0,0
20: 0,0,0,0,0,0,0,0,5.095,0
30: 0,14.003,0,12.692,179.76,546.456,27.455,0,0,0
40: 0,0,41.419,17.796,0,0,0,0,0,0
50: 0,0,0,0,0,0,0,0,0,0.096
60: 0,0,0,0,0,0,55.979,0,0,0
70: 0,481.468,12.085,0,211.235
```

The untrimmed history preserves the long pre-cut-to-restored confirmation
silence. Large later confirmation bursts include buffered service; they are not
instantaneous physical link rates or evidence that intervening zeros are benign.

### Own source, target and reply plateaus

Management counters below mean client source-read, successful server target-
socket write, server sink-reply read and client local-reply write respectively.
They are not interchangeable receiver frontiers. The ordinary probe saves gap
magnitudes without endpoints, so no sampled plateau is asserted to be the exact
26.570694s write or22.436904s confirmation interval.

| Sample | Source, B | Target write, B | Server reply read, B | Client reply write, B |
|---|---:|---:|---:|---:|
| 7 | 165,558,006 | 98,843,342 | 317 | 304 |
| 15 | 165,558,006 | 163,598,390 | 498 | 304 |
| 28 | 165,558,006 | 164,873,110 | 792 | 304 |
| 29 | 165,558,006 | 164,926,646 | 806 | 317 |
| 44 | 270,526,540 | 270,526,540 | 1,184 | 1,100 |
| 59 | 270,526,540 | 270,526,540 | 1,184 | 1,100 |
| 64 | 270,526,540 | 270,526,540 | 1,184 | 1,114 |
| 71 | 282,753,494 | 270,526,540 | 1,184 | 1,128 |
| 72 | 298,516,480 | 271,784,382 | 1,198 | 1,198 |
| 73–74 | 298,516,480 | 298,516,480 | 1,267 | 1,212 |

The first source/reply plateau starts by sample7, before the15s physical cut.
Client reply304B persists from Unix1789060611178 through1789060632178 (21s),
although target writes and already-read replies advance. Source stays unchanged
through sample29. During44–64, source equals target writes, while already-read
reply bytes still lag locally; target remains fixed through71. Client Unix
1789060648179→1789060668179 bounds that20s source plateau. This distinguishes
return service from unfinished then-accepted target writes, but does not locate
the exact reply in a server writer, native carrier, reader or Product actor.
At73–74 all target writes are sampled, but local replies remain short; only the
final probe proves exact settlement after the last management observation.

In44→64, native forward ACK counters add11,862,090TCP/120,562QUIC bytes;
reverse ACK counters add2,652TCP/3,163QUIC bytes. Used producer timestamps
advance with stable epochs; idle TCP stamps can remain old. Client Product
flight declines37,750,916→25,741,410B, including QUIC46/47 debt
18,879,214/18,857,102→17,183,014/8,543,796B. Both QUIC native flights are0 at
the endpoints; their flight limits remain about8.48→8.60MB and5.64MB. TCP47
adds11,729,636native ACK bytes while its reported Original Product flight is0.
These domains do not identify critical copies or justify a useful/wasteful-copy
claim. At66–71 sampled native flight and total queue are0 while Product flight
remains23,376,520→14,722,192B; zero aggregate queue is not command-slot readiness,
available recovery authority or proof of continuous native idleness.

### Matched shape, phase service and costs

All75samples verify independent200+200Mbps, DOWN30ms/UP70ms, zero configured
loss/jitter/outage, netem limit8192 and65536BHTB burst/cburst. Client eth0/eth1
are46/47; server eth1/eth0 are46/47. Only46UP is10Mbps from the sample at
15.001726s through restoration at25.002850s;47 stays200. Rates equal ceilings,
all class/qdisc drop deltas are0. One session10840567746103731421 retains eight
active physical outputs, no suspect/failed state; native epochs remain stable
once all eight are initialized at sample10.

| Actual management target-write phase | `a16`, Mbps | Candidate, Mbps | Candidate UP46 /47 class Mbps |
|---|---:|---:|---:|
| 0→5s | 155.060 | 140.131 | 142.175 /125.016 |
| 5→15s | 75.944 | 60.813 | 66.802 /73.934 |
| Strict16→24s | 119.326 | **.840** | **1.604 /1.866** |
| 25→40s | 68.915 | 56.483 | 71.881 /50.867 |
| Own post40 sample window | 110.547 over40→45 | 6.589 over40→74 | 8.333 /15.221 |

Target rates use their own producer timestamps, class rates the collector
window. Their mismatch with raw confirmation is retained. During the cut even
unrestricted47 is largely unused; neither its configured capacity nor subsequent
bursts explain away zero ordered confirmation. The ordinary capture lacks
owner timers and exact frame events, so it cannot prove removal reduced actual
work or assign the remaining failure to a particular synchronous caller.

| Whole sampled cost | Ordinary `a16` | Candidate |
|---|---:|---:|
| Sample window, s | 45.006732 | 74.009282 |
| UP46 /47 class bytes | 467,368,412 /749,700,687 | 344,644,516 /332,909,957 |
| DOWN46 /47 class bytes | 7,015,954 /8,612,499 | 5,055,643 /4,818,249 |
| Summed UP backlog peak / final, B | 28,677,318 /12,763,272 | 24,305,824 /922,448 |
| Summed DOWN backlog peak / final, B | 46,620 /13,425 | 49,564 /330 |
| Client RSS peak / final, KiB | 355,388 /342,712 | 338,900 /332,632 |
| Server RSS peak / final, KiB | 75,776 /75,776 | 102,180 /102,180 |
| Client lifetime CPU peak / final, % | 126 /123 | 127 /108 |
| Server lifetime CPU peak / final, % | 66.2 /37.8 | 58.7 /14.1 |

Lower bytes and some lower sampled costs accompany much less useful work and a
longer observation, not improved efficiency. Client lifetime CPU declines112→108%
over44–64; it is not interval CPU or measured lock occupancy. Final RSS/backlog
are not matched settled-state or post-teardown retention measurements. Client
log and probe stderr are empty; the366Bserver log contains two `H3_NO_ERROR`
remote-close warnings during teardown after completion, not autonomous failures.

The semantic/work proof passes its targeted checks, but the ordinary practical
forecast is not met. Preserve the adverse result and exact trial independently;
no performance promotion, profile rescue or causal claim from diagnostic timings
is justified. Source retention/removal is the parent's separate disposition.

## Ordinary ACK-gap pending service: mechanism proof, mixed practical outcome

This separate trial starts from `a16b404`, not the rejected projection-removal
candidate. Request ACK validation, release, authoritative gaps, queue pruning
and staleness still apply immediately. Heavy gap enumeration moves from every
preselect/novel ACK into a selected fair Dispatch turn, with pending ownership,
retained prearmed model/capacity futures and the existing absolute deadline.
The job retains no target/range/native/queue snapshot. Actual claim, queue/copy,
membership/qualification, model/capacity and deadline changes invalidate it;
exact replay and unrelated input do not independently rescan frozen work.

The final guard preserves the old queued-send retry predicate: pending gap
discovery can select Dispatch, but a no-action evaluation cannot retry an
otherwise blocked queue early. Per-region fresh ranking/proof/Apply and EOF
ownership remain unchanged. This is not timing-identical relocation: first lazy
observation moves to selected service, and transient shared load deliberately
has no model publication. Frequent real invalidations, one large scan and direct
structural dispatch can still consume service. No new clock/rate/profile knob.

**No performance promotion:** the actual frozen-state work defect is corrected
in its actor control, but this ordinary comparison delivers fewer bytes more
slowly, worsens maximum confirmation gap and loses pre-cut/cut service. Restored
target service, maximum write gap and client peak RSS improve. Preserve both
sides rather than treating either the mean or one tail as the sole outcome.
The only performance comparator is ordinary `a16` run23406; neither the rejected
projection trial nor any diagnostic run is a speed baseline. One chronological
realization does not establish causal effect sizes for each difference.

### Proof sequence and exact artifacts

Initial RED30086 compiles1m43s but fails fixture setup: after source EOF the new
attachment legitimately receives FIN before PathProofData. Corrected82855 then
fails compilation on Debug-formatting a non-Debug command. Neither is the
intended Product failure. After preserving the exact command assertions and
removing those interpolations, retry42394 compiles1m00s and reaches RED in.04s:
heavy enumeration count rises3→9 across exact replay ACKs/reverse DATA, expected
unchanged3. Counts are sampled at actual DATA Apply frontiers1/4; all bytes reach
the sink, model generation stays fixed, alternate repair lane stays full and
the minimum observed gap deadline remains future. This is an actor work proof,
not an isolated capacity-release causality test; prearmed-wait tests remain
separate controls.

Initial GREEN76852 passes33control tests after1m42s compilation. Independent
review then catches the premature queued-drain retry described above. After
that correction, final60243 compiles53s and passes33control,12client and2actual
path-model-publication tests: **47distinct passes**, not48 and not an extra33
for the earlier run. Ordinary build33957 takes3m35s with the existing unused
batch-helper warning. The frozen ordinary executable is
`bin/ack-gap-pending-service-20260911/mptunnel`; no diagnostics, source changes
or compiler overlap occur during runner3923, which exits0 in48.011265s.

The [verified raw archive](ACK_GAP_PENDING_SERVICE_ORDINARY_20260911.raw.tar.gz)
is298,051B/19regular files: five results; seven logs (setup RED, compile-failed
RED, final RED retry, initial/final GREEN, build, driver); five patches (initial/
corrected/final RED and initial/final candidate); `run.py` and `shape.sh`.
Final patch contains four source files plus RFC. Gzip integrity, tar comparison
and each decompressed member's bytes pass; runner/shape match the ordinary
`a16` archive byte-for-byte. No configs, credentials, binaries or links.

### Exact delivery and all48raw bins

| Outcome | Ordinary `a16` | Pending-service candidate |
|---|---:|---:|
| Accepted = confirmed bytes | 526,385,152 | 488,636,416 |
| Exact completed streams / errors | 1/1 /0 | 1/1 /0 |
| Elapsed, s | 45.465808 | 47.180088 |
| Whole confirmed Mbps | 92.621 | 82.855 |
| First write / confirmation, s | .105228 /.408993 | .106295 /.409787 |
| Maximum write gap, s | 7.235997 | 5.519041 |
| Maximum confirmation gap, s | 4.145083 | **4.514583** |
| Nominal40s endpoint to settlement, s | 5.465808 | 7.180088 |
| Raw bins / zeros | 46 /11 | 48 /16 |

Completed bytes fall7.17%, elapsed rises3.77%, whole rate falls10.54%; maximum
write gap improves23.73% while confirmation gap worsens8.91%. Both probes are
status `ok`, exact, without errors or censoring. There is no UP echo workload.
Source backpressure can extend local acceptance past40s; the last bin is partial.

| Raw confirmation phase | `a16`, Mbps | Candidate, Mbps | Candidate zeros |
|---|---:|---:|---:|
| 0–5s | 65.683 | 78.352 | 1/5 |
| 5–15s, pre-cut | 96.384 | 61.285 | 4/10 |
| 15–25s | 98.764 | 64.600 | 4/10 |
| Interior16–24 inclusive | 104.288 | 62.573 | 4/9 |
| 25–40s, restored | 83.343 | 95.116 | 5/15 |
| Own post40 bins, final partial | 113.510 over40–46 | 103.968 over40–48 | 2/8 |

```text
raw bin start (s): receiver-confirmed Mbps
 0: 7.164,211.01,114.435,59.149,0,0,39.846,0,0,8.485
10: 15.108,0,21.88,117.6,409.93,82.838,167.981,25.81,107.407,100.267
20: 161.695,0,0,0,0,565.385,158.955,64.391,33.938,0
30: 0,179.831,0,136.839,0,0,69.686,37.077,129.691,50.952
40: 14.348,0,4.479,77.498,0,452.284,14.737,268.396
```

The restored improvement includes a large release after four zero bins; it
does not demonstrate continuous service. Trimmed bins and burst rates are not
substitutes for elapsed delivery or physical capacity.

### Own target/reply chronology and native limits

Management source-read, successful target-socket write, sink-reply read and
local-reply write are separate counters. The probe has no saved exact maximum-
gap endpoints or wall-clock origin, so the following plateaus are not asserted
to be the exact4.514583s confirmation or5.519041s write intervals.

| Sample band | Source read, B | Target write, B | Server reply read, B | Client reply write, B |
|---|---:|---:|---:|---:|
| 5→14, pre-cut | 117,973,546→142,420,834 | 117,240,650→118,628,906 | 266→434 | 122→213 |
| 21→25 | 273,570,856 unchanged | 206,461,992 unchanged | 756 unchanged | 756 unchanged |
| 29→31 | 379,151,538→385,353,458 | 346,359,538→373,729,586 | 1,022→1,092 | 910 unchanged |
| 34→36 | 455,072,322 unchanged | 387,978,058→442,079,586 | 1,218→1,344 | 1,036 unchanged |
| 43→47 | 488,636,416 unchanged | 466,995,074→488,636,416 | 1,624→1,791 | 1,204→1,400 |

Thus slow service already occurs before the cut; later reply holds also coexist
with substantial target progress. The21→25 target/reply plateau is exactly
server Unix1789063215200→1789063219200. The29→31 and34→36 reply-flat boundaries
are client Unix1789063223201→1789063225201 and1789063228202→1789063230202.
At last sample47 all target writes are observed, but local replies are short;
only the final probe proves settlement after that sample.

During21→25, healthy47 native ACK counters add23,662,844TCP and63,200,972QUIC
bytes while target writes remain fixed. QUIC46 ACKs are flat with advancing
producer stamps21,089,279→25,702,752us and about6.55MBnative flight. TCP46 adds
262,892ACKed bytes, but its stamp advances only20,951,286→21,621,379us across
the4s management band: it is not a fresh per-second poll throughout. Its last
sampled5.69s RTT is therefore cached evidence, not exact interval attribution.
Native epochs are unchanged. Client Product flight shrinks9,186,112→136,608B
while source minus target remains67,108,864B. The unclaimed source queue and
assigned DSN frontier are not exported here; subtracting these domains cannot
prove all outstanding source bytes reached the receiver or identify a hole.

In29→31 and34→36, native forward ACK deltas are respectively
28,429,104TCP/56,107,693QUIC and20,500,258TCP/45,127,247QUIC bytes. That carrier
progress does not locate the delayed reply prefix. Ordinary logs contain no
gap-call counters, owner timers or per-frame joins, so neither removal of actual
work during these intervals nor an actor/native root cause is established.

### Shape and complete sampled costs

All48samples preserve independent200+200Mbps, DOWN30ms/UP70ms, zero configured
loss/jitter/outage, netem limit8192,65536BHTB bursts and equal rates/ceilings.
Only46UP is10Mbps at15.004991s, restored at25.006093s;47 stays200. Client
eth0/eth1 are46/47, server eth1/eth0 are46/47. All class/qdisc drop deltas are0.
Session13210812091047262669 retains eight active physical identities without
suspect/failed states; all native epochs are initialized and stable from10.

| Actual target-write phase | `a16`, Mbps | Candidate, Mbps | Candidate UP46 /47 class Mbps |
|---|---:|---:|---:|
| 0→5s | 155.060 | 187.585 | 155.430 /116.831 |
| 5→15s | 75.944 | **14.396** | 63.555 /28.578 |
| Strict16→24s | 119.326 | **50.273** | 9.918 /142.642 |
| 25→40s | 68.915 | **135.925** | 123.591 /131.877 |
| Own post40 sample window | 110.547 over40→45 | 31.216 over40→47 | 19.305 /180.438 |

Target rates use actual producer times; class rates use their collector window.
The restored target gain is real in this capture, while earlier target service
is worse. Different stage timing prevents equating these with confirmation-bin
means or inferring useful-copy efficiency from class bytes.

| Whole sampled cost | Ordinary `a16` | Candidate |
|---|---:|---:|
| Sample window, s | 45.006732 | 47.011064 |
| UP46 /47 class bytes | 467,368,412 /749,700,687 | 449,416,941 /682,444,780 |
| DOWN46 /47 class bytes | 7,015,954 /8,612,499 | 7,374,429 /11,718,616 |
| Summed UP backlog peak / final, B | 28,677,318 /12,763,272 | 27,630,042 /11,059,663 |
| Summed DOWN backlog peak / final, B | 46,620 /13,425 | 51,434 /2,442 |
| Client RSS peak / final, KiB | 355,388 /342,712 | 325,136 /256,572 |
| Server RSS peak / final, KiB | 75,776 /75,776 | 138,828 /138,828 |
| Client lifetime CPU peak / final, % | 126 /123 | 121 /120 |
| Server lifetime CPU peak / final, % | 66.2 /37.8 | 61.4 /43.7 |

Client RSS improves, server RSS rises; lower UP traffic accompanies fewer
completed bytes, while DOWN traffic increases. Process CPU is lifetime `ps`,
not interval CPU or saved owner cost. Final queue/RSS samples are not matched
post-teardown measurements. Client log and stderr are empty; the366Bserver log
has two normal `H3_NO_ERROR` close warnings after completion, no probe errors.

Disposition: targeted frozen-state quiescence is proved, but the broader
ordinary practical forecast is not cleared. Retain improved restored service
and write-gap evidence alongside worse pre-cut/cut delivery, confirmation tail,
whole throughput and server memory. No accepted stall correction, performance
promotion, tuned rescue or attribution of every difference to the new owner.
The parent rejected practical promotion and reversed all five candidate
runtime/RFC/test files using the exact saved patch: source is back at `a16b404`.
`target/release/mptunnel` remains the frozen rejected executable, not a rebuilt
ordinary baseline; the archived trial and mechanism proof remain evidence only.

## Passive prefix/owner diagnostic: assigned data already delivered, knowledge lags

Run29636 answers the next information question on ordinary `a16b404` plus a
temporary four-file observer, not the rejected pending-service candidate.
Repeated target plateaus have **assigned horizon = receiver ordered frontier =
completed target prefix**, with no receiver reorder, while unassigned source
remains at the client and its positive ACK frontier is far behind. These
bracketed plateaus are neither an outstanding assigned receiver hole nor a
target-write stall. They locate withheld new assignment alongside delayed
positive knowledge; they do not yet locate the delayed ACK's publication,
native, decode or logical-Input stage. No further gap algorithm or performance
promotion follows from this diagnostic.

### Scope, provenance and completeness

Client `request_prefix_state` samples at an existing Product-locked loop head,
after claim reconciliation. Assigned offset, unassigned raw queue, retained
unique unACKed bytes, positive frontier, authoritative gaps, peer MAX and queue
accounting are coherent there; response receive/reorder cursors belong to the
same actor. Nondata queue accounting includes control charges, not just repair
payload. Server snapshots follow successful receive Apply, before the batch's
target write. Its completed-target watermark advances only by successful
returned batch bytes, including unlogged batches, not by the receive cursor.
Management target bytes instead count successful socket `poll_write` bytes:
partial writes/pending flushes can separate these two target domains.

Both new snapshots select session13273794050165890851/stream0 and emit at most
once per second at existing service points. Sampled positive target writes have
paired begin/end events. No background observer or Product clock/queue change.
The two reused files provide caller-keyed owner wait/hold only, not the prior
inner-plan/gap timers. The selected existing receive-hole events remain enabled;
PERF=1/PERF_SAMPLES=0 produces no per-call sample records.

Initial build18663 fails because SessionId lacks Display; the `.0` formatting
correction changes no decision. Retry completes in3m33s, with the existing unused
batch-helper warning. The final four-file overlay is frozen and fully reversed
before traffic; executable `bin/prefix-owner-trace-20260911/mptunnel` is explicit.
Runner exits0 in56.007191s. The
[verified archive](PREFIX_OWNER_TRACE_20260911.raw.tar.gz) is464,662B/13safe
regular files: five results; failed-build/retry/driver logs; initial/final
four-file patches; wrapper; `run.py`; `shape.sh`. Gzip integrity, tar comparison
and every decompressed member's byte comparison pass. No configs or binaries.

There are56client snapshots,44server prefix snapshots and28unique successful
target-write begin/end pairs, no unmatched pairs. All sampled completed byte
counts equal their exact range lengths; largest elapsed is1,380us, then589us.
This is a sampled transaction population, not an unbiased all-write percentile.
A long write starting inside the suppression interval can be unlogged. Missing
actor snapshots are silence, not unchanged state. `await_us` includes retained
write/flush, feedback servicing and scheduling, not exclusive socket wait.

### Exact prefix joins and the remaining return-stage question

All times below are Unix milliseconds minus1789064729000, expressed in seconds;
this convenient shared wall anchor is **not** probe start or a shared `t_mono`
origin. A=client assigned horizon, U=unassigned raw source, F=client positive ACK
frontier, R=server receive cursor, T=fully completed target prefix. Client and
server snapshots bracket each other rather than being simultaneous. Monotonic
unchanged A and an observed R=T=A bound later progress through that prefix.

| Plateau | Client A constant | Server evidence R=T=A, reorder0 | U at client endpoints | F at client endpoints |
|---|---|---|---:|---:|
| Early/cut | 210,985,050 at14.414–21.422 | 14.606–21.672 snapshots | 26,131,184→30,768,464 | 168,311,170→173,598,970 |
| Restored | 275,833,874 at28.457–34.481 | 31.294; next positive write34.863 | 20,868,132→25,140,292 | 228,017,594→233,525,842 |
| Late drain | 277,499,786 at42.521–50.553 | 42.069,43.076,44.116; next write51.275 | 28,196,772→34,328,040 | 238,237,834→241,160,434 |

Management independently keeps target bytes at those exact values in samples
14–21,28–34 and42–51. For the latter two intervals source reads continue while
new assignment is mostly or entirely withheld. Client-authoritative gaps below
the already completed server prefix are delayed sender knowledge, not evidence
that those bytes are still missing at the receiver. Retained cache and Product
flight alone could not distinguish this; the new exact frontiers do.

True receiver gaps also occur outside those plateaus. At24.677, for example,
server R=T=241,189,634 with5,416,674B reorder and first gap
[241,189,634,259,661,170); client A was275,112,978 at24.451. By27.824 the server
has R=T=275,637,266 with reorder0. Do not generalize the no-hole classification
to the entire capture or call all recovery copies unnecessary.

An independent application-return hold overlaps restored service: client
response receive cursor remains802/reorder0 at34.481–40.511. Management samples
34→40 show target275,833,874→276,573,454 and sink reply-read1,012→1,138B, while
local reply-write stays802B. This shows missing progress before logical response
receipt, not whether the reply or ACK was already decoded/queued. There is no
exact feedback publication/admission/decode/Input trace in this capture.

### Owner time inside these same prefix plateaus

All2,939client and648server perf rows reconcile interval count/bytes/time deltas
with cumulative totals. Client has57flush groups; each takes at most4ms to print,
with at least1,000ms between periodic groups. The following use complete flush
groups contained inside the joined plateau, not adjacent individual row stamps
from one flush. Completed-call attribution retains boundary uncertainty up to
the contributing call duration; it is not exact instantaneous lock occupancy.

| Full-flush wall interval | Span,s | Actor hold,s | Preselect guard,s | Direct/queued dispatch guard,s | ACK-Apply guard,s | Actor wait,s |
|---|---:|---:|---:|---:|---:|---:|
| 15.336–21.357 | 6.021 | 5.945063 | 2.575655 | 2.913078 | .300855 | .027582 |
| 31.415–34.431 | 3.016 | 3.001772 | 2.748347 | .000502 | .236403 | .002464 |
| 43.482–50.518 | 7.036 | 7.002995 | 6.310004 | .001859 | .657467 | .006553 |

Instrumented control sites1612/3082/3989 identify those three guards in the
saved patch; ordinary source line numbers differ. Preselect includes source
admission, recovery and other preparations, not a separately timed gap function.
Dispatch includes direct structural and queued work, not queued repairs alone.
The late window has999preselect holds(max14,132us),204dispatch holds and406ACK
holds. Writer hold totals in these three windows are.010337/.000275/.000320s.

Whole client actor hold is52.141697s, writer hold.991322s; actor/writer waits
1.006519/.047497s. Dominant actor sites total37.293526s preselect,9.717496s
dispatch and3.601212s ACK Apply. Largest single actor hold is25,779us(dispatch),
not a multi-second individual call. Every event has the existing1us floor.
Hold includes scheduler descheduling and observation inside ownership; waits
are successful acquisition measurements, writer wait excludes earlier Busy
retries, and none is CPU. Do not sum parent stages with their guard or claim
all this elapsed time is removable. Substantial owner work coexists with stale
positive knowledge, but this alone does not locate the critical ACK boundary.

### Own delivery, all56raw bins and physical/resource context

Exact316,866,560B accepted=confirmed,1/1completed,0errors, statusok in55.345273s:
45.802Mbps. First write/confirmation .106532/.413887s; maximum write gap3.495988s
and confirmation gap6.855796s. Settlement extends15.345273s past nominal40s.
No UP echo workload or censoring. Raw bins contain27zeros; the last is partial.
Probe does not save exact max-gap endpoints or a wall origin, so the sampled
plateaus are not asserted to be the exact6.855796s confirmation interval.

| Raw confirmation phase | Mbps | Zero bins |
|---|---:|---:|
| 0–5s | 72.537 | 0/5 |
| 5–15s | 100.448 | 2/10 |
| 15–25s | 42.604 | 4/10 |
| Interior16–24 inclusive | 47.338 | 3/9 |
| 25–40s | 5.993 | 12/15 |
| 40–56s, final partial | 40.739 | 9/16 |

```text
raw bin start (s): receiver-confirmed Mbps
 0: 8.736,157.769,133.852,33.728,28.6,51.38,177.017,212.337,221.509,212.837
10: 54.255,0,28.591,0,46.554,0,0,21.022,0,0
20: 2.824,293.497,61.081,33.224,14.396,0,0,0,28.221,0
30: 0,19.737,0,41.94,0,0,0,0,0,0
40: 20.578,0,0,0,0,25.717,0,0,0,0.806
50: 0,0,2.001,305.381,136.994,160.351
```

All56management/shaper rows verify independent200+200Mbps, DOWN30ms/UP70ms,
zero configured jitter/loss/blackhole, netem limit8192,65536BHTB bursts and equal
rates/ceilings. Only46UP falls to10Mbps at15.001685s and returns200at25.002747s;
47 remains200. All class/qdisc drop deltas are0. Eight active physical outputs
per role persist without suspect/failed states; native epochs are initialized
and unchanged from sample10. Management/collector/probe clocks remain distinct.

Restored samples28→34 send only5,956B across both DOWN classes (~7.94kbps), with
server native ACK deltas1,338TCP/1,170QUIC B. Late42→50 sends17,229DOWN bytes
(~17.22kbps), native ACK deltas2,428TCP/3,111QUIC B. All server path queue-byte
samples in those bands are0; sampled QUIC RTT is~100–102ms with~11–12KBflight
and advancing producer stamps. Idle TCP producer stamps can lag substantially.
These are small return-context totals, not the exact positive ACK frame or
proof of its application. Neither native progress nor zero sampled queues
proves continuous timely service; no measured bulk return-wire congestion is
identified here.

| Whole sampled cost (55.006951s) | Value |
|---|---:|
| UP46 /47 class bytes | 331,271,154 /393,868,419 |
| DOWN46 /47 class bytes | 4,681,451 /5,634,859 |
| Summed UP backlog peak / final,B | 18,768,638 /3,742,131 |
| Summed DOWN backlog peak / final,B | 48,492 /25,949 |
| Client RSS peak / final,KiB | 321,064 /279,040 |
| Server RSS peak / final,KiB | 77,240 /77,240 |
| Client lifetime CPU peak / final,% | 131 /113 |
| Server lifetime CPU peak / final,% | 51.3 /20.3 |

Lifetime `ps` CPU is not interval CPU; sampled final memory/queues are not
post-teardown retention. Logs are1,302,109client +331,724server bytes, an explicit
observation cost. Probe stderr is empty; two server H3_NO_ERROR close warnings
follow completion. No ordinary-vs-diagnostic speed claim is warranted.

Information forecast outcome: the joined prefixes distinguish real receiver
holes from several material stalls after all currently assigned data reached
the target. Delayed positive knowledge and unassigned source are demonstrated;
the exact withholding point along feedback publication→native→decode→Input is
still unresolved. The next decision belongs to that existing feedback-service
boundary, not another guessed gap-work correction or a waived practical gate.

## ACK-prefix stage diagnostic: earliest lag precedes common attachment arrival

Run79321 materially narrows the existing feedback-service question. A positive
ACK covering an already completed427,743,428B request prefix is generated and
command-admitted, yet no equal-or-greater explicit positive prefix reaches the
client's common attachment forwarder for **at least about24.0s**. The earliest
withholding boundary is after server command admission and before that client
hook, not delayed desired generation or only the final actor Apply. This scope
still includes server command/writer service, native transport, client decoding
and per-attachment mailbox service; it does not prove a purely native failure.
A separate multi-second shared-FIFO/actor service delay is also observed.

### Observer meaning and reproducibility

Runtime is `a16b404` plus seven temporary feature-only files. Six fixed scalar
slots bind the first selected session848173939728819236/stream0 per process;
they retain stage count, latest-any-ACK time and greatest explicit positive
[0,x) with its first producer time. Equal replay cannot renew that first time.
No frame history, per-output identity, retained range list or new wake exists.
The repeated <=1/s report does not refresh producer evidence.

| Stage | Actual boundary |
|---|---|
| generated | Desired cumulative ACK installed at server binding |
| command_admitted | Successful frame enqueue to an output command lane |
| attachment_arrival | Client common forwarder, after per-attachment mailbox receive |
| shared_fifo_admitted | Successful shared FIFO send; timestamp after send returns |
| actor_dequeued | Actual selected/retained ACK enters actor handling |
| applied | Validated positive application, including validated subsumed replay, before expensive recovery |

Admission is not native write; attachment arrival is not raw decode. Post-send
stamping may trail a concurrent consumer. Attachment arrival, FIFO admission
and actor entry are not ACK validation. Witnesses are explicit [0,x), not largest
range end or inferred union coverage. Counts differ with fanout, replay and
chunking; they are not six equal conservation totals. Startup frames racing the
cfg-only session initialization can be unobserved rather than misattributed.

Build3718 succeeds in3m35s with the existing unused batch-helper warning. Root
freezes `bin/ack-prefix-stage-20260911/mptunnel` and the exact seven-file patch,
then reverses all source edits before traffic. The wrapper enables PERF=1 and
PERF_SAMPLES=0 alongside the selected prefix/stage/hole events; no inner-plan
observer or per-call samples. Runner exits0 in55.009476s. The
[verified raw archive](ACK_PREFIX_STAGE_20260911.raw.tar.gz) is488,198B/11safe
regular files: five results, build/driver logs, exact patch, wrapper, `run.py`
and `shape.sh`. Gzip/tar and every decompressed member's bytes pass; no configs
or binaries. This diagnostic is not an ordinary performance comparator.

### Decisive retained-witness joins

Times below are Unix milliseconds minus1789066034000, shown in seconds. This
shared wall anchor is not probe start or a common monotonic origin. Producer
times are distinct from later periodic report times.

| Observation | Explicit prefix,B | Producer time | Report / supporting state |
|---|---:|---:|---|
| Server generated and command-admitted | 427,743,428 | 30.100 both | Report30.240; R=completedT=427,743,428,reorder0 |
| Client attachment greatest shortly before | 361,346,060 | 29.705 | Report30.030; client A427,743,428,U711,496,F360,625,164 |
| Client attachment greatest still below that prefix | 392,821,476 | 54.129 | Report54.135; actor/Applied only390,691,588 |
| First later retained attachment witness above it | 441,065,700 | 54.866 | Report55.137; actor/Applied441,065,700 at54.867 |

Because the greatest witness never decreases, the new below-threshold witness
at54.129 supplies a conservative24.029s producer-time lower bound after known
command admission. The later larger witness only upper-brackets threshold
crossing; it is not the same ACK frame or proof that exact427,743,428 spent
24.766s in a particular queue. Client F remains below that already delivered
prefix through54.135. Later assignment grows slowly, rather than A remaining
constant for the whole interval: A440,797,092/U5,240,924 at54.135. Receiver and
target frontiers remain above the completed427,743,428 prefix throughout.

The delay is not universal startup behavior. Four retained exact-value
command→attachment matches (193,039,166;218,920,198;230,117,230;254,982,404)
take30–85ms. All47retained generated/admitted value matches have identical
millisecond producer times. These are sparse matching witnesses, not unbiased
all-ACK latency distributions or per-frame identities.

The client has additional local service delay:

| Explicit prefix,B | Earlier hook/time | Later hook/time | Prefix-attainment separation |
|---|---|---|---:|
| 363,377,676 | Shared FIFO37.979 | Actor42.088 | 4.109s |
| 362,984,460 | Attachment33.980 | Actor/Applied39.036 | 5.056s |

The51retained actor/Applied value matches differ0–2ms. Late reports have
arrival−FIFO count differences3–4 and FIFO−actor122–131; actor−Applied is0.
This shows persistent work between those hooks, including pending/deferred
actor frames. It is not an exported exact command-slot occupancy or a proof
that every delay belongs to the shared FIFO. Backpressure can couple that
local backlog to the earlier unobserved mailbox/reader span.

Final passive reports are not synchronized: server54.835 records7,391generated
and21,716admitted ACKs, greatest440,952,164; client55.137 records22,512arrival,
22,508FIFO and22,382actor/Applied, greatest441,065,700. The later client count
exceeding the earlier server report is not evidence of duplication beyond
normal fanout or a conservation failure. Unused role-specific slots remain
None/count0; no final stage flush is invented.

### Own prefix, reply and owner context

Unlike the preceding capture, much of this run has slow advancing frontiers
rather than one long constant A. Real cut-phase holes occur: server R218,920,198
is fixed at19.217–21.223 while reorder grows14,425,982→50,917,638B; client A
reaches286,029,062/U0 at20.998. Do not erase that forward hole when diagnosing
the later already completed prefix. At30.240 R=T427,743,428/reorder0, and
restored snapshots after32.254 have no reorder through52.543.

Client response cursor854/reorder0 stays fixed at30.030–37.091, then868/reorder0
at38.093–44.118. Management30→36 target427,743,428→430,073,188 and sink reply
read1,036→1,176B while local reply-write854 stays fixed. Thus application return
service also lags actual target writes; the stage helper observes request ACKs,
not the exact application reply. No exact max-confirmation-gap endpoints or
probe wall origin are saved, so these windows are not asserted to be its exact
7.815789s gap.

All2,993client and758server perf rows reconcile interval/cumulative count,
bytes and time. Complete owner-flush groups inside the missing-prefix interval
30.996–54.134 cover23.138s: actor hold22.766675s, including preselect10.664234s,
direct/queued dispatch10.126003s and ACK Apply.899790s; actor wait.136389s.
Inside the response854 hold,30.996–37.055 covers6.059s with5.984263s actor hold
(3.857630dispatch,1.868585preselect). Response868 interior39.063–44.105 covers
5.042s with4.993191s actor hold (2.605894dispatch,2.125444preselect).

Whole actor hold47.969329s/writer1.271652s; actor/writer waits1.247609/.057444s.
Dominant instrumented control sites1621/3091/4008 total26.912217s preselect,
15.145923s dispatch and3.355703s ACK Apply. Largest single actor hold38,309us
is not one multi-second call. These are completed-call elapsed ownership times,
with the1us floor and boundary/descheduling/observer effects, not CPU. Preselect
and dispatch are broad guards, not isolated gap or queued-plan timers. Their
large overlap with delayed ACK service does not locate the missing upstream
ACK frame or establish that removing all measured time is possible.

### Own delivery, complete series and cost

Exact446,038,016B accepted=confirmed,1/1completed,0errors, statusok in54.301541s
=65.713Mbps. First write/confirmation .105487/.409609s; maximum write gap2.553686s,
confirmation gap7.815789s. Settlement extends14.301541s beyond nominal40s. No UP
echo workload or censoring. All55raw bins below include26zeros and a partial
final bin; no trimmed mean substitutes for wall-clock delivery.

| Raw confirmation phase | Mbps | Zero bins |
|---|---:|---:|
| 0–5s | 119.725 | 0/5 |
| 5–15s | 62.807 | 5/10 |
| 15–25s | 89.020 | 4/10 |
| Interior16–24 inclusive | 68.037 | 4/9 |
| 25–40s | 53.659 | 11/15 |
| 40–55s, final partial | 43.102 | 6/15 |

```text
raw bin start (s): receiver-confirmed Mbps
 0: 8.077,142.253,140.413,120.913,186.97,0,242.715,29.412,0,0
10: 60.096,0,227.264,0,68.586,277.864,0,0,40.274,0
20: 0,226.893,97.371,170.853,76.943,706.6,33.074,0,27.647,0
30: 0,0,0,0,0,0,37.557,0,0,0
40: 0,0,0,37.845,0,0,193.628,87.652,0,116.942
50: 64.103,6.771,10.914,86.936,41.735
```

The706.6Mbps confirmation bin reflects released confirmation of prior target
work, not a measured instantaneous400Mbps-link-capacity violation.
All55shape rows validate direct200+200Mbps, DOWN30ms/UP70ms, zero configured
loss/jitter/blackhole,8192netem limit and65536BHTB bursts. Only46UP is10Mbps
from15.003894 until25.004999s;47 stays200. All class/qdisc drop deltas are0.
Eight native output epochs per role remain unchanged from sample10.

Management30→54 records702,533DOWN class bytes (~234kbps), server native ACK
deltas66,044TCP/101,760QUIC B. Server carrier queue-byte samples are0 throughout
this band. QUIC1 RTT175.306ms and25,870Bflight persist with advancing producer
stamps; idle TCP samples may be older. Forward native ACKs still add69,552,384TCP
and105,114,221QUIC B, not necessarily useful new originals. Neither direction's
native byte counter identifies the critical ACK. Management `queue_bytes` is
observed carrier byte state, not command-lane occupancy; client has50unobserved
path-queue values in these25rows, not measured zeros.

| Whole sampled cost (54.009263s) | Value |
|---|---:|
| UP46 /47 class bytes | 348,838,649 /567,667,770 |
| DOWN46 /47 class bytes | 5,166,400 /6,778,718 |
| Summed UP backlog peak / final,B | 27,152,908 /313,959 |
| Summed DOWN backlog peak / final,B | 43,083 /0 |
| Client RSS peak / final,KiB | 279,736 /276,088 |
| Server RSS peak / final,KiB | 135,720 /135,720 |
| Client lifetime CPU peak / final,% | 122 /113 |
| Server lifetime CPU peak / final,% | 53.2 /25.4 |

CPU is lifetime `ps`, not interval ownership or collector-adjusted CPU. Final
memory/queues are not post-teardown retention. Logs contain1,467,933client and
530,207server bytes, including55/54six-slot reports and55/54prefix snapshots.
All43sampled target-write pairs succeed, largest225us, with no unmatched pairs;
unsampled writes retain the earlier censoring caveat. Probe stderr is empty;
two server H3_NO_ERROR warnings follow completion.

Information forecast succeeds: desired installation/admission is timely for
the critical completed prefix; its earliest material lag is before common
attachment arrival, with secondary local FIFO/actor delay. Exact writer/native/
decode/mailbox attribution remains open. Preserve the7.816s practical gap and
do not turn this diagnostic into a performance win or select another gap tweak.
