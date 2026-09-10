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

## ACK carrier-prefix diagnostic: early decode lag and coupled local service

Information-only run61091, `aggregate-combined-up-ack-carrier-prefix-0911`,
closes normally after50.005532s. It extends the preceding six-slot observation
with successful carrier-write and fully decoded-frame witnesses; it does not
change runtime policy or constitute an ordinary performance comparison.
Build45395 succeeds in3m35s with the existing unused-helper warning. The exact
13-source-file,45,693B overlay was frozen and completely reversed before traffic;
ordinary source remains a16b404. The explicitly selected executable is
`./.tmp/reflection/bin/ack-carrier-prefix-20260911/mptunnel`, with periodic PERF
enabled and samples disabled. Session7591395517251646329/stream0 is stable;
client/server PIDs are490729/495753.

[Raw capture and exact observer inputs](ACK_CARRIER_PREFIX_20260911.raw.tar.gz)
contains11 regular files: five results, build/driver logs, complete patch,
wrapper, run.py and shape.sh. Archive375,249B; gzip integrity, member identities
and every decompressed member's byte comparison pass. No configs or executable
are included. This archive, not a selected subset of stage rows, is authoritative.

### Outcome and unchanged physical conditions

| Own diagnostic outcome | Value |
|---|---:|
| Local accepted = target confirmed bytes | 303,562,752 |
| Exact completed streams / failures | 1 /0 |
| Delivery elapsed / mean | 49.539437s /49.022Mbps |
| First write / confirmation | .109156 / .414570s |
| Maximum write / confirmation gap | 11.405079 /12.064186s |
| Settlement beyond nominal40s | 9.539437s |
| Raw bins / zero bins | 50 /29 |

There is no separate echo workload in this UP probe. Status is `ok`, exact
accounting true, lower-bound false, errors empty and stderr0B. Full one-second
target-confirmation bins are below; bin49 is partial. The484.769Mbps burst
releases accumulated confirmation and is not a physical400Mbps-cut violation.

| Raw seconds | Mbps, in chronological order |
|---|---|
| 0–9 | 8.854,138.443,317.431,59.865,0,0,57.243,29.648,274.880,52.896 |
| 10–19 | 23.389,0,0,0,0,0,0,39.858,0,0 |
| 20–29 | 484.769,0,0,59.013,7.436,0,0,0,88.602,0 |
| 30–39 | 0,0,0,0,0,0,0,0,0,0 |
| 40–49 | 76.328,0,76.494,145.878,.569,90.333,0,0,299.685,96.889 |

Raw-bin means are104.919Mbps at0–5s,43.806 at5–15s,65.675 at16–25s,
5.907 at25–40s and76.587 at40–49s. These are full listed intervals, not the
probe's trimmed series or a reconstructed exact final partial-bin rate.
All50management rows preserve two independent links: client eth0/eth1=46/47,
server eth1/eth0=46/47; each200Mbps, DOWN30ms/UP70ms, jitter/loss0,
netem limit8192, HTB burst65,536B. Only46UP is10Mbps from15.001689 until
25.002646s;47 stays200. No UDP blackout and all class/qdisc drop deltas are0.

### Eight-stage joins: first witnesses, not per-frame or per-output traces

The preceding fixed-prefix witness contract remains in force. `carrier_written`
means a complete ordinary TCP write/flush or successful generic QUIC native
write; batch success is stamped after the batch, partial/error paths are
censored. It is not physical transmission. `decoded` is after authenticated
TCP decoding / complete QUIC frame read, before downstream attachment service.
Each slot retains the greatest explicit positive prefix and its first producer
timestamp. There is no frame identity, selected-output identity or arrival
history; unequal prefixes give bounds, not exact per-frame transit times.
Replay does not refresh a prefix's first timestamp. Raw early witnesses do not
replace logical validation. All following wall times subtract1789066773000ms
for readability; this is not either process's monotonic origin or probe start.

| Explicit prefix,B | Generated / admitted / written,s | Decoded witness,s | Supported bound |
|---|---|---|---|
| 189,358,948 | 13.200 /13.200 /13.200 | same prefix23.133 | 9.933s written→first same-prefix decoded |
| 241,245,149 | 25.550 /25.550 /25.550 | smaller216,737,014 first42.366; larger242,001,409 first43.372 | ≥16.816s before an equal-or-greater decoded witness;43.372 upper-brackets crossing, not this frame's transit |
| 242,633,878 | 33.044 /33.044 /33.044 | smaller242,001,409 first43.372; larger242,699,414 first43.478 | ≥10.328s, with43.478 crossing upper witness |

These are producer-stamp bounds, not later reporting times. At23distinct retained
server prefixes, generation/admission stamps match and written differs by0–1ms;
the periodic admitted/written counts track together. This does not prove every
unretained ACK's timing or permit unsynchronized final count conservation.

The critical prefix joins still show already completed target service with
unassigned local source and delayed positive sender knowledge:

| Bracketed stage | Client coherent state | Server coherent state |
|---|---|---|
| Early | at13.165–22.189s A=189,358,948,U=3,051,282,F=125,301,366 | at13.395–22.427s R=T=189,358,948,reorder0 |
| Restored | at26.194–31.286s A=241,245,149,U32,341,433→32,893,633,F205,089,158→205,641,358 | at26.433–30.587s R=T=241,245,149,reorder0 |
| Later | at33.321–41.386s A=242,633,878,U31,653,504→34,853,120,F205,687,758→208,404,534 | at33.682–41.236s R=T=242,633,878,reorder0 |

Cross-role ranges are sampled brackets, not synchronized state. The same client
response cursor remains436B at29.275–40.385s with reorder0, then advances478B
at41.386s. This is its own return-service plateau; no exact probe wall anchor
was saved, so it is not asserted to equal the12.064186s confirmation interval.
All19sampled target-write begin/end pairs succeed, maximum618us, none unmatched;
unsampled writes remain outside that claim.

Crucially, the early decode lag is not independent of local downstream pressure.
Prefix125,301,366 has the complete retained chain: generated/admitted9.473,
written9.474, decoded9.504, attachment11.650, FIFO11.695, actor/applied12.678s.
That is30ms written→decoded,2.146s decoded→attachment,45ms attachment→FIFO
and983ms FIFO→actor. Later exact scalar-prefix witnesses show:

| Prefix,B | Local first-witness interval | Delay |
|---|---|---:|
| 206,492,318 | FIFO26.804→actor39.330; Apply39.331 | 12.526s FIFO→actor |
| 206,331,718 | attachment25.152→Apply38.112 | 12.960s |
| 210,511,094 | decoded25.721→Apply42.380 | 16.659s |

At39retained actor/Apply matches, first timestamps differ0–1ms. Across50client
reports, decoded-minus-attachment count difference peaks1,823; at26–42s it is
180–242, attachment-minus-FIFO3–4 and FIFO-minus-actor125–131. These are witness
count differences, not exact command-slot occupancy. They establish material
downstream delay; a reader held behind earlier decoded frames can propagate
backpressure upstream into the written→decoded interval. This capture therefore
does not establish a separate physical/native fault or isolate one carrier.

### Same-capture owner and native context

All2,509client/585server periodic rows reconcile interval counts/bytes/time to
cumulative deltas and final totals. The51client flushes have unique components
per flush and maximum4ms printing width. Complete-flush differences contained
inside the corresponding producer-witness intervals are:

| Wall-offset flush interval,s | Span | Actor hold | Preselect | Direct+queued dispatch | ACK handling |
|---|---:|---:|---:|---:|---:|
| 14.080–22.104 | 8.024 | 7.889881 | 1.636177 | 5.630300 | .251759 |
| 27.141–39.262, inside the12.526s FIFO wait | 12.121 | 12.071026 | 11.276617 | .131231 | .585652 |
| 26.134–42.273, inside restored written→decoded bound | 16.139 | 16.047490 | 14.823558 | .190532 | .862851 |

Seconds are completed-call elapsed time, floored1us, including descheduling and
observer cost—not CPU or one uninterrupted call. These windows overlap; never
sum them. Static instrumented control.rs sites1621/3091/4008 own the displayed
preselect/dispatch/ACK scopes. Whole actor/writer holds are46.127488/.801561s;
waits .775139/.035059s. Largest single preselect/dispatch/ACK holds are
34.915/19.620/23.671ms. Inside27.141–39.262s actor wait is only .004666s.
Thus actual repeated local service occupies nearly the full measured long FIFO
residence; scalar stages still cannot identify each ACK's particular blocking task.

All eight native output epochs per role remain stable from sample10. Management
26→42 adds265,686DOWN class bytes, server native ACK13,360TCP/15,945QUIC B;
server observed carrier queue bytes stay0. QUIC46 native RTT100.083ms/flight9,926B
persists with advancing producer stamps, while QUIC47 ends107.985ms/0Bflight.
Forward native ACK adds34,297,738TCP/25,287,512QUIC B. These small reverse/native
counters include control and other frames, not the critical ACK's native progress;
forward counts likewise are not new useful originals. Client queue observations
include34NULL values in these17rows, not zeros. No command-lane slot count is
exported. Management33→41 target-write totals rise65,536B while local source
reads rise2,542,616B; this does not erase the bracketed A=R=T plateaus above.

| Whole sampled cost,49.005309s | Value |
|---|---:|
| UP46 /47 class bytes | 337,265,211 /405,193,828 |
| DOWN46 /47 class bytes | 3,723,359 /3,891,508 |
| Summed UP backlog peak / final,B | 27,835,236 /13,529,703 |
| Summed DOWN backlog peak / final,B | 35,611 /12,115 |
| Client RSS peak / final,KiB | 321,956 /314,292 |
| Server RSS peak / final,KiB | 116,896 /116,896 |
| Client lifetime CPU peak / final,% | 120 /112 |
| Server lifetime CPU peak / final,% | 54 /19.4 |

CPU is process-lifetime `ps`, not interval CPU. Final samples precede full
settlement and teardown; no retention/leak conclusion follows. Client/server
logs are1,266,249/440,056B, including50/46eight-slot reports and50/46prefix
snapshots. Two server H3_NO_ERROR warnings follow completion; client has none.

The information forecast succeeds: early critical ACK knowledge has already
been generated, admitted and accepted by a carrier, yet remains undecoded for
material time; separately, decoded knowledge waits materially in local service.
The coupled backpressure interpretation remains necessary. Preserve this run's
12.064s gap and weak restored service; no ordinary performance promotion,
independent-native-fault claim, or new algorithm selection follows from it.

## Ordinary ordered feedback: material service gain, broader acceptance pending

Run95903 tests one finite, ordered ACK/MAX Input transaction on ordinary a16,
not an observer or a restored rejected trial. Every ACK still applies its own
validation, scope, positive release, qualification, sampling and pruning in
order. One Product guard covers the fixed ready-feedback quantum; one fresh gap
evaluation follows its novel ACK facts before prepared publication/FIN/unlock.
Other frames are exact barriers. Independent preselect discovery and its wakes
remain. Fatal input revokes existing claim authority before unlock. There is
no ACK merge, retained target view, new timer/limit, controller change or
drain-to-empty loop. A longer finite synchronous Input transaction and changed
first timing observations/interleaving are explicit risks, not claimed neutral.

The actual-actor fixture initially fails setup44817 after1m39s: full positive
ACK2 correctly encodes scopeNone, notSome(0). Correcting only that expectation
gives intended RED19106 after38.21s compilation:3heavy discoveries between
already-ready ACK1/ACK2 versus0expected, with actual native claims, shared-input
readiness, exact intermediate gap and final release all checked first. GREEN
27151 gives0 and passes52distinct checks:33control,12client,5service,1split-ACK,
1copy-reserve. These include order/barrier/error, half-close, prearmed capacity
and cooperative service controls. They prove the mechanism, not its speed.

Ordinary release61255 succeeds in1m04s with the pre-existing unused batch-write
helper warning. The six-file patch comprises RFC plus five source/test files;
frozen binary is `./.tmp/reflection/bin/ordered-feedback-20260911/mptunnel`.
No diagnostics or compiler overlap occur in this run. Driver exits0 in
43.007028s. [Exact ordinary evidence](ORDERED_FEEDBACK_ORDINARY_20260911.raw.tar.gz)
is249,942B/15regular files: five results, driver/build/initial-RED/retry/GREEN
logs, initial/final RED patches, candidate patch, run.py and shape.sh. Gzip,
tar comparison and every decompressed member's bytes pass; no configs/binaries.

The only performance comparator here is preserved ordinary a16run23406,
`aggregate-combined-up-ack-recovery-invalidation-0910`; its
[raw archive](ACK_RECOVERY_INVALIDATION_ORDINARY_20260911.raw.tar.gz) and full
46-bin series remain above. Diagnostic61091 is causal context, not a speed
baseline. These are chronological, same-profile realizations, not equal-work
or simultaneous paired trials; this result cannot assign all differences to
the one changed scheduling boundary.

### Full delivery, phases and remaining weak intervals

| Outcome | Ordinary a16 | Ordered-feedback candidate |
|---|---:|---:|
| Local accepted = target confirmed,B | 526,385,152 | 1,091,108,864 |
| Exact completed streams / failures | 1 /0 | 1 /0 |
| Elapsed,s | 45.465808 | 42.660189 |
| Whole confirmed Mbps | 92.621 | 204.614 |
| First write / confirmation,s | .105228 /.408993 | .105882 /.409412 |
| Maximum write gap,s | 7.235997 | 1.162112 |
| Maximum confirmation gap,s | 4.145083 | 1.542599 |
| Settlement beyond nominal40s,s | 5.465808 | 2.660189 |
| Raw bins / zeros | 46 /11 | 43 /1 |

Completed work rises107.28%, whole rate120.92%, and elapsed falls6.17%; maximum
write/confirmation gaps fall83.94%/62.78%. First service is slightly later
(write+.654ms, confirmation+.419ms), not a claimed startup improvement. Both
complete exactly with statusok, no probe errors/censoring. This UP workload has
no echo attempts and cannot establish loaded echo latency or DOWN behaviour.

| Raw confirmation phase | a16,Mbps | Candidate,Mbps | Candidate zeros |
|---|---:|---:|---:|
| 0–5s | 65.683 | 174.452 | 0/5 |
| 5–15s, pre-cut | 96.384 | 225.303 | 0/10 |
| 15–25s | 98.764 | 184.725 | 1/10 |
| Interior16–24 inclusive | 104.288 | 184.959 | 1/9 |
| 25–40s, restored | 83.343 | 205.211 | 0/15 |
| Own post40s raw bins,last partial | 113.510 over6bins | 226.056 over3bins | 0/3 |

All43candidate raw target-confirmation bins follow. Do not substitute trimmed
bins or treat the partial final bin as a whole elapsed second.

| Raw seconds | Mbps,in chronological order |
|---|---|
| 0–9 | 15.179,194.607,242.263,233.615,186.597,150.525,150.001,98.581,262.043,213.873 |
| 10–19 | 423.616,148.802,271.223,241.487,292.879,182.619,193.214,207.836,0,166.696 |
| 20–29 | 188.997,58.675,141.348,192.932,514.932,55.452,231.708,247.752,122.031,140.287 |
| 30–39 | 342.681,68.733,207.289,264.453,312.524,240.286,252.893,194.022,140.170,257.881 |
| 40–42 | 263.001,243.478,171.690 |

Weak service is not erased: bin18is zero; bins21/25/31give58.675/55.452/68.733Mbps,
and the1.543s confirmation gap remains. There is no configured blackout here;
an outage-like visual dip is not evidence of path failure. The514.932Mbps
confirmation burst can release accumulated prior service, not violate the
two200Mbps physical cuts. The probe retains maximum gap magnitudes but no exact
endpoints, so the ordinary management rows below are not called their exact cause.

### Source, target and reply service in this realization

All42adjacent target-write samples advance, unlike a16's5s target plateau at
34–39s. In the restored25→40band, every source-read, target-write, server-reply
read and client-reply write adjacency advances. Their respective total gains
are380,911,976B,380,751,368B,883B and883B. The minimum one-adjacency target
gain is2,295,776B, not a claim of continuous subsecond service.

| Actual target-socket producer window | a16,Mbps | Candidate,Mbps |
|---|---:|---:|
| 0→5 | 155.060 | 174.452 |
| 5→15 | 75.944 | 228.340 |
| Strict16→24 | 119.326 | 141.014 |
| 25→40 | 68.915 | 203.054 |

The cut target gain is smaller than the raw-confirmation gain: these are
different producer clocks and byte boundaries, with buffered confirmation.
Candidate sample18→19 records target462,624,814→467,137,278B and client reply
1079→1093B, while raw bin18is zero. Without the probe's wall anchor and exact
ACK stages, neither measurement invalidates the other or localizes the1.543s gap.

| Candidate sample | Source read,B | Target write,B | Server /client reply,B |
|---|---:|---:|---:|
| 16 | 481,192,174 | 416,459,830 | 981 /981 |
| 24 | 624,309,918 | 557,474,166 | 1247 /1247 |
| 25 | 693,154,086 | 626,205,830 | 1303 /1303 |
| 34 | 899,729,182 | 833,237,758 | 1807 /1807 |
| 39 | 1,043,661,174 | 976,552,310 | 2115 /2115 |
| 40 | 1,074,066,062 | 1,006,957,198 | 2186 /2186 |
| 42,last sampled | 1,091,108,864 | 1,076,557,070 | 2321 /2321 |

Last client/server management generations are1789068870161/1789068870151ms.
The target's final14,551,794B fall after its last sample; full settlement comes
from the exact probe, not synchronized final management conservation.

### Physical/native context and cost, including higher absolute resource use

All43candidate rows match the46-row comparator's direct200+200 topology,
DOWN30ms/UP70ms, zero configured loss/jitter/outage, netem limit8192 and HTB
burst/cburst65536; rates equal ceilings. Only client eth0/link46 changes to
10Mbps at15.003411s and returns200 at25.004516s (a16:15.001773/25.002925).
Link47 remains200; server eth1/eth0 map46/47. All class/qdisc drop deltas are0.
Session13062760317167047878 and PIDs491998/497005 remain stable. Startup grows
the path set; all eight native epochs per role are initialized/stable from
sample9, with no observed suspect/failed state.

| UP class service, Mbps | a16 link46 /47 | Candidate link46 /47 |
|---|---:|---:|
| 0→15 | 89.136 /146.906 | 177.719 /189.980 |
| Strict16→24 | 10.023 /195.684 | 9.938 /194.429 |
| 25→40 | 112.396 /91.589 | 194.233 /192.061 |

During restored25→40, client native ACKs add84,561,146TCP/618,236,941QUIC B,
versus177,696,181/193,611,011in a16. This suggests materially changed carried
work, not an exact Original-allocation or useful-copy attribution. Candidate
Product flight falls59,192,520→15,477,672B. At40, client QUIC46/47 RTT is
116.811/211.848ms, with3,346,860/5,643,924B native flight. Reverse server QUIC
path_id1 retains296.466ms RTT versus100.570ms in a16; no uniform native-latency
improvement or echo claim follows. Native observations/ACK progress do not
identify a critical Product byte. Observed client queue peak143,412B in25–40
coexists with32NULL path-queue values; command-slot occupancy is not exported.

| Whole sampled cost | a16 | Candidate |
|---|---:|---:|
| Sample window,s | 45.006732 | 42.006780 |
| UP46 /47 class bytes | 467,368,412 /749,700,687 | 763,417,301 /1,005,665,982 |
| DOWN46 /47 class bytes | 7,015,954 /8,612,499 | 10,318,970 /15,185,763 |
| Summed UP backlog peak /final,B | 28,677,318 /12,763,272 | 26,617,192 /2,899,956 |
| Summed DOWN backlog peak /final,B | 46,620 /13,425 | 47,099 /47,099 |
| Client RSS peak /final,KiB | 355,388 /342,712 | 350,600 /330,044 |
| Server RSS peak /final,KiB | 75,776 /75,776 | 126,644 /93,716 |
| Client lifetime CPU peak /final,% | 126 /123 | 174 /174 |
| Server lifetime CPU peak /final,% | 66.2 /37.8 | 68.7 /68.7 |

Completed work is2.073×, sampled UP traffic1.454× and DOWN1.632×. As an observed
cost proxy, UP class bytes/exact confirmed bytes fall2.312→1.621(−29.88%),
DOWN .029690→.023375(−21.27%). These differing sampled windows end before full
settlement; the ratios are not exact lifetime retransmission/copy amplification.
Higher absolute CPU/server peak RSS are real headroom tradeoffs, not demonstrated
per-byte efficiency regressions. `ps` reports rounded process-lifetime averages;
there are no CPU ticks here, and multiplying by probe duration would omit the
process's startup/lifetime boundary. No exact CPU-per-byte conclusion is made.
Finite-VPS CPU/memory pressure remains an acceptance concern; final queues/RSS
are not post-teardown retention. Client log and probe stderr are empty; two
server H3_NO_ERROR warnings occur at normal teardown.

Disposition: the forecast receives material practical support in this one
ordinary heterogeneous-UP realization—more completed work, shorter settlement,
better all-phase service and substantially smaller worst gaps. Retaining an
isolated mechanism checkpoint is supported; universal optimality, full stall
closure and performance/release acceptance are not. Preserve the residual gaps,
absolute resource cost and reverse RTT tradeoff, then use the declared healthy
and affected-direction gates rather than diagnostic speed or a favourable rerun.

## Separate healthy ordinary pair: gain persists without QoS

Predeclared control7360 then candidate91118 reuse the same frozen ordinary a16
and ordered-feedback binaries; only `NO_QOS=1` removes the46UP rate change.
There is no new build, runtime parameter or observer. This is an independent
healthy pair, not a replacement for the preceding QoS result. Candidate runtime
and its52focused checks are preserved by checkpoint011b724 and the preceding
ordinary archive. Drivers exit0 after42.007560/43.005078s.
[Healthy raw evidence](ORDERED_FEEDBACK_HEALTHY_20260911.raw.tar.gz) contains
14regular files: both five-file result sets, two driver logs, run.py and shape.sh;
441,963B, gzip/tar/every-member byte verification pass. No configs/binaries.

The forecast was to preserve healthy service, with a possible gain where ready
feedback still backlogs; no gain was promised without that backlog. The control
does exhibit such stalls. This pair supports a material gain in bytes, all-phase
delivery and worst gaps, while
preserving higher absolute CPU, slightly higher client peak memory and longer
final elapsed time. One pair is not statistical proof or global acceptance.

| Healthy UP outcome | a16 control | Candidate |
|---|---:|---:|
| Local accepted = target confirmed,B | 526,188,544 | 1,180,565,504 |
| Exact completed streams /failures | 1 /0 | 1 /0 |
| Elapsed,s | 41.740160 | 42.766993 |
| Whole confirmed Mbps | 100.850 | 220.837 |
| First write /confirmation,s | .108411 /.411490 | .107294 /.410305 |
| Maximum write gap,s | 2.002279 | .722869 |
| Maximum confirmation gap,s | 4.587237 | .532616 |
| Settlement beyond nominal40s,s | 1.740160 | 2.766993 |
| Raw bins /zeros | 42 /8 | 43 /0 |

Candidate completes2.244×the work at118.98%higher whole rate. Maximum write
and confirmation gaps fall63.90%/88.39%; first service improves by~1ms, not a
material startup-latency claim. Elapsed/settlement grows1.026833s (+2.46%whole)
while delivered work more than doubles. Neither shorter equal-work completion
nor lower tail settlement is claimed. Both statuses/accounting are exact/ok,
with no probe errors or censoring; UP has no echo workload.

| Raw confirmation phase | Control,Mbps | Candidate,Mbps |
|---|---:|---:|
| 0–5s | 38.311 | 190.638 |
| 5–15s | 66.730 | 234.401 |
| 15–25s, still healthy | 114.820 | 206.824 |
| 25–40s, still healthy | 109.649 | 222.018 |
| Own post40 bins,last partial | 278.860 over2 | 249.605 over3 |

All85raw bins are retained below. The post40 averages include different partial
final bins and are not an equal-duration tail comparison. No phase is trimmed.

| Case /raw seconds | Mbps,in chronological order |
|---|---|
| Control0–9 | 10.719,180.838,0,0,0,0,39.750,48.094,144.319,186.130 |
| Control10–19 | 58.764,48.995,141.249,0,0,367.787,20.866,20.792,0,44.680 |
| Control20–29 | 37.280,75.734,194.121,346.280,40.661,50.549,22.640,30.578,53.455,94.517 |
| Control30–39 | 25.370,32.016,327.516,61.462,111.639,208.654,45.805,555.125,25.402,0 |
| Control40–41 | 417.237,140.483 |
| Candidate0–9 | 8.220,219.923,243.270,256.479,225.298,231.307,233.724,195.876,216.227,162.119 |
| Candidate10–19 | 213.987,279.274,302.333,279.665,229.497,238.042,247.071,193.929,165.043,237.087 |
| Candidate20–29 | 229.855,155.625,138.815,262.847,199.928,243.765,193.507,181.027,168.762,343.896 |
| Candidate30–39 | 286.192,233.752,196.685,202.412,111.972,242.702,302.629,205.520,213.020,204.427 |
| Candidate40–42 | 279.416,253.882,215.516 |

After startup, the weakest full candidate bin is111.972Mbps at34s: improved
service is not a flat400Mbps pipeline. Control's555.125Mbps confirmation burst
can unlock buffered progress and does not exceed physical capacity on the wire.
No exact maximum-gap endpoints or ACK-stage observations exist in these ordinary
captures, so individual raw dips are not assigned to a new mechanism.

### Actual service and native context

All42candidate adjacent target-write samples advance. Control target/reply-read
is fixed at74,592,906B/162B over server Unix1789069061079→1789069066079ms
(samples3→8), and227,864,784B/714B over1789069077079→1789069080079ms(19→22).
Control source still adds12,680,584/14,747,398B in those respective bands.
The corresponding candidate target gains145,781,886/74,757,450B; these are
same nominal windows, not matched per-byte paths. Native/controller startup
and offered work differ, so absence of the plateau is not a per-ACK causal trace.

| Actual target-socket producer window | Control,Mbps | Candidate,Mbps |
|---|---:|---:|
| 0→5 | 119.373 | 192.181 |
| 5→15 | 61.137 | 235.492 |
| 15→25 | 93.922 | 206.815 |
| 25→40 | 123.850 | 224.405 |

At the last sample, control source/target totals are522,712,846/519,752,654B,
candidate1,180,565,504/1,157,816,504B. Final source/target/confirmation settlement
is not synchronized with sampling: the exact probe covers the missing tail,
including22,749,000candidate target bytes after its last management sample.

Each role keeps one PID/session: control493112/498113,
session5463748244420873100; candidate494194/499191,
session13188515728276293598. Startup adds outputs through sample10; no existing
native epoch changes or suspect/failed states are observed. All eight output
epochs per role are stable from10, not from9. IDs are role-scoped.

In25→40, forward native ACKs add255,560,836TCP/264,028,630QUIC B in control,
65,341,248/644,594,790B in candidate. Native work shifts materially; these
counters cannot separate accepted Originals, copies or useful repair winners.
Candidate Product flight grows12,919,924→32,984,472B (control24,651,819→
65,154,784B). At40, candidate client QUIC46/47 RTT is166.611/100.194ms versus
406.339/100.519ms control; server QUIC path_id0 RTT is212.033ms versus101.322ms,
an adverse reverse observation. No uniform native-latency or echo claim follows.
Client observed queue peak in25–40 is142,844B versus244,660B; each band includes
32NULL path-queue values, not measured zeros or command-slot counts.

### Matched healthy profile and resource/wire tradeoffs

All42control/43candidate samples confirm both independent links remain200Mbps
in both directions throughout: no10Mbps phase, jitter, configured loss or
blackout. DOWN30ms/UP70ms, limit8192, HTB burst/cburst65536 and rate=ceil match.
Client eth0/eth1 map46/47; server eth1/eth0 map46/47. All class/qdisc drop deltas
are0; no double-counting parent/child backlog or offload-derived loss claim.

| UP class service,Mbps | Control46 /47 | Candidate46 /47 |
|---|---:|---:|
| 0→15 | 96.666 /80.780 | 190.501 /188.621 |
| 15→25 | 135.645 /157.754 | 195.947 /197.890 |
| 25→40 | 166.852 /113.146 | 191.457 /191.076 |

| Whole sampled cost | Control | Candidate |
|---|---:|---:|
| Sample window,s | 41.007358 | 42.004850 |
| UP46 /47 class bytes | 689,760,580 /574,398,615 | 1,010,402,957 /1,008,992,453 |
| DOWN46 /47 class bytes | 8,493,801 /8,828,085 | 15,999,823 /14,013,137 |
| Summed UP backlog peak /final,B | 21,066,756 /9,198,894 | 19,323,664 /3,611,778 |
| Summed DOWN backlog peak /final,B | 45,791 /1,183 | 48,906 /48,906 |
| Client RSS peak /final,KiB | 346,152 /346,152 | 349,380 /335,796 |
| Server RSS peak /final,KiB | 87,984 /87,984 | 78,580 /78,580 |
| Client lifetime CPU peak /final,% | 125 /125 | 193 /193 |
| Server lifetime CPU peak /final,% | 50.6 /41.3 | 80.9 /80.9 |

For2.244×completed bytes, sampled UP/DOWN traffic grows1.597×/1.733×.
Observed UP class-byte/confirmed-byte proxy falls2.402→1.711(−28.80%);
DOWN .032920→.025423(−22.77%). Sample windows differ and exclude final tails;
these are not exact lifetime copy/retransmission ratios. Absolute CPU grows
materially and client peak RSS rises3,228KiB, while server RSS falls9,404KiB.
Rounded lifetime `ps` is not CPU ticks; no precise CPU-per-byte efficiency or
instantaneous core-saturation claim is available. Finite-server headroom remains
a practical cost, not waived by higher throughput. Final memory/queues are not
post-teardown leak measurements. Both client logs/stderr are empty; each server
has only two normal-teardown H3_NO_ERROR warnings.

Disposition: healthy independent-UP performance supports the same isolated
checkpoint and proceeding to the declared affected shared500 gate. The benefit
does not require the QoS transition, and the control's multisecond sampled
plateaus do not recur here. Longer elapsed/settlement, absolute CPU cost and
remaining variable service stay visible. No echo, DOWN, blackhole, harsh-loss,
cross-mode or release acceptance is inferred, and this does not prove optimality.

## Shared500 ordinary four-cell gate: UP gain, DOWN timing failure

This predeclared gate stops practical promotion. Relative to a16b404,011b724
raises shared UP confirmed goodput75.240→362.459Mbps and removes the control's
multisecond sampled reply/target plateaus. DOWN goodput rises8.53%, but echo
median/p95/max rise19.01%/25.07%/8.62% and the maximum body-read gap rises79.22%.
All160 echoes succeed; success does not erase the latency regression.
This is one sequential realization per cell, not proof that the quantum causes
every changed interval, nor an accepted whole-model/release result.

### Identity, matched conditions and complete outcomes

Order was control-UP39206,candidate-UP28579,control-DOWN75855,candidate-DOWN2857.
All four drivers exit0 in46.006416/42.005313/41.006080/41.005369s.
Frozen ordinary binaries are a16b404 and011b724; no diagnostic flags, new build,
controller/queue/quantum adjustment or per-role override was used.
The result stems are `mixed-combined-{up,down}-ordered-feedback-shared-
{control,candidate}-{up,down}-0911` under `./.tmp/reflection/results/`.
The [raw archive](./ORDERED_FEEDBACK_SHARED_20260911.raw.tar.gz) contains26
regular files: all four five-file results, four driver logs, unchanged run.py
and shape.sh;733,084B. Gzip integrity,tar comparison and every decompressed
member's byte comparison pass. No configs,credentials,binaries or reverse-order
follow-up data are included.

All170 management/shape rows (46/42/41/41) retain500Mbps rate=ceil on both
interfaces,65536B burst/cburst,8192 netem limit, physical UP70ms/DOWN30ms,
zero jitter, no configured loss, no blackhole and zero class/qdisc drop deltas.
Both Docker networks remain attached, but traffic uses47: client eth1/server
eth0; unused46 contributes only42B per role/cell. This is one active shared cut,
not independent capacity aggregation. Each role has one process/session and
four active native identities stable from sample1 onward; no suspect/failed
path state appears. Native epochs are role-specific, not cross-role IDs.

| Ordinary outcome | UP control | UP candidate | DOWN control | DOWN candidate |
|---|---:|---:|---:|---:|
| Confirmed upload /HTTP body,B | 428,146,688 | 1,896,284,160 | 1,832,624,456 | 1,989,019,582 |
| Probe duration,s | 45.523054 | 41.853765 | 40.000358 | 40.000153 |
| Whole goodput,Mbps | 75.240 | 362.459 | 366.522 | 397.802 |
| First local write /body,s | .106385 | .105157 | .577328 | .583844 |
| First upload confirmation,s | .409480 | .408451 | — | — |
| Maximum local upload-write gap,s | 1.488886 | .403995 | — | — |
| Maximum confirmation /body-read gap,s | 8.879382 | .708430 | .207720 | .372282 |
| Raw bins /zero bins | 46 /24 | 42 /0 | 40 /0 | 40 /0 |
| Echo successes /attempts | — | — | 80 /80 | 80 /80 |
| Echo p50 /p95 /max,ms | — | — | 225.620 /424.413 /535.156 | 268.514 /530.808 /581.266 |
| Maximum echo-success spacing,s | — | — | .712793 | .848482 |

Both uploads finish1/1, accepted=target-confirmed, exact=true, no upload errors
or censoring. They offer different amounts of work; this is not equal-work
completion time. DOWN is HTTP200 with one duration-limited partial8GiB request,
not completion of that full object. Each DOWN echo sends/receives5120B in80
64B requests, nominal500ms cadence and3s timeout, with no failed/disconnected
attempt. Candidate's final echo ends40.121014s, beyond nominal40s, and remains
included. UP has no competing echo; its confirmation gaps are not echoed latency.

### Complete service phases and adverse chronology

Means below use every raw one-second bin in the indicated half-open probe
interval, without trimming zeros. Last upload bins may be partial settlement
bins; short confirmation/body bursts above500Mbps are application release,
not evidence that the physical cut exceeded its configured rate.

| Raw-bin phase,Mbps | UP control | UP candidate | DOWN control | DOWN candidate |
|---|---:|---:|---:|---:|
| 0–5 | 46.895 | 239.404 | 278.871 | 273.739 |
| 5–15 | 29.484 | 380.595 | 403.065 | 447.451 |
| 15–25 | 83.721 | 379.179 | 378.987 | 413.692 |
| 25–40 | 88.515 | 372.142 | 363.068 | 395.462 |
| After40,available bins | 121.819 | 396.692 | — | — |

DOWN startup body service falls1.84%; subsequent phase throughput improves.
Echo deterioration is spread across phases, not only the single largest value.
Quantiles reproduce the probe's sorted nearest-index rule; phases use actual
attempt starts, not nominal scheduling slots.

| DOWN echo start phase | n each | Control p50 /p95 /max,ms | Candidate p50 /p95 /max,ms |
|---|---:|---:|---:|
| 0–5 | 10 | 229.683 /325.021 /325.021 | 235.826 /488.348 /488.348 |
| 5–15 | 20 | 233.897 /501.217 /535.156 | 277.555 /530.808 /581.266 |
| 15–25 | 20 | 261.956 /367.113 /381.873 | 328.909 /553.055 /554.155 |
| 25–40 | 30 | 172.537 /282.313 /290.213 | 249.382 /400.032 /514.305 |

Control's worst body gap is startup .577328→.785048s,58,192→65,536 bodyB.
Candidate's is22.897019→23.269301s,1,147,355,934→1,147,367,934B; the next
12,000B read does not identify the missing Product/native range.
Candidate echo45 overlaps that body gap:22.840983→23.258414s,417.431ms.
Control's worst echo22 is11.005758→11.540914s; candidate's echo23 is
11.502135→12.083402s. Candidate also has a15.116617→19.230080s cluster
of eight completed echoes with391.526–554.155ms latencies, and echo78 at
39.350719→39.865025s takes514.305ms. None is hidden by an all-success status.
All160 full attempt records and exact body-gap counters remain in the archive.

UP management corroborates different service, without identifying exact
per-frame causality. Control's client-delivered reply counter stays417B from
samples20→28 (Unix1789069616103→1789069624104); server target/reply counters
stay222,474,354B/571B at24→29 (1789069620104→1789069625104).
The8s and5s sampled plateaus overlap but are not the probe's exact8.879382s
gap endpoints: the upload JSON does not retain those endpoints.
Candidate source,target and both reply counters advance at every one of41
adjacent samples. Its final management target is1,855,826,542B,40,457,618B
short of final exact probe settlement; do not truncate or extrapolate that tail.
Control's final sampled source/target reach428,146,688B.
Management captures and the probe have no saved exact shared wall-start anchor;
same-numbered seconds below are coarse bands, not request-level joins.

### Native allocation, physical queues and resources

DOWN's latency shift coexists with increased shared/native residence.
The following native quantiles use samples5–40,36 QUIC samples or108
TCP-path samples per cell; they are not echo-carrier measurements.

| DOWN server native context | Control | Candidate |
|---|---:|---:|
| QUIC RTT p50 /p95 /max,ms | 215.750 /385.188 /551.865 | 263.845 /522.415 /551.092 |
| TCP RTT p50 /p95 /max,ms | 218.769 /436.601 /522.824 | 267.757 /519.079 /580.292 |
| QUIC delivery-rate p50 /p95,Mbps | 395.566 /437.740 | 365.236 /381.606 |
| TCP per-path delivery-rate p50 /p95,Mbps | 35.689 /124.894 | 89.798 /171.290 |
| QUIC flight p50 /p95,B | 9,646,248 /14,759,687 | 9,914,256 /18,587,052 |
| TCP per-path flight p50 /p95,B | 993,328 /5,218,592 | 2,423,952 /7,060,448 |
| TCP aggregate socket Send-Q p50 /max,B | 3,872,276 /11,007,740 | 6,260,018 /15,538,934 |
| TCP aggregate NOTSENT p50 /max,B | 48,424 /479,288 | 0 /295,392 |
| Final sampled native TCP /QUIC ACKedB | 756,925,382 /1,530,438,725 | 1,038,165,396 /1,269,759,515 |

TCP share of these native ACK bytes rises33.092→44.983%; this includes native
control/copy bytes and is NOT measured Original lane allocation or useful-copy
ownership. Ordinary telemetry cannot provide accepted Original×lane/cause
conservation. All TCP socket snapshots identify BBR; kernel minrtt stays
100.018–100.020ms across the two cases, while smoothed RTT varies widely.
BBR's windowed mrtt is a different field, not a new propagation measurement.
Observed NOTSENT is lower while Send-Q/RTT are higher, so one queue scalar
cannot stand in for total delay.

The candidate's coarse sample15–19 band has QUIC RTT497.114–551.092ms,
DOWN HTB backlog16.754–29.870MB and TCP Send-Q10.325–15.539MB. Every native
TCP/QUIC ACK counter progresses. Control's same band has lower endpoints
but its own queue/RTT peaks; candidate's late25–40 QUIC RTT median251.309ms
versus170.669ms accompanies the sustained late echo shift.
Candidate sample22→24 also progresses TCP654,700,640→692,719,078B and
QUIC578,230,904→660,667,295B. This excludes a wholly frozen sampled carrier,
not a delayed critical byte or local Input residence. No exact echo-to-output,
native handoff/decode or Input-guard timing was captured.

| Sampled whole-cell cost | UP control | UP candidate | DOWN control | DOWN candidate |
|---|---:|---:|---:|---:|
| Sample window,s | 45.006213 | 41.005118 | 40.005887 | 40.005100 |
| Active47 UP class bytes | 482,275,851 | 2,222,673,577 | 46,979,419 | 38,747,547 |
| Active47 DOWN class bytes | 11,870,376 | 62,134,940 | 2,379,297,575 | 2,406,246,587 |
| UP backlog peak /final,B | 22,825,520 /0 | 16,707,307 /4,471,394 | 150,370 /0 | 158,124 /32,011 |
| DOWN backlog peak /final,B | 75,294 /0 | 91,425 /34,732 | 29,971,974 /2,810,816 | 29,870,398 /11,279,382 |
| Client RSS peak /final,KiB | 402,996 /383,540 | 366,524 /366,524 | 70,388 /70,388 | 79,380 /79,380 |
| Server RSS peak /final,KiB | 39,296 /39,296 | 57,348 /57,348 | 392,664 /371,516 | 325,316 /325,316 |
| Client lifetime CPU peak /final,% | 112 /112 | 179 /179 | 82.6 /82.5 | 76.9 /76.9 |
| Server lifetime CPU peak /final,% | 35.9 /21.7 | 108 /108 | 196 /196 | 188 /188 |

DOWN HTB backlog p50/p95 rises9,319,052/17,325,850→12,203,796/28,655,236B.
Its return backlog median falls86,669→69,526B. More bulk is therefore not
equivalent to uniformly less queueing. DOWN class bytes/bodyB improve
1.298301→1.209765(−6.82%), return bytes/bodyB .025635→.019481(−24.01%);
lower absolute CPU/server RSS and better wire ratios do not waive worse timing.

UP has4.429×completed work and4.609×UP/5.234×DOWN sampled wire. Its UP
class-byte/confirmed-byte proxy rises1.126427→1.172120(+4.06%), reverse
.027725→.032767(+18.18%). This is adverse observed traffic cost, not a pure
wire-efficiency win. Sample40 Product flight19,183,585→57,631,344B and
client QUIC native flight6,682,241→9,182,448B belong to different accounting
domains, not a synchronized final settlement. Client native TCP/QUIC ACK progress at25→40 is
16,182,944/118,696,732→118,977,204/681,678,550B; it corroborates useful
progress but is not unique payload attribution. Server QUIC RTT over sample10
onward has median100.097→112.222ms; client111.726→156.397ms.

All wire ratios use unequal sampled windows and complete useful-byte
denominators, not exact lifetime amplification. Rounded process-lifetime
`ps` is not interval CPU or CPU-seconds; no precise per-byte CPU regression,
core saturation or leak follows. Higher absolute UP CPU/server memory still
matters on a finite host. Queue NULL is unobserved, not zero or a full command
lane. Both UP client logs and all four probe stderr files are empty. Each UP
server logs one H3_NO_ERROR at teardown; DOWN clients log a final Broken pipe
or reset, servers RemoteClosed then H3_NO_ERROR. These follow successful
duration-limited probes; no additional independent echo failure is invented.
Drivers retain the existing HTB large-quantum warnings.

### Full raw one-second series and bounded disposition

The archive is authoritative for all168 raw bins and160 echo attempts.
Displayed bins retain the recorded precision; an em dash means no bin, not zero.

| Bin start,s | UP control | UP candidate | DOWN control | DOWN candidate |
|---:|---:|---:|---:|---:|
| 0 | 4.576 | 10.694 | 2.097 | 2.620 |
| 1 | 151.351 | 137.792 | 224.920 | 141.010 |
| 2 | 0.000 | 390.715 | 279.305 | 369.815 |
| 3 | 78.547 | 387.587 | 633.436 | 591.397 |
| 4 | 0.000 | 270.230 | 254.595 | 263.852 |
| 5 | 201.807 | 295.809 | 532.677 | 620.188 |
| 6 | 0.000 | 432.675 | 292.549 | 253.366 |
| 7 | 0.000 | 405.109 | 505.894 | 531.353 |
| 8 | 0.000 | 402.519 | 398.143 | 410.269 |
| 9 | 0.000 | 430.692 | 398.837 | 442.455 |
| 10 | 0.000 | 408.756 | 451.658 | 406.304 |
| 11 | 93.035 | 324.852 | 352.700 | 305.137 |
| 12 | 0.000 | 292.872 | 344.007 | 461.916 |
| 13 | 0.000 | 468.730 | 316.197 | 440.646 |
| 14 | 0.000 | 343.933 | 437.987 | 602.876 |
| 15 | 0.000 | 480.927 | 348.131 | 398.897 |
| 16 | 79.839 | 340.499 | 453.843 | 464.995 |
| 17 | 226.558 | 506.388 | 394.315 | 375.625 |
| 18 | 183.996 | 401.125 | 372.877 | 416.039 |
| 19 | 346.819 | 333.957 | 375.065 | 381.170 |
| 20 | 0.000 | 522.479 | 374.128 | 520.909 |
| 21 | 0.000 | 345.300 | 412.743 | 398.465 |
| 22 | 0.000 | 222.394 | 428.694 | 379.545 |
| 23 | 0.000 | 278.619 | 321.416 | 495.880 |
| 24 | 0.000 | 360.105 | 308.660 | 305.399 |
| 25 | 0.000 | 314.957 | 473.380 | 470.111 |
| 26 | 0.000 | 459.705 | 364.099 | 407.372 |
| 27 | 0.000 | 321.566 | 351.684 | 397.715 |
| 28 | 94.735 | 271.190 | 344.143 | 389.942 |
| 29 | 0.000 | 426.350 | 307.995 | 392.069 |
| 30 | 0.000 | 414.106 | 357.554 | 300.194 |
| 31 | 0.000 | 388.963 | 359.270 | 503.650 |
| 32 | 321.817 | 352.986 | 340.933 | 357.794 |
| 33 | 71.399 | 417.057 | 348.380 | 427.379 |
| 34 | 211.052 | 391.927 | 305.055 | 363.224 |
| 35 | 238.296 | 386.400 | 348.138 | 304.053 |
| 36 | 93.599 | 387.257 | 380.176 | 473.132 |
| 37 | 0.000 | 304.753 | 411.484 | 364.082 |
| 38 | 84.409 | 449.629 | 368.815 | 379.505 |
| 39 | 212.423 | 295.285 | 384.919 | 401.711 |
| 40 | 203.842 | 396.800 | — | — |
| 41 | 0.000 | 396.584 | — | — |
| 42 | 81.426 | — | — | — |
| 43 | 274.922 | — | — | — |
| 44 | 63.300 | — | — | — |
| 45 | 107.424 | — | — | — |

The useful UP change survives this topology, but this DOWN realization fails
the predeclared timing/non-regression gate. Larger local synchronous Input
service remains a possible contributor; increased native RTT/backlog and
changed carrier allocation are observed confounders. The changed client request
ACK/MAX owner is not the unchanged server response-ACK owner, yet short
requests can still exercise it. Neither source reachability nor correlated
global RTT assigns an individual echo's delay.

Root selected one unchanged reverse-order healthy DOWN pair, candidate then
control, to distinguish repeatability/order variation before any further
mechanism change. It is not an equal-load causal test and cannot alone isolate
Input quantum from changed offered bulk or native history. Preserve both
orders, including contrary outcomes; do not promote on a favourable scalar or
add repeats/parameters until an outcome is obtained. The four-cell archive
above is closed independently of that follow-up. No broader matrix or optimality
claim follows from this checkpoint.

## Reverse-order shared DOWN: echoed-latency harm repeats

The one predeclared order discriminator is now closed: candidate5956 then
control7714,011b724 versus a16b404, unchanged ordinary binaries and profile.
Both drivers exit0 in41.014422/41.005408s. No additional repeat or runtime
adjustment follows. Candidate echo median/p95/max are again worse, by
44.96%/53.82%/96.16%, despite7.03% higher bulk goodput. The maximum body-gap
ordering reverses, so that scalar is not a consistent regression across pairs.
Promotion remains stopped; reverse order does not isolate quantum causality
from native history, carrier allocation or different delivered bulk load.

The [separate reverse-pair archive](./ORDERED_FEEDBACK_SHARED_REVERSE_20260911.raw.tar.gz)
is364,594B with14 regular files: both five-file result cells, two driver logs,
run.py and shape.sh. Gzip,tar and all decompressed-byte comparisons pass.
It retains every80 raw body bins and159 echo attempts; none is discarded,
and no first-pair result is replaced. Result stems are
`mixed-combined-down-ordered-feedback-shared-reverse-{candidate,control}-down-0911`.
There are no configs,credentials or binaries in the archive.

| Reverse pair,control→candidate | Control | Candidate |
|---|---:|---:|
| HTTP200 duration-limited body,B | 1,887,752,556 | 2,020,457,306 |
| Body duration,s | 40.000334 | 40.000157 |
| Whole goodput,Mbps | 377.547 | 404.090 |
| First body,s | .584497 | .580545 |
| Maximum body gap,s | .346357 | .274600 |
| Echo successes /attempts | 80 /80 | 79 /79 |
| Echo p50 /p95 /max,ms | 240.086 /342.481 /402.024 | 348.040 /526.798 /788.622 |
| Maximum success spacing,s | .683616 | 1.035332 |
| Echo request /response bytes | 5,120 /5,120 | 5,056 /5,056 |

Both40-bin body series have zero zero-bins; each transfer remains one partial
8GiB HTTP request. All attempted echoes succeed, with no timeout/disconnect.
Candidate's79 rather than80 attempts reflects its actual serial scheduling
history, not a fabricated failed80th attempt. Quantiles include all successes.

| Probe phase | Control /candidate body,Mbps | Echo n,control /candidate | Control echo p50 /p95,ms | Candidate echo p50 /p95,ms |
|---|---:|---:|---:|---:|
| 0–5 | 271.896 /291.736 | 10 /10 | 198.821 /353.527 | 209.303 /635.605 |
| 5–15 | 406.871 /432.958 | 20 /19 | 230.995 /342.481 | 391.521 /635.777 |
| 15–25 | 387.472 /423.632 | 20 /20 | 207.240 /309.627 | 372.657 /486.427 |
| 25–40 | 386.601 /409.266 | 30 /30 | 264.684 /335.443 | 316.888 /423.407 |

Candidate's worst echo14 is7.158169→7.946791s,788.622ms; echo9 at
4.521128→5.156733s and echo22 at11.541699→12.177476s each exceed635ms.
Control's worst echo51 is25.510588→25.912612s,402.024ms. Candidate's
maximum body gap is startup .580545→.855146s,58,192→123,728B; echo1
at .503506→.985875s overlaps it. Control's worst body gap instead occurs
32.259009→32.605366s,1,524,928,052→1,524,940,052B, overlapping portions
of echoes64/65. These are exact probe-clock intervals, not reconstructed
native byte ranges or management wall-time matches.

All82 saved profiles independently confirm500Mbps rate=ceil,65536B bursts,
8192 netem queues, physical UP70/DOWN30ms, zero loss/jitter/blackhole and
zero class/qdisc drop deltas. Only47 materially contributes; unused46 adds42B
per role again. Each role/session retains four active native path/epoch
identities from sample1 onward, with no suspect/failed states. Candidate
session787599877491418959 and control8332347493666251804 are distinct.

| Reverse-pair sampled cost/context | Control | Candidate |
|---|---:|---:|
| Sample duration,s | 40.005199 | 40.014224 |
| DOWN47 class bytes | 2,384,889,212 | 2,397,685,386 |
| UP47 class bytes | 45,051,364 | 36,739,177 |
| DOWN backlog p50 /p95 /max,B | 10,832,282 /16,570,970 /18,900,356 | 16,462,092 /25,548,830 /29,975,905 |
| DOWN final backlog,B | 10,832,282 | 10,649,646 |
| UP backlog p50 /max /final,B | 76,960 /114,492 /29,673 | 62,136 /111,747 /28,830 |
| Client RSS peak /final,KiB | 71,084 /71,084 | 82,120 /82,120 |
| Server RSS peak /final,KiB | 399,756 /385,556 | 322,524 /316,760 |
| Client lifetime CPU peak /final,% | 82.8 /82.8 | 79.5 /78.9 |
| Server lifetime CPU peak /final,% | 200 /200 | 187 /187 |
| Server QUIC RTT p50 /p95 /max,ms | 231.557 /308.932 /332.996 | 335.609 /458.716 /538.665 |
| Server TCP RTT p50 /p95 /max,ms | 231.241 /332.074 /346.781 | 339.689 /495.302 /556.705 |
| Server QUIC flight p50 /p95,B | 10,454,400 /14,649,228 | 11,943,087 /16,252,236 |
| Server TCP per-path flight p50 /p95,B | 1,390,080 /3,585,248 | 2,257,432 /8,556,232 |
| Final sampled native TCP /QUIC ACKedB | 711,220,256 /1,579,847,532 | 968,273,942 /1,335,224,830 |

Native distributions again use samples5–40, not echo-winner attribution.
The native/shared-queue shift repeats with the echo shift: candidate coarse
samples7→11 have DOWN backlog17.764→29.976MB and QUIC RTT239.195→458.716ms,
with a524.475ms intermediate sample. TCP ACK counters205,669,886→317,506,304B
and QUIC130,296,449→250,519,345B progress throughout that band. This is
context for candidate's early echo-tail cluster, not an exact causal join.
Higher RTT and lower aggregate CPU can coexist; neither rules out individual
actor holds or identifies the decoded/writer boundary of a delayed echo.

Observed DOWN class-byte/body-byte proxy improves1.263348→1.186704(−6.07%),
return .023865→.018184(−23.81%). Server CPU/RSS fall while client RSS rises;
these tradeoffs repeat and still do not waive worse echoed latency. Sampling
excludes final application tails, native bytes are not unique Originals,
and lifetime CPU is not instantaneous CPU or equal-work efficiency. Both
stderr files are empty; the two client Broken pipe and server RemoteClosed/
H3_NO_ERROR sequences are after successful duration-limited captures.

The decisive practical result is repeated echoed-latency harm in both orders,
not a repeated body-gap penalty or proof of a particular local owner defect.
Root therefore selected a narrow client-quantum and deferred-slot residence
observer first, with no runtime fix or further favourable repeat. Summed/union
occupancy can test whether those changed boundaries directly explain the extra
100ms-class latency; it does not assign any remaining wait. A frame arriving
only late cannot establish a long already-decoded Input hold. Native/writer/
decode and indirect allocation remain alternatives requiring their own exact
join if this boundary is too small. No policy parameter, global acceptance or
claim of optimality is justified by the current ordinary samples.

## Feedback-quantum observer: measured boundary is too small

This information capture falsifies direct feedback-quantum/deferred-slot
occupancy as the material owner of its echoed-latency tail. Across both selected
streams, every measured quantum occupies only1.836ms of wall-interval union in
total, and the union including all measured deferred intervals is16.811ms over
39.623923s. The run nevertheless has716.686ms echo p95 and934.747ms maximum.
This does not rule out indirect allocation effects or other client/carrier work;
it provides no basis to shrink the finite feedback quantum or promote performance.

### Capture identity and complete measured scope

Root feature build92718 exits0 in1m23s with the one existing unused-helper
warning. Ordinary runtime011b724 plus the one-file temporary
`feedback-quantum-observer-0911.patch` was frozen at
`./.tmp/reflection/bin/feedback-quantum-observer-20260911/mptunnel`;
all source instrumentation was reversed before traffic. Only
`client_feedback_quantum` events were enabled. Lab96888 and the concurrent
existing direct-echo worker18374 both exit0; lab driver duration41.004744s.
No policy,shape,controller or service rule changed. This diagnostic and its
tiny additional direct TCP connection are not an ordinary speed comparator.

The [raw archive](./FEEDBACK_QUANTUM_SHARED_20260911.raw.tar.gz) is201,289B,
13 regular files: five result files under
`./.tmp/reflection/results/mixed-combined-down-feedback-quantum-shared-down-0911/`,
build/driver logs,exact patch,direct-echo JSONL/stderr and unchanged helper,
run.py and shape.sh. Gzip,tar and every decompressed-byte comparison pass.
It preserves all40 raw body bins,76 MPP and100 direct echo attempts without
trimming or repetition below. No configs,credentials or binaries are archived.

ClientPID501844,session15081739296404950121 emits206 contiguous events,
seq1–206:133 quanta and73 deferred records. Stream0/port10022 is the echo;
stream1/port8080 is the bulk request. Every quantum completes, all73 barriers
join exactly to a subsequent same-stream/kind/range deferred consumption;
none errors,censors or remains unmatched. Consumption is Input selection,
not DATA application or successful local delivery.

| Measured quanta | Echo stream0 | Bulk stream1 | Both |
|---|---:|---:|---:|
| Quanta | 129 | 4 | 133 |
| ACK /MAX frames | 119 /74 | 4 /1 | 123 /75 |
| Successfully novel ACKs | 76 | 1 | 77 |
| Elapsed sum,µs | 1,729 | 37 | 1,766 |
| Pre-guard sum,µs | 2 | 0 | 2 |
| Held-through-unlock sum,µs | 1,681 | 35 | 1,716 |
| Maximum elapsed,µs | 84 | 33 | 84 |
| Wall elapsed union,µs | 1,796 | 40 | 1,836 |
| Wall held union,µs | 1,762 | 38 | 1,800 |

The finite quanta contain70 single-frame,61 two-frame and2 three-frame groups;
maximum additional-ready count is3. Bulk's four quanta end within .272899s
of its first quantum; the changed request-feedback scope is not a bulk response
ACK-processing trace. Barriers are60 none,58 Probe,14 DATA and1 Receipt.
Deferred monotonic sums/maxima are DATA1,461/216µs,Probe13,482/3,777µs,
Receipt129/129µs; all73 total15,072µs,wall union15,117µs. Thirteen DATA
barriers are echo replies and one is the bulk header range[0,208).

The maximum84µs quantum is echo seq134 at Unix1789071377143907→1789071377143991µs,
with two applied frames and DATA barrier[3200,3264). Its deferred slot is
stored1789071377143988µs and consumed1789071377144205µs (216µs monotonic). The largest
3.777ms deferred interval,seq164,is a Probe, not a delayed DATA frame.
Deferred intervals begin before the quantum unlock and overlap its tail:
never add their sums as independent CPU or wall occupancy. Their combined
wall union with all quanta is16,811µs,not16,953µs summed wall lengths.

Instant-derived integer microseconds truncate submicrosecond work; separately
read Unix stamps differ from elapsed values by at most1µs per quantum.
Thus zero pre-guard values are not mathematical zero. Pre-guard includes
ready-count work and lock acquisition, not pure lock wait. Held time ends
after guard release/notification, before event formatting/emission. All logs
are after unlock, so measured times are not total observer overhead or CPU.
The observer excludes unrelated Product guards, native writers and time before
this Input boundary. Barrier metadata lacks ingress identity; no repair/winner
carrier or per-echo decoded residence can be manufactured from these records.

### Own service and direct-path context

MPP returns HTTP200 with2,044,950,416 bodyB over40.000416s,408.986Mbps,
one duration-limited partial8GiB request. First body .578227s; maximum gap
.493925s at11.583174→12.077099s,557,515,494→557,581,030B. All40 raw bins
are positive. All76 MPP echoes succeed,4,864 request/responseB,p50/p95/max
359.938/716.686/934.747ms; maximum success spacing1.108154s. Worst echo42
is22.257505→23.192252s. Echo8 at4.000833→4.917966s takes917.133ms.
Neither is assigned to a carrier or actor by this observer.

| Own probe phase | Body,Mbps | Echo n | Echo p50 /p95 /max,ms |
|---|---:|---:|---:|
| 0–5 | 314.683 | 10 | 217.882 /917.133 /917.133 |
| 5–15 | 429.063 | 18 | 415.911 /688.184 /716.686 |
| 15–25 | 447.810 | 19 | 438.493 /679.994 /934.747 |
| 25–40 | 401.158 | 29 | 241.479 /436.051 /797.185 |

The direct worker independently starts at Unix1789071348951676502ns and runs
50s; all100 attempts succeed,6,400 request/responseB. Its full50s median
282.774ms is NOT a loaded-only median. First foreground management stamps are
1789071349566/1789071349571ms,first echo quantum1789071349804.677ms;
last management1789071389570ms and final server teardown warning1789071390635ms.
There is no exact saved
probe wall-start, so unmatched probe/direct medians cannot be subtracted into
an exact MPP stage. Coarse loaded overlap is clear: source/body counters advance
at every management adjacency1→40. Direct offsets3–39s lie inside that active
body-service interval with margin; offsets43–50s follow teardown.

| Direct worker start-offset band,s | Interpretation | n | p50 /p95 /max,ms |
|---|---|---:|---:|
| 0–1 | Early startup,not a matched loaded baseline | 2 | 100.283 /100.384 /100.384 |
| 1–3 | Startup transition | 4 | 108.909 /276.446 /276.446 |
| 3–39 | Conservative loaded interior | 72 | 361.195 /528.898 /570.255 |
| 39–43 | End/teardown transition | 8 | 100.466 /185.421 /185.421 |
| 43–50 | Post-teardown quiet | 14 | 100.226 /100.249 /100.254 |

The bypass therefore independently experiences substantial loaded common-path
or host delay, with a quiet return to≈100ms. It does not traverse MPP's logical
feedback owner, but has its own TCP connection/controller and host work; this
is not proof that a particular MPP echo has the same delay or zero extra delay.

### Physical/native cost and disposition

All41 profiles verify unchanged500Mbps rate=ceil,65536B bursts,8192 netem
limit,UP70/DOWN30ms,zero loss/jitter/blackhole and zero class/qdisc drop deltas.
Only47 materially contributes; unused46 adds42B per role. ServerPID506778 and
client501844 retain four active path/native-epoch identities from sample1.
Native distributions below use samples5–40; TCP quantiles pool three paths.

| Own sampled cost/context | Measurement |
|---|---:|
| Sample duration | 40.004548s |
| DOWN /UP47 class bytes | 2,384,181,163 /39,405,200 |
| DOWN backlog p50 /p95 /max /final,B | 15,130,020 /28,333,797 /30,003,942 /3,069,990 |
| UP backlog p50 /p95 /max /final,B | 61,158 /103,402 /110,834 /151 |
| Client RSS peak /final,KiB | 80,512 /80,512 |
| Server RSS peak /final,KiB | 324,052 /298,404 |
| Client lifetime CPU peak /final,% | 77.9 /77.8 |
| Server lifetime CPU peak /final,% | 191 /191 |
| Server QUIC RTT p50 /p95 /max,ms | 348.929 /526.576 /545.392 |
| Server TCP RTT p50 /p95 /max,ms | 352.895 /543.296 /568.560 |
| Server QUIC flight p50 /p95 /max,B | 13,801,260 /22,031,196 /25,335,895 |
| Final sampled native TCP /QUIC ACKedB | 779,978,913 /1,515,345,191 |

Socket snapshots identify BBR. These native and physical queues corroborate
loaded delay, not critical-byte ownership; sample tails/native control-copy
domains are not synchronized with final body bytes. Lifetime ps CPU is not
instantaneous CPU or observer overhead. Client log100,054B contains206 events
and one final Broken pipe warning; server316B has the final RemoteClosed and
H3_NO_ERROR. Both probe/direct stderr files are empty. No failed echo or
post-teardown memory-leak conclusion is hidden in those warnings.

Information forecast succeeds: the full measured quantum/deferred occupancy
is far too small to own this run's100ms-class excess. Native/shared delay is
independently visible through the direct companion, while any additional MPP
echo service delay remains unassigned. Stop the quantum-size hypothesis here;
do not infer a replacement timer/queue policy or ordinary performance acceptance.

## Ordinary single-underlay context: mixed tail/cost penalty remains

The declared QUIC-only58584 and TCP-only48379 cells both finish0, sequentially,
using ordinary011b724 and the same shared500Mbps zero-impairment profile.
There is no diagnostic overlay or extra direct companion. The two existing
ordinary mixed-candidate realizations are retained as comparators, not rerun.
Both single-underlay cells deliver more body bytes and lower echo p95 than
either mixed realization. This establishes a practical mixed-context penalty,
not its causal owner or a fixed protocol preference. TCP-only's median302.990ms
is worse than mixed's first268.514ms median but better than its second348.040ms;
do not turn the tail result into a uniform ranking of all metrics.

The [context archive](./ORDERED_FEEDBACK_SHARED_CONTEXT_20260911.raw.tar.gz)
is206,917B,14 regular files: both five-file results,two drivers,run.py and
shape.sh. Gzip,tar and every decompressed-byte comparison pass. Result stems
are `{quic,tcp}-combined-down-ordered-feedback-shared-context-{quic,tcp}-down-0911`.
Every80 raw one-second bins and160 echo attempts are retained there, without
trimming or reprinting the full series. No configs,credentials or binaries.
Drivers close in41.005560/41.004401s. No runtime/build/queue/controller change
or equal-load intervention was made for this comparison.

| Ordinary DOWN outcome | QUIC-only | TCP-only | Mixed first | Mixed reverse-order |
|---|---:|---:|---:|---:|
| HTTP body,B | 2,147,273,508 | 2,215,548,070 | 1,989,019,582 | 2,020,457,306 |
| Body duration,s | 40.000307 | 40.035023 | 40.000153 | 40.000157 |
| Whole goodput,Mbps | 429.451 | 442.722 | 397.802 | 404.090 |
| First body,s | .412756 | .579834 | .583844 | .580545 |
| Maximum body gap,s | .100666 | .368722 | .372282 | .274600 |
| Echo successes /attempts | 80 /80 | 80 /80 | 80 /80 | 79 /79 |
| Echo p50 /p95 /max,ms | 103.955 /155.291 /312.639 | 302.990 /348.148 /472.096 | 268.514 /530.808 /581.266 | 348.040 /526.798 /788.622 |
| Maximum echo-success spacing,s | .683827 | .667702 | .848482 | 1.035332 |

The new cells both have HTTP200,one duration-limited partial8GiB request,
40 positive raw body bins and zero failed/disconnected echoes; each echoes
5120B in each direction. TCP's final read extends body duration35ms beyond40s;
retain its actual duration and counters, not an inferred exact40s rate.
Q's maximum body gap is .513080→.613746s,48,000→113,536B; worst echo4
is2.000680→2.313319s. TCP's maximum body gap is11.379579→11.748301s,
596,295,024→596,360,507B; worst echo44 is22.005952→22.478048s.
Body and echo maxima need not identify the same service interval.

| New-cell probe phase | QUIC body,Mbps | TCP body,Mbps | QUIC echo p50 /p95,ms | TCP echo p50 /p95,ms |
|---|---:|---:|---:|---:|
| 0–5,10 echoes each | 344.759 | 350.620 | 105.002 /312.639 | 296.348 /321.981 |
| 5–15,20 each | 445.343 | 457.921 | 104.192 /153.503 | 306.289 /348.374 |
| 15–25,20 each | 437.628 | 453.903 | 103.452 /151.333 | 305.032 /341.551 |
| 25–40,30 each | 441.636 | 456.835 | 103.762 /144.841 | 299.352 /316.520 |

Both sets of41 physical samples verify500Mbps rate=ceil,65536B burst/cburst,
8192 netem limit,physical UP70/DOWN30ms,no loss/jitter/blackhole and zero
class/qdisc drop deltas. Only47 materially contributes; unused46 adds42B per
role. QUIC session6844004768702610990 keeps one native path/epoch from sample0;
TCP session17844283929592175503 keeps three from sample1,all active throughout.
TCP socket records identify BBR with kernel minrtt100.019–100.020ms. Neither
mode is a raw single-flow baseline or an equal-independent-controller control
for mixed's three TCP plus one QUIC paths.

| Sampled cost/native context | QUIC-only | TCP-only |
|---|---:|---:|
| Sample duration,s | 40.005358 | 40.004167 |
| DOWN /UP47 class bytes | 2,269,813,446 /36,995,741 | 2,337,535,995 /8,581,149 |
| DOWN backlog p50 /p95 /max,B | 1,912,320 /5,739,948 /11,787,660 | 14,075,786 /16,415,244 /16,850,144 |
| DOWN final backlog,B | 1,937,718 | 13,911,668 |
| UP backlog p50 /max /final,B | 55,498 /80,920 /84 | 16,103 /25,541 /7,950 |
| Client RSS peak /final,KiB | 37,844 /37,844 | 32,744 /26,524 |
| Server RSS peak /final,KiB | 368,200 /360,328 | 137,344 /129,828 |
| Client lifetime CPU peak /final,% | 93.7 /93.3 | 21.4 /21.3 |
| Server lifetime CPU peak /final,% | 155 /155 | 56 /53.6 |
| Server native RTT p50 /p95 /max,ms | 101.377 /149.317 /162.480 | 295.454 /341.291 /342.625 |
| Server native flight p50 /p95,B | 5,960,460 /9,771,960 | 1,920,048 /16,905,400 per TCP path |
| Server native queue p50 /p95 /max,B | 0 /0 /0 | 125,680 /130,933 /166,466 per TCP path |
| Final sampled native ACKedB | 2,200,349,184 | 2,231,778,855 |

Native distributions use samples5–40 (36 QUIC or108 TCP path samples).
QUIC's near-baseline echo coexists with≈101ms native RTT and low shared queue;
TCP's≈303ms echo coexists with≈295ms native RTT and a14.076MB median shared
queue. Mixed's native/physical tails are larger and more variable in the two
preserved cells. These coarse similarities are not matched per-request
latency-stage subtraction. They identify an important context confounder, not
the exact winning echo path or the cause of its excess residence.

Observed DOWN class-byte/body-byte ratios are1.057068/1.055060 in Q/TCP,
versus1.209765/1.186704 in mixed. Return ratios are .017229/.003873 versus
mixed .019481/.018184. Thus mixed serves fewer useful bytes with more sampled
wire overhead and higher server lifetime CPU (188/187% versus155/53.6%).
Q has higher server RSS than either mixed realization, whereas TCP is much
lower; retain that tradeoff. Native ACK counts include control,Original and
repair domains; ordinary captures do not expose accepted repair-cause/unique-
duplicate receipt counters. Neither class/body residual nor native ACK/body
subtraction proves repair volume,unnecessary copies or an allocation defect.
Timing windows and final transit/cancellation tails remain unequal.

Both stderr files are empty. Q has final client Broken pipe and server
H3_NO_ERROR; TCP has final client reset and server RemoteClosed after successful
duration-limited results. These are not additional failed echo attempts.
Lifetime CPU is not interval CPU,finite-headroom cost is not waived,and final
RSS/backlog is not a post-teardown leak measurement.

Disposition: the mode context strengthens the mixed tail/throughput/cost
concern but does not select a native or MPP policy change. Existing quantum
observation already rejects that measured boundary as the material delay owner
in its own capture; these ordinary modes do not identify the remaining owner.
No fixed TCP/QUIC preference,copy suppression,release acceptance or claim that
the overall model is proved follows. Root retains the next exact-cause decision.

## Native-competition discriminator: external TCP reproduces common delay

Ordinary011b724 QUIC-only with three independent raw TCP downloads reproduces
substantial echoed-latency inflation without any MPP TCP carrier in that
session. Relative to the preserved Q-only context cell, echo median/p95 rise
103.955/155.291→252.303/412.535ms,while native QUIC RTT and the physical
shared queue also rise. This supports independent TCP/QUIC competition as a
material contributor in this topology. It does not prove pure kernel causality,
assign every mixed echo delay,or make all MPP overhead unavoidable.

Foreground `quic-combined-down-ordered-feedback-native-competition-down-0911`
and the fixed raw sidecar both close0; driver duration41.004663s. Same500Mbps
UP70/DOWN30ms profile,ordinary executable and40s bulk+echo workload; no observer
or runtime change. The sidecar uses the existing failover_download_probe,
three synchronized fixed requests to10.238.47.20:8080,not additional MPP
paths or repeated replacement requests. Its three physical sockets are
independently identified in the saved read-only socket snapshot.

The [raw archive](./ORDERED_FEEDBACK_NATIVE_COMPETITION_20260911.raw.tar.gz)
is63,880B with14 regular files: foreground five-file result,driver,sidecar
JSON/stderr/started marker,socket snapshot,both existing probe sources,
run.py and shape.sh. Gzip,tar and all decompressed-byte comparisons pass.
It retains40 foreground and41 sidecar raw bins plus all80 foreground echo
attempts. No configs,credentials or binaries are included.

| Outcome/domain | Q-only reference | Q with external TCP | Raw three-request sidecar |
|---|---:|---:|---:|
| HTTP body,B | 2,147,273,508 | 899,977,722 | 1,420,687,824 |
| Own duration,s | 40.000307 | 40.000449 | 40.001199 |
| Own goodput,Mbps | 429.451 | 179.994 | 284.129 |
| First body,s | .412756 | .423362 | .102866 |
| Maximum body-read gap,s | .100666 | .354890 | .090955 |
| Echo successes /attempts | 80 /80 | 80 /80 | — |
| Echo p50 /p95 /max,ms | 103.955 /155.291 /312.639 | 252.303 /412.535 /773.214 | — |

Foreground is HTTP200,one duration-limited partial8GiB request,all40 body
bins positive. All80 echoes succeed with5120 request/responseB and no failure
or disconnect; maximum success spacing .881118s. The worst echo2 is
1.000710→1.773924s. Maximum body gap5.342904→5.697793s advances
37,210,170→37,222,170B. The sidecar has exactly3 started requests,3 partial,
0 completed,0 failed,0 early termination and0 replacements. Its
`complete=true` only means positive received bytes in this duration-mode
implementation; it does NOT mean three complete8GiB transfers. Its aggregate
max gap is not a per-worker gap and has no saved endpoints.

### Overlap and native evidence

The sidecar's post-connect cohort release is recorded at Unix
1789072123.459934473s,after .101843s setup. Its started marker is written by
the barrier release before all three workers issue their HTTP requests,so raw
bins share that cohort origin. First foreground management is1789072123987ms,
about .527s later; there is no exact saved foreground probe wall-start.
The raw and foreground40s measurement windows therefore overlap closely but
are not identical. Do not add179.994+284.129 as an exact common-window rate,
nor compare same-indexed bins as simultaneous independent observations.

| Own-clock phase | Foreground body,Mbps | Foreground echo n,p50 /p95,ms | Sidecar body,Mbps |
|---|---:|---:|---:|
| 0–5 | 52.969 | 10,236.113 /773.214 | 358.666 |
| 5–15 | 144.367 | 20,340.615 /491.579 | 334.262 |
| 15–25 | 224.364 | 20,246.127 /331.310 | 239.966 |
| 25–40 | 216.496 | 30,237.302 /349.890 | 255.309 |

The management0→5/5→15/15→25/25→40 bands record combined DOWN class service
491.389/490.435/489.931/484.675Mbps. Native QUIC ACK bytes progress in those
same sampled bands by32,146,342/178,551,936/293,558,100/406,783,608B.
Those physical/native counters establish simultaneous active service,not
ownership of the foreground's critical byte. Class totals include the raw
sidecar and must not be divided by foreground Q bytes as MPP amplification.

At Unix1789072139.554362450s (sidecar offset16.094428s),the saved socket
read has exactly three physical raw server8080 connections to client ports
55228/55212/55234,all BBR. Their RTTs are331.564/338.746/331.630ms,kernel
minrtt100.019–100.021ms and BBR windowed mrtt100.032–100.039ms. Aggregate
Send-Q is81,099,584B,NOTSENT72,104,608B and ACKed668,049,904B. The two
loopback HTTP socket records are excluded from these physical totals.
This is one timestamped snapshot,not a whole-run distribution or direct
measurement of the echo's residence. Native counters and queues have different
domains; Send-Q minus NOTSENT is not a reconstructed Product prefix.

All41 profiles verify unchanged500Mbps rate=ceil,65536B bursts,8192 netem
limit,zero loss/jitter/blackhole and zero class/qdisc drop deltas. Only47 is
material; unused46 adds42B per role. Session3326591589660230011 has one active
QUIC path/native epoch,stable from sample0 in both roles (client505059,
server509965). Raw TCP ownership is outside that session. The lack of MPP TCP
paths does not remove shared physical/host contention or all MPP processing.

| Same sampled cost/context | Q-only reference | Q plus external TCP |
|---|---:|---:|
| Sample duration,s | 40.005358 | 40.004435 |
| DOWN /UP47 class bytes | 2,269,813,446 /36,995,741 | 2,441,612,709 /25,466,371 |
| DOWN backlog p50 /p95 /max,B | 1,912,320 /5,739,948 /11,787,660 | 10,443,100 /15,684,790 /18,457,802 |
| DOWN final backlog,B | 1,937,718 | 953,172 |
| UP backlog p50 /max /final,B | 55,498 /80,920 /84 | 47,489 /61,555 /0 |
| Client MPP RSS peak /final,KiB | 37,844 /37,844 | 34,920 /34,920 |
| Server MPP RSS peak /final,KiB | 368,200 /360,328 | 285,000 /285,000 |
| Client MPP lifetime CPU peak /final,% | 93.7 /93.3 | 48.1 /48.1 |
| Server MPP lifetime CPU peak /final,% | 155 /155 | 121 /121 |
| Server Q RTT p50 /p95 /max,ms | 101.377 /149.317 /162.480 | 250.033 /306.804 /381.257 |
| Server Q flight p50 /p95 /max,B | 5,960,460 /9,771,960 /10,727,376 | 6,743,088 /8,998,044 /14,233,956 |
| Server Q delivery-rate p50 /p95,Mbps | 491.607 /495.378 | 252.278 /288.124 |

Native quantiles use samples5–40. Process CPU/RSS here cover mptunnel only,
not the raw client or HTTP-server work,and lifetime ps is not instantaneous
CPU or equal-work efficiency. Lower MPP CPU accompanies less foreground work;
it cannot be used as whole-host contention cost. Physical counters combine
both workloads and sampled tails differ. No accepted repair/cause/winner
observer is present,so no copy-necessity or exact wire-cause claim is available.
Foreground/sidecar stderr and foreground server log are empty; the client has
one final Broken pipe warning after a successful duration-limited capture.

The foreground echo tail is materially worse than Q-only yet lower than both
mixed p95 values530.808/526.798ms; its maximum773.214ms lies between the two
mixed maxima. These are different offered-load/allocation contexts,not a
per-stage difference to subtract. External TCP competition is sufficient to
reproduce a substantial penalty without MPP scheduling those TCP carriers;
the remaining mixed wire/tail/resource cost is still neither proved avoidable
nor waived as unavoidable. No protocol preference,native knob,MPP fix or
performance promotion is selected by this discriminator alone.

## Mixed repair chronology: closed diagnostic evidence

Capture46891 closes0 in41.011350s using the existing frozen
`feedback-quantum-observer-20260911/mptunnel` executable:011b724 ordinary
semantics,quantum observer disabled by the event filter. No new source observer,
build or policy was introduced for this run. Only the six existing repair,
receive-hole,ACK and return-feedback events listed below were enabled.
This is chronological evidence,not an ordinary speed comparison or complete
repair-volume/winner audit. In particular,the logs include duration-stop cleanup.

The [raw archive](./ORDERED_FEEDBACK_REPAIR_TIMING_20260911.raw.tar.gz) is
5,691,879B,10 safe regular files: five results under
`./.tmp/reflection/results/mixed-combined-down-ordered-feedback-repair-timing-down-0911/`,
driver,reused feature-build log/exact patch,run.py and shape.sh. Gzip,tar and
all decompressed bytes compare successfully. Every40 raw body bins and78 echo
attempts remain in the archived probe; no configs,credentials or binaries.

### Own timing and observation coverage

HTTP200 returns2,052,589,850 bodyB over40.019585s,410.317Mbps,one
duration-limited partial8GiB request. First body .583014s; every raw body bin
is positive. Maximum read gap .467867s spans11.706182→12.174050s,
560,167,574→560,179,574B. All78 echoes succeed,4,992 request/responseB,
p50/p95/max388.370/577.826/1011.305ms,maximum success spacing1.054411s.
Worst echo19 is9.505479→10.516784s; no failed or disconnected attempts are
omitted. The read and echo maxima are different intervals. Actual probe/echo
durations extend about19ms beyond40s and are retained,not forced to40s.

| Own probe phase | Body,Mbps | Echo n | Echo p50 /p95 /max,ms |
|---|---:|---:|---:|
| 0–5 | 298.282 | 10 | 209.052 /327.463 /327.463 |
| 5–15 | 442.488 | 19 | 376.071 /662.967 /1011.305 |
| 15–25 | 429.650 | 20 | 458.876 /520.494 /544.283 |
| 25–40 | 413.855 | 29 | 405.214 /581.163 /601.946 |

| Enabled event | Client records | Server records |
|---|---:|---:|
| receive_hole | 126,228 | 0 |
| receive_hole_release | 100,633 | 0 |
| stream_ack_received | 79 | 27,330 |
| server_data_ack_recovery | 0 | 12,782 |
| server_repair_carrier_accept | 0 | 15,193 |
| feedback_return | 957 | 1,007 |

Client seq1–227897 and server seq1–56312 are contiguous within these filtered
logs;there are no client_feedback_quantum events. This does not establish
coverage of unlogged producers. Client log is63,608,006B/227,898lines,server
28,084,624B/56,314lines:91,692,630B combined. Their costs are material
observation context,not a measured zero-overhead trace. Timing cannot replace
either ordinary mixed realization or prove that the ordinary echo winner had
the same history. Decision,accepted-copy,ACK and receive-event counts have
different meanings; summing them or their overlapping ranges is not all repair
traffic,unique receipt bytes or proof that every copy was useful/unnecessary.

Session18012237210639543726 is stable across management and return-feedback
records. ClientPID506137 and server511027 have separate diagnostic monotonic
origins; cross-role chronology uses Unix stamps with their actual precision.
The bulk stream is1 and echo0. Events without a session field can be scoped
to this single-session capture,not reused across unrelated StreamId values.

### Matched physical/native and resource context

All41 samples verify500Mbps rate=ceil,65536B burst/cburst,8192 netem limit,
physical UP70/DOWN30ms,zero loss/jitter/blackhole and zero class/qdisc drop
deltas. Traffic materially uses47; unused46 adds42B per role. All four
path/native-epoch identities remain active and stable from sample1 in each
role. Native distributions below use samples5–40 (36 QUIC/108 TCP-path values).

| Own sampled cost/context | Measurement |
|---|---:|
| Sample duration,s | 40.011122 |
| DOWN /UP47 class bytes | 2,400,348,141 /36,532,529 |
| DOWN backlog p50 /p95 /max /final,B | 20,069,158 /31,575,698 /35,138,136 /6,065,808 |
| UP backlog p50 /p95 /max /final,B | 67,480 /83,294 /88,290 /25,380 |
| Client RSS peak /final,KiB | 87,564 /87,564 |
| Server RSS peak /final,KiB | 296,536 /281,596 |
| Client lifetime CPU peak /final,% | 85.6 /85.1 |
| Server lifetime CPU peak /final,% | 160 /160 |
| Server QUIC RTT p50 /p95 /max,ms | 406.718 /553.488 /649.081 |
| Server TCP RTT p50 /p95 /max,ms | 403.178 /566.367 /600.848 |
| Server QUIC flight p50 /p95 /max,B | 13,609,596 /21,859,860 /25,598,760 |
| Server TCP per-path flight p50 /p95 /max,B | 4,148,520 /7,415,208 /8,696,688 |
| Final sampled native TCP /QUIC ACKedB | 1,067,379,504 /1,236,526,113 |

Final management client local-delivery2,052,582,986B and server source-read
2,117,663,146B are different IO domains and timestamps; neither is a final
unique-body/copy conservation equation. Native counters include control/copy
traffic. Lifetime ps CPU is not interval CPU or exact logging overhead;
sampled RSS and final queues are not post-teardown leak measurements.

Stop censoring is explicit: client Broken pipe is logged at
Unix1789072912603ms; a final accepted path_failure_reinjection record appears
at1789072912676ms,after that local stop. Last client feedback terminal event
is1789072912757ms,while final server/client management is
1789072912550/1789072912545ms. Thus post-stop recovery/return facts must not be promoted
to completed useful workload service. The server then logs RemoteClosed and
H3_NO_ERROR;probe stderr is empty. Existing all-active sampled states do not
contradict later teardown labels,and those labels are not an independent
mid-workload carrier failure claim.

Exact range/admission/receiver attribution is outside this aggregate subsection;
no unsupplied winner or unavoidable-overhead conclusion is inferred from these
counters. The retained diagnostic supports that separate causal review,not a
runtime change or performance promotion.

### Exact copy/receipt chronology

Independent range review scopes these records to session18012237210639543726,
bulk stream1. All15,190 bulk accepted copies total206,389,209 payload bytes.
None even partly crosses the server's previously logged contiguous stored
frontier. This excludes that concrete violation, not every sparse-positive
violation: the event does not expose the complete positive set.

Client frontier observations strictly earlier in Unix milliseconds already
wholly cover15,117 copies/204,128,529B (98.9048%). These are receipt witnesses,
not sender knowledge or proof a repair was ex-ante unnecessary. Same-ms
chronology is excluded. The dominant pattern follows configured70ms return
propagation, not a demonstrated large server Input stall:

| Accepted cause | Copies / payload B | Already received copies / B | Receipt→admission p50 /p95,ms | Admission→server cover p50 /p95,ms |
|---|---:|---:|---:|---:|
| Persistent gap | 14,460 /183,334,447 | 14,439 /183,027,847 | 70 /73 | 1 /6 |
| Active tail | 714 /22,253,394 | 677 /21,085,746 | 70 /74 | 2 /8 |

The owner join uses the latest preceding decision with identical start,
containing end and exact target underlay/path/incarnation, without inventing
split-frame continuations. It joins12,732 persistent copies/171,081,881B;
1,728 TCP copies/12,252,566B remain owner-unjoined. QUIC-owner→TCP contributes
12,717/170,865,481B;12,702/170,646,481B are already received before admission.
Their receipt lead is70/73ms and subsequent server-cover delay1/6ms.
All12,717 critical heads release on Original QUIC ingress: none of the only
ten accepted QUIC-copy intervals covers those heads. This identifies the head's
provenance, not every byte's arrival in its containing extent/frame.

Long-delay witness [1136212242,1136226842): client receipt
Unix1789072894973ms (client lines118751–118753); server decision
1789072895693,acceptance1789072895694,positive cover1789072895695ms
(server lines31504/31506/31508). Original owner QUIC0/incarnation4 has
age1,216,092us; alternate TCP0/incarnation1 projected ETA895,980us.
Thus full receipt precedes admission721ms,server knowledge follows1ms later.
These logs do not split generation/native-return/local Input residence.
It is not dominant volume: >80ms lead is136copies/1,928,224B (0.934% of bulk
copy bytes); >100ms71/1,086,880B (0.527%); >500ms31/350,400B (0.170%).
Those descriptive bands are not runtime thresholds or physical delay bounds.

Blanket suppression has a contrary witness: [560167782,560182382) is accepted
from Original TCP2/incarnation3 to QUIC0/incarnation4 atUnix1789072884361ms
(server lines15373/15375),owner age1,059,380us,alternate ETA1,726,906us.
Client lines53809/53811 at1789072884756ms confirm both exact QUIC fragments
and release all14,600B395ms later. No earlier QUIC copy covers the head.
Here alternate recovery genuinely supplies missing ordered data.

Cross-role joins use shared Unixms,not separate diagnostic monotonic origins.
Per-stream ordering is checked; concurrent log interleaving is not lost data.
Nonprogressing arrivals and frozen fallback deadlines remain unobserved.
Next add only existing fallback/owner-estimate scalars to the decision event:
post-fallback-dominated work rejects an early-comparison change as its direct
remedy; material early work selects that model for a counterexample before a
fix. Decision-before-fallback does not imply later admission-before-fallback.
No batching change,copy ban,timer increase or performance promotion follows.

### Frozen fallback classification: closed diagnostic38507

2026-09-11; source011b724 with the temporary one-file decision-event extension,
build81130, then the runtime overlay fully reversed before traffic. Raw input:
`./.tmp/reflection/results/mixed-combined-down-ordered-feedback-repair-deadline-down-0911/`.
This section concerns exact recovery chronology, not ordinary performance;
full probe/profile/resource accounting is separate.

One session10333278507748740352, bulk stream1, is observed. Server/client event
sequences are complete1..53452/1..221943, with no Unix timestamp regression
after sequence ordering. Cross-role joins use shared Unix milliseconds, never
the processes' independent t_mono origins. Client contiguous-frontier advances
are conservative positive receipt witnesses, not sender knowledge or complete
nonprogressing-arrival history. No bulk accepted copy partly crosses the
previously logged server contiguous stored_frontier; the complete sparse
positive set is not exposed, so this is not a general authority audit.

The11934 queued persistent-recovery decision events classify3198 pre-fallback
and8736 post-fallback at their EXISTING observation epoch. Their generated
frame counts are10395 single,1519 double and20 triple. Queued decisions are
not carrier accepts. All bulk accepted copies total14274/197872941 payload B:
13453/171149109B persistent and821/26723832B tail. Tail decisions are outside
this added timing event and remain separately classified.

As in46891, match a persistent accept only to the latest preceding decision
with identical start, containing end, and exact target underlay/path/incarnation.
This joins11897 distinct decisions to11897 accepts; it does not infer timing
for split successors. Quantiles below are per accepted copy, not byte-weighted.
Receipt→accept uses only strictly earlier full-cover witnesses; subsequent
server-cover delay is measured for that same already-received subset.

| Decision epoch | Accepted copies / payload B | Persistent-byte share | Already client-received copies / B | Receipt→accept p50/p95,ms | Accept→server cover p50/p95,ms |
|---|---:|---:|---:|---:|---:|
| Before retained fallback | 3162 /42396292 | 24.772% | 3158 /42337892 | 71 /74 | 2 /7 |
| At/after retained fallback | 8735 /117454867 | 68.627% | 8720 /117238467 | 70 /74 | 1 /6 |
| Unjoined timing | 1556 /11297950 | 6.601% | 1555 /11295350 | 71 /75 | 1 /6 |

One already-received post-fallback copy has no later logged server cover;
the post-group last column therefore has8719 observations, not8720. The
pre-associated payload is21.426% of ALL bulk copy bytes. Even assigning every
unjoined persistent byte to early decisions gives only27.136% of all copy
bytes; this is an observed-category ceiling, not a counterfactual speed gain
or assurance that delayed work would disappear without replacement copies.

The early minority is not only startup. Using5s bands from the first server
event (not the application's start), its accepted MB are
1.987,9.792,3.130,2.310,3.756,8.278,7.889,5.254. Post-fallback MB in those
same bands are17.269,12.449,16.758,24.721,17.831,7.887,7.139,13.401.
Both early and mature work repeatedly coexist with the normal return journey.

All3162 joined early decisions have target ETA below their actual legacy
owner-completion projection. Their median owner/target projections are
778523/671738us; median retained fallback remaining is89105us. Of8735
post-fallback decisions,1364 have target ETA at least the owner projection:
the already-due fallback legitimately does not require that early comparison.
Do not derive native PTO or per-assignment timing by subtracting the logged
first-owner age from the independently retained fallback fields.

Admission-clock qualification matters. `decision_before_fallback` is exact
at observed_at; a later accepted copy need not still be early. The
decision_to_log scalar ends just before the diagnostic call, not after all
possible scheduling delay before its stamp. The accept event likewise follows
the actual mutation. Millisecond subtraction is not an exact admission clock.
Thirty early-associated copies/410808B have nominal remaining margin<=1ms;
2937/39361580B have nominal margin>10ms, but these descriptive margins are
not runtime thresholds or proof against unmeasured descheduling.

There is an exact maturation control without a cross-event clock inference:
server50008 records [1915124643,1915139243), fallback_in2us and
decision_to_log10us. The boundary has passed inside the synchronous decision
function before its return and later actor dispatch; server50010 accepts the
14600B. Intent enqueue only mutates the actor-owned sender queue, so it cannot
publish this carrier copy before that function returns. Separately,
server36793→36795 shows only52us remaining then an accept-event2ms later;
this5536B case is consistent with maturation but is not exact mutation-time
proof. Keep it in the pre-decision-associated group rather than silently
relabeling all early decisions as early admissions.

Representative late pre-fallback chronology: [1278110428,1278125028),
Original QUIC0/incarnation4. Client133025–133027 wholly covers it at
Unix1789073998359ms. Server36356 decides and36358 accepts at8427ms;
retained fallback remaining82538us, owner projection572511us, target
TCP1/incarnation2 ETA381429us, diagnostic decision-to-log6us. Server36363
covers the range at8430ms. The already-arrived Original head is proven:
none of the capture's three accepted QUIC-copy intervals covers it.

Nearby post-fallback example [1285439844,1285454444) also releases on Original
QUIC (client134139–134141,Unix1789073998547ms). Server36710/36712 at8623ms
has fallback already5610us late, owner projection623921us and target
TCP1/incarnation2 ETA669748us; server36714 covers it at8624ms. Thus normal
feedback-in-transit duplication is present on BOTH sides of the fallback,
including a case that the early owner-completion comparison does not authorize.

Useful mature recovery remains directly witnessed. For [576660968,576675568),
server16818/16820 atUnix1789073985416ms records Original QUIC0/incarnation4,
fallback496433us late and a TCP2/incarnation3 copy with ETA662548us.
Client60199 at5541ms receives the exact14600B on TCPindex1 and releases the
head125ms after copy admission. Token5's same-direction proof maps that index to the exact
server target. Only two accepted copies cover this head; the second,
server16873 at5801ms, targets TCP1/incarnation2 AFTER the first copy already
delivered. Server16887 first logs positive cover at5892ms. The second event
is385ms after the first, near its logged384674us copy deadline, not an exact
reconstruction of expiry. This is a useful alternate winner followed by a
sender-not-yet-informed retry, not evidence for banning all copies.

Disposition: the information forecast resolves a material EARLY MINORITY,
but rejects early-comparison removal as the dominant direct remedy for this
capture's mixed copy cost. Its42.396MB accepted association is a legitimate
scope for a separate owner-model counterexample, not proof that a new formula,
None, unconditional loss timing or suppression would be correct or useful.
Most classified accepted work is already post-fallback and would remain under
an early-only change. Preserve that ceiling, the useful alternate winner and
ordinary latency tradeoffs; no runtime recovery policy is selected here.

### Diagnostic38507: full service, native cost and raw archive

2026-09-11, statistics companion to the preceding exact-copy analysis. The
closed driver returns0 in41.047531s. This is source011b724 plus the frozen
2644B one-file diagnostic patch, build81130 (1m22s, only the pre-existing
unused-wrapper warning), executable
`./.tmp/reflection/bin/repair-deadline-20260911/mptunnel`.
The observer was reversed before traffic; none of these measurements promotes
the ordinary candidate or ranks it against a less-instrumented run.

The probe reports HTTP200,2,071,133,239 body B in40.000497821s:
414.221493Mbps. This is one duration-limited partial8GiB response, not a
completed object. First body is0.614160s. Maximum body-read gap0.772739s runs
from9.874723 to10.647462s, body453,763,060→453,787,060B. These are application
body offsets, not relay DSNs. The capture preserves the worst gap, not an
independent complete list of every body read or gap.

All79 sequential64B echo exchanges succeed,5,056 exact bytes each direction;
no timeout, unavailable record or restart is reported. Median/p95/max are
375.743/582.986/656.397ms. Worst echo#57 runs28.915970–29.572367s; #24 is
653.654ms at12.017202–12.670856s, and #56 is644.008ms at
28.271938–28.915946s. Largest successive-success spacing is0.775494s between
#23 and#24; that includes the probe's500ms cadence and is not one echo's RTT.
Final echo#78 completes at40.246728s, after the bulk's40s work window.

The40 RAW one-second body bins have no zero; the stored34-bin trimmed average
432.787Mbps is not whole-run goodput. Complete rawMbps, by ten-second blocks:

```text
 0– 9:   .466 146.165 457.275 473.769 403.110 451.250 348.659 578.334 412.432 358.644
10–19: 534.729 369.331 198.060 662.621 364.215 405.278 434.066 398.098 353.302 556.139
20–29: 431.528 409.160 450.546 386.235 447.200 450.387 470.535 420.987 412.904 241.247
30–39: 672.038 413.773 472.767 424.319 434.062 231.237 643.795 442.102 418.193 390.013
```

| Probe-clock band,s | Raw-bin mean,Mbps | Echoes starting in band | Echo p50/p95/max,ms |
|---|---:|---:|---:|
|0–5|296.157|10|193.932 /332.353 /332.353|
|5–15|427.828|20|378.240 /505.161 /653.654|
|15–25|427.155|20|375.743 /496.500 /504.565|
|25–40|435.891|29|439.726 /644.008 /656.397|
|30–40,overlapping late subset|454.230|20|422.852 /582.986 /588.331|

Band quantiles use the sorted successful attempts at index
round((n−1)×quantile), matching the probe convention. High one-second bursts
can reflect buffered delivery and sampling windows; they do not establish that
the500Mbps cut is misconfigured. There is no QoS/restoration phase here.

All41 service rows verify each class rate/ceil62,500,000B/s, UP70ms and DOWN30ms,
zero configured loss/jitter, no whole-UDP blackhole, and zero observed class
or netem drops. Class changes at5s epochs preserve that profile. Active traffic
is on47: clienteth1→servereth0. The four attached carriers are three TCP and
one QUIC, all using10.238.47.20:7443 despite their configured `*-46` names.
The unused clienteth0/servereth1 each add only42B. This is one shared500Mbps
cut per direction, not independent capacity for every carrier.

Over first→last class samples, active DOWN wire grows2,398,404,235B and UP
34,853,191B. Their ratios to final body are1.1580 and0.01683, respectively;
these differently bounded native/wire/application windows are not exact
lifetime amplification. Active DOWN class backlog peaks31,241,202B/2,815
packets; UP peaks177,245B/1,552 packets. Native flight and qdisc backlog can
refer to overlapping bytes and must not be added as independent queues.

The final server native-delivery counters total2,298,724,017B: TCP1,120,928,198B
(48.763%) and QUIC1,177,795,819B. These are transport ACK bytes, including
framing/copies, not successful Original payload or application delivery.
All four exact native epochs remain stable and counters never regress; every
observed path remains active. Per-carrier final observations follow. IDs are
management path/physical-instance identities, not response-copy incarnations.

| Server native path/instance | Final ACKed B | SRTT final / sampled max,ms | Native flight final / sampled max,B |
|---|---:|---:|---:|
|TCP0/4|302386151|488.322 /651.991|405440 /9501776|
|TCP1/3|456015755|485.120 /650.249|7926352 /7926352|
|TCP2/2|362526292|486.305 /654.068|1640584 /6956192|
|QUIC0/1|1177795819|487.088 /631.344|14727636 /21504037|

Summed server native flight peaks35,827,564B and ends24,700,012B. Reported
native TCP unsent queues peak530,044B summed and end125,942B; the distinct
`ss` snapshots show summed Send-Q peak17,585,108B/end9,281,880B. Send-Q includes
unacknowledged bytes and is not synonymous with unsent. QUIC final native
window is26,981,413B, versus37,506,392B sampled peak; this scalar is not a
new policy or evidence of a loss-induced stall. Client reverse native ACK
counters end11,109,731B TCP and644,386B QUIC, with the same stable-epoch
qualification. Management/native sampling times differ; no subsecond winner
or exact physical residence is derived from these aggregate observations.

| Process,one stable PID each | Lifetimeps CPU first / final / peak,% of one core | RSS first / final / peak,KiB |
|---|---:|---:|
|Client|2.7 /85.7 /85.8|32348 /92936 /94304|
|Server|3.2 /160 /160|31968 /247892 /269816|

These are lifetime `ps` percentages, not interval CPU, exclusive on-CPU work or
thread attribution; this capture has no tick/owner/perf counters. Neither CPU
per delivered byte nor post-load reclamation is established. Management has
no errors. Its final snapshot precedes probe completion: bulk client delivery
is2,069,666,495 relay B and server target-read progress2,133,139,685B. Thus
that row cannot be used as exact final settlement, leaked retention or missing
echo evidence. Client Broken pipe and server RemoteClosed/H3_NO_ERROR warnings
occur at the teardown end; the complete probe still reports no echo failure.

Observation cost is material: client61,983,301B plus server28,083,768B logs,
90,067,069B total. The six enabled event types produce275,395 diagnostic
records: client221,943/server53,452; remaining three lines are the warnings
above. No unrelated event or perf recorder is present. This reinforces the
information-only disposition; the414.221Mbps observation is not an ordinary
speed win. Exact copy/receipt conclusions remain those in the prior section.

Raw archive: [ORDERED_FEEDBACK_REPAIR_DEADLINE_20260911.raw.tar.gz](ORDERED_FEEDBACK_REPAIR_DEADLINE_20260911.raw.tar.gz).
It contains the five exact result files, closed driver, frozen observer patch,
build log, and existing run.py/shape.sh:10 regular files, no executable/config
or secret-bearing input;5,711,088B compressed,91,792,713B uncompressed.
Every archived member was compared byte-for-byte to
its source without hashes. The runner and shape scripts plus observed classes
preserve the measurement method; preceding sections state source, binary,
event filter and profile. No runtime, threshold or controller change follows.

### Diagnostic80295: ACK-admission capture, full service and costs

2026-09-11. Closed driver0 in41.004515s; source011b724 plus the frozen9058B
two-file observation patch, build44306 (1m25s, pre-existing unused-wrapper
warning only). Executable:
`./.tmp/reflection/bin/server-ack-admission-20260911/mptunnel`.
Both source changes were reversed before this single capture. The only events
are server_ack_actor_admission,server_data_ack_recovery,
server_repair_carrier_accept and stream_ack_received. No client-receive,
native-trace, perf or tick-CPU observer is enabled. The exact admission/decision
join is separate from these full-run statistics; this is not an ordinary
performance comparison or a runtime correction.

HTTP200 delivers2,023,458,600 body B in40.000054553s,404.691168Mbps. The8GiB
object is intentionally partial after40s: one partial request, none completed.
First body arrives0.583049s after probe start. Worst read gap0.516832s is
9.716537–10.233369s, body436,713,470→436,779,006B. These are body offsets,
not relay DSNs; probe.json exports the worst interval rather than every read.
There is no failover/recovery phase in this healthy cell.

All79 sequential64B echo exchanges succeed,5,056 bytes each way. Median/p95/
maximum are360.218/492.354/745.859ms; zero timeouts, unavailable records or
probe-reported restarts. Worst#44 runs22.291715–23.037574s; #22 takes715.761ms
at11.002191–11.717952s. The next largest#43 is543.625ms at
21.748071–22.291697s. Largest successive-success spacing0.835875s lies between
#21 and#22 and includes the500ms probe cadence; it is not one request's RTT.
First#0 takes100.627ms; last#78 completes39.847458s after303.187ms.

All40 RAW one-second body bins are positive. The stored34-bin trimmed mean
427.532Mbps must not replace whole-run404.691Mbps. Complete rawMbps:

```text
 0– 9:   2.620 125.142 454.078 570.959 348.831 475.958 291.433 546.832 413.447 264.409
10–19: 569.071 464.956 257.035 548.834 411.571 355.226 383.018 356.949 658.202 372.064
20–29: 477.198 423.927 304.324 439.908 556.786 369.977 394.333 368.150 370.480 543.693
30–39: 393.463 459.431 399.403 409.410 306.406 554.138 476.270 355.661 439.193 274.790
```

| Probe-clock band,s | Raw-bin mean,Mbps | Echoes starting in band | Echo p50/p95/max,ms |
|---|---:|---:|---:|
|0–5|300.326|10|248.883 /488.483 /488.483|
|5–15|424.355|20|350.952 /480.251 /715.761|
|15–25|432.760|19|380.844 /543.625 /745.859|
|25–40|407.653|30|366.748 /464.414 /466.002|
|30–40,overlapping late subset|406.817|20|406.401 /464.414 /466.002|

Quantiles use sorted successful attempts at round((n−1)×quantile), matching
the probe convention. Sampling windows/buffered ordered delivery can produce
one-second bursts above500Mbps without implying a larger physical cut.

All41 service rows verify500Mbps rate/ceil in both directions, UP70ms/DOWN30ms,
no configured loss/jitter, no UDP blackhole and zero actual class/netem drops.
The5s epoch changes retain this same profile. Traffic uses47, clienteth1 and
servereth0; the unused opposite interfaces each add42B. The three TCP plus
one QUIC attached carriers all address10.238.47.20:7443 despite `*-46` names.
This is one shared cut per direction, not four independent500Mbps paths.

Active first→last class samples add2,392,055,287B DOWN and34,149,490B UP.
Ratios to final body are1.18216 and0.01688; those mismatched observation
windows are not exact lifetime wire amplification. DOWN backlog peaks
28,798,828B/2,775 packets and UP160,425B/895 packets. These queue bytes can
overlap transport flight and must not be added to it as separate occupancy.

Final server native ACK counters total2,298,843,763B:1,040,398,993B TCP
(45.257%) plus1,258,444,770B QUIC. Four exact native epochs stay stable,
counters never regress, and every sampled live path remains active. These
are native delivery counters with framing/copies, not Original payload or
application receipt. Management path/physical-instance IDs below are not
response-copy incarnations and must not be borrowed from an earlier capture.

| Server native path/instance | Final ACKed B | SRTT final / sampled max,ms | Native flight final / sampled max,B |
|---|---:|---:|---:|
|TCP0/2|697880718|273.648 /515.697|7929248 /11504360|
|TCP1/3|155708911|263.780 /540.189|40544 /6617360|
|TCP2/4|186809364|270.693 /512.402|1077312 /4241192|
|QUIC0/1|1258444770|267.173 /517.387|10249668 /20093157|

Summed server native flight peaks31,487,469B and ends19,296,772B. Reported
native TCP unsent queues peak391,428B summed and end145,108B. Separate `ss`
snapshots show summed Send-Q peak12,722,640B/end9,692,374B; Send-Q includes
unacknowledged bytes and is not unsent. Final QUIC native window22,179,893B
versus25,239,627B sampled peak is not an independently attributed delay.
Client reverse native ACK counters end11,057,447B TCP plus436,554B QUIC,
also without epoch change/regression. Management/native sampling times differ;
these totals do not locate the critical ACK or copy's physical residence.

| One stable process PID per role | Lifetimeps CPU first / final / peak,% of one core | RSS first / final / peak,KiB |
|---|---:|---:|
|Client|2.7 /70.7 /71.3|34224 /85956 /85956|
|Server|3.3 /173 /173|31276 /276152 /284624|

Lifetime `ps` is not interval CPU, exclusive on-CPU cost or thread attribution.
No per-byte CPU or post-load reclamation conclusion is supported. Management
contains no errors and its final snapshot precedes full probe teardown. Bulk
client relay delivery there is2,022,392,888B and server target-read progress
2,086,411,644B; both echo management directions reach5,056B. Different body,
relay and snapshot endpoints cannot establish a final retained-byte leak.
Client Connection reset by peer and server RemoteClosed/H3_NO_ERROR warnings
occur at teardown; the complete probe has zero failed echo attempts.

The logs total75,500,435B: client19,437B/server75,480,998B. There are199,280
diagnostic records,80 client plus199,200 server, and three warning lines.
Server event counts are142,373 admission observations,27,413 applied-ACK
observations,13,267 recovery decisions and16,147 accepted-copy observations.
An admission observation includes its recorded disposition and must not be
counted as successful enqueue without classification. The capture omits
client-receive history intentionally; absence is not a negative receipt fact.
This sizeable observer cost prevents an ordinary speed/cost ranking from
these numbers. Exact causal conclusions belong to the following join.

Raw archive: [ORDERED_FEEDBACK_SERVER_ACK_ADMISSION_20260911.raw.tar.gz](ORDERED_FEEDBACK_SERVER_ACK_ADMISSION_20260911.raw.tar.gz).
Ten regular files contain both logs, probe.json/probe.err, service.jsonl,
closed driver,9058B patch, build log and existing run.py/shape.sh. Size is
4,488,384B compressed,77,234,621B uncompressed. Every member was compared
byte-for-byte to its source, without hashes. No binary/config/secret is
included. No runtime, controller, queue or timer change follows this report.

### Diagnostic80295: positive ACK admission before accepted-copy decisions

2026-09-11. Independent all-positive-range join of the preceding archived
capture; no additional experiment or source change. The sole observed session
is15085197147764974401, serverPID515449, bulkstream1; echo isstream0. Recovery
events without a session field are joined only within this same process/stream
and the observed single session. Exact target underlay/path/incarnation is part
of the join; management physical IDs are not substituted for copy identities.

All142,284 bulk ACK admissions succeed:135,112async and7,172try. The89 echo
admissions also succeed(87async/2try). There are no Full or Closed observations.
Thus the observer's explicitly unknown later PendingMailboxFrame publication
path is not exercised here. All143,123 bulk positive intervals parse as valid
nonempty ranges with matching declared counts; scoped ACK positives are
included, not just explicit[0,x) prefixes. Admission is raw mailbox evidence;
it does not itself perform ACK validation or establish actor application.

Match each accepted persistent-gap copy to the latest preceding decision with
the same start and exact target tuple, requiring its end to lie within the
decision's range. This conservatively identifies the first accepted fragment;
unjoined fragments are unknown, not attributed to a convenient older decision.
For each joined range, query the earliest actual successful ACK that wholly
covers it. Independently union every admitted positive interval by its captured
admission time, allowing different ACKs to cover different pieces. Both methods
identify exactly the same full-coverage set: sparse/combined coverage adds no
extra whole-copy cases beyond the independent prefix-only join.

| Bulk persistent-gap accepted-copy domain | Copies | Payload B |
|---|---:|---:|
|All accepted|15,141|190,290,388|
|Exact first-fragment decision join|13,237|176,346,843|
|Unjoined; timing classification unknown|1,904|13,943,545|
|Entire copy positively admitted before its decision|4,497|56,804,208|

These are accepted-copy payload sums, not unique source bytes or physical wire
bytes. An additional1,631 joined copy extents have only partial prior admitted
coverage:19,572,000B of their23,802,094B payload. That intersection is not
authority to remove the entire copy; its uncovered remainder still matters.
For all13,237 joins the decision's actual ack_frontier equals accepted.offset,
and no prior logged stored frontier enters the copied extent. Thus a covering
positive fact was not already applied at the decision: its first byte was still
the lowest unacknowledged byte. The logs do not export the entire historical
sparse Apply ledger, so this is not a claim about every interior byte.

Ordering uses the same-process monotonic instants, not millisecond log adjacency:
admitted_at is captured after successful publication, whereas decision_at is the
existing recovery observation. Strict admitted_at<decision_at therefore proves
publication before that decision even if the diagnostic lines interleave. The
reverse inequality would not prove the ACK absent: a producer can be preempted
between publication and its timestamp. One14,600B witness is logged after its
decision despite its captured admission being earlier, illustrating the limit
of log-sequence-only joins. All required instants parse in this capture.

Among the4,497 full-coverage cases, earliest covering admission leads the
decision by median210.571us,p95958.586us,max8,209.844us. Witness routes are
4,222async/53,264,586B and275try/3,539,622B. Every such extent later obtains a
logged validated contiguous positive cover. This establishes later receipt
truth, not one-to-one identity between a raw input ACK and a synthesized
ServerFeedbackBatch result. Exposure recurs throughout the run: each successive
five-second band from the first bulk server event contains6.01,8.00,6.60,7.22,
5.13,10.44,7.48,5.92MB respectively; these are event-clock bands, not probe bins.

Two exact examples use server diagnostic sequence numbers A/D/C/V for successful
admission, recovery decision, actual accepted copy and later positive cover:

| Copied DSN interval | A / D / C / V | Admission lead | Original owner -> copy target |
|---|---|---:|---|
|[3851812,3866412)|353 /354 /355 /358|99.361us|TCP2/inc2 -> TCP0/inc3|
|[1620793780,1620808380)|156697 /156741 /156743 /156752|8,209.844us|QUIC0/inc4 -> TCP0/inc3|

First example: ACK[0,3917348) finishes admission at local monotonic
1110097369414733ns; decision is1110097369514094ns with actual frontier3851812.
The accepted range/target match exactly; subsequent stored frontier3917348
covers it. Longest-lead example: ACK[0,1620817780) finishes admission at
1110128051611164ns; decision is1110128059821008ns, actual frontier1620793780.
Later stored frontier1621437398 covers the accepted14,600B. These prove real
available positive information, not a failed send or inferred native receipt.

The legal service boundary remains unresolved. Current handle.rs recv_frame
collects only events.len() at batch entry, emits all collected positives before
scopes/MAX, and retains ordered DATA/lifecycle/probe/error boundaries. Pending
synthesized frames do not reopen that collection. Server relay still performs
recovery before selecting Input and after each returned novel ACK; a later
admission can consequently precede a decision without belonging to the earlier
batch. Crucially, it can also arrive after a proposed finite Input quantum's
entry budget. The four events record neither that cutoff nor intervening
non-ACK barriers. Even the longest example is therefore not proof that a legal
011b724-style server quantum could consume that witness before its decision.
Same-batch positive-first ordering is not contradicted by this capture.

Disposition: real mailbox-ready advisory work, not a demonstrated mandatory
ACK-authority violation or a measured safely catchable batch. The56.8MB is
29.85% of all persistent-copy payload here, nominally11.36Mbps over40s/about
2.81% of404.691Mbps useful service. That is a work scale, not a predicted
goodput/latency gain; unjoined decisions, partial copies, publication races and
shared native competition remain. No winning echo interval is joined to this
work, and the capture intentionally lacks client receipt history. With native
competition independently reproducing hundred-millisecond latency, this
small/uncertain removable portion does not select a server Input rewrite,
copy ban, merge/cadence change or new observer. Preserve it for a later scoped
decision; proceed with the already-declared matched baseline context instead.
