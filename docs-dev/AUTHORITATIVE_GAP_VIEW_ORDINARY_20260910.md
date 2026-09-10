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
