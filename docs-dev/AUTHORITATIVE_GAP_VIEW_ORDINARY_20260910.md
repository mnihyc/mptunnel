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
