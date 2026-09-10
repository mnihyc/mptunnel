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
