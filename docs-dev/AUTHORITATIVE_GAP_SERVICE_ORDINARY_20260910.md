# Authoritative-gap request service: ordinary trial failed settlement

Recorded: 2026-09-10. **Rejected for performance promotion.** The request-local
candidate passes95focused checks, but its first ordinary comparison fails the
existing85s settlement guard. It confirms368,664,530of442,040,320locally accepted
bytes, leaving73,375,790B unconfirmed. The maximum confirmation gap is26.665989s.
This is not a completed34.503Mbps transfer or an accepted optimization.

Sampled target-write service improves during the restricted phase, but is
already materially worse **before** the QoS change, degrades after restoration,
and stops advancing for the final25sampled seconds. The failure is not merely
the original cut-phase shortfall moved to a harmless settlement tail.

## Contract, source and fixed comparators

The [model and exact prior attribution](INDEPENDENT_QOS_RECOVERY_MODEL_20260910.md)
motivate selecting one independently due authoritative omission after excluding
queued and unexpired-copy coverage. Coverage is service, not receipt. Every
selected extent retains its own Original clocks and fresh rank/Apply bound;
silent retained fallback, structural recovery, native policy, existing quantum
and resource limits remain unchanged. The old first-head policy refused a
real, otherwise serviceable successor before the first copy's DATA ACK.

The predeclared forecast was material recovery of useful healthy-link service,
not instantaneous210Mbps or a guaranteed speed gain. Increased speculative
work, range scans, memory and shared service costs were explicit falsifiers.
The actual evaluator RED preceded implementation. Final focused log
`./.tmp/reflection/authoritative-gap-service-focused-r3-0910.log` records
95passes, including the real successor and migrated T06 rank/Apply tests;
do not add the separately run successor check to claim96unique passes.
The ordinary optimized build takes81s with one existing dead-code warning.

The measured candidate is the then-uncommitted request-service change on
working `b0baca2`, frozen at
`./.tmp/reflection/bin/authoritative-gap-service-request-20260910/mptunnel`.
No feature observer runs with this traffic. Root preserves the candidate
checkpoint for exact diagnosis; it is not promoted or silently substituted
for a release. This report owns no runtime changes.

The fixed ordinary comparators are the previous aggregate QoS cell and its
one-link47negative control, not the slower diagnostic observer:

- `aggregate-combined-up-native-refill-independent-qos-0910`;
- `mixed-combined-up-native-refill-independent-qos-0910`;
- new `aggregate-combined-up-authoritative-gap-service-qos-0910`.

All are under `./.tmp/reflection/results/`. Existing comparators, including
all86complete confirmation bins, remain in the
[independent-link report/archive](NATIVE_REFILL_INDEPENDENT_20260910.md).
The [candidate archive](AUTHORITATIVE_GAP_SERVICE_ORDINARY_20260910.raw.tar.gz)
has15regular files plus one directory entry: five result files, eight
RED/GREEN/focused/build/driver logs, `run.py` and `shape.sh`. Root created and
listed it; the file count was independently checked. Candidate source identity
is the preserved worktree/checkpoint, not a source patch embedded in this archive.

## Identical physical profile; failed logical completion

The two direct independent links each have200Mbps per direction. Client
eth0/eth1map to46/47; server eth1/eth0map to46/47. Each configured link has
TCP and QUIC carriers sharing its physical cut, not one separate cut per carrier.
Only46UPchanges200→10→200Mbps during nominal15–25s;47and both return cuts stay
200Mbps. DOWN/UPpropagation is30/70ms, with no configured loss, jitter or UDP
outage. All86candidate service rows verify rate=ceil, burst/cburst65536B,
netem limit8192, intended delays and zero class/netem drop deltas. First rows
reporting restriction/restoration occur15.003066/25.004169s.

| Outcome | Healthy47 control | Aggregate `b0` | Request candidate |
|---|---:|---:|---:|
| Locally accepted bytes | 975,437,824 | 1,294,925,824 | 442,040,320 |
| Target-confirmed bytes | 975,437,824 | 1,294,925,824 | 368,664,530 |
| Complete streams | 1/1 | 1/1 | 0/1 |
| Elapsed, s | 43.867147 | 41.538802 | 85.480357, censored |
| Completed whole Mbps | 177.889 | 249.391 | Not available |
| First local write / confirmation, s | .105625 /.409011 | .105451 /.409741 | .105411 /.410068 |
| Maximum confirmation gap, s | .828446 | .634001 | 26.665989 |
| Maximum local-write gap, s | .519904 | .545897 | 3.444041 |
| Valid exact terminal accounting | Yes | Yes | No |

Candidate JSON reports`status=loss`, `complete=false`, one failed stream,
`upload_ack_accounting_valid=false`, `upload_accounting_exact=false` and
`upload_accounting_lower_bound=true`. Its34.503Mbps is the reported censored
confirmed-byte lower bound, not completed goodput; local41.370Mbps is not
delivery either. `probe.json.exit_code=0` does not mean successful settlement.
The outer driver exits1at its unchanged `duration+45s` guard; cleanup then
terminates the products and the probe records `ConnectionResetError`.
Do not classify that cleanup reset as an independently observed native failure.

Both candidate product logs and probe stderr are empty. No exact confirmation
bin history is available: both raw and trimmed arrays are empty after invalid
terminal accounting. No bins are fabricated from local writes, native ACKs,
management rates or traffic classes. The maximum-gap endpoints are also absent.
The full86sampled management/socket/class rows remain in the raw archive.

## Where service fails: improvement at the cut does not clear earlier/later harm

The following is **target-socket acceptance**, not reconstructed sink-confirmed
throughput. The server wraps its outbound target in `ObservedProductIo`;
`traffic.total.from_peer_bytes` increments only on successful target
`poll_write`/`poll_write_vectored` (`runtime/telemetry.rs`). Server `to_peer`
instead counts reads of the sink's small confirmation stream. These are separate
directions and stages; socket acceptance need not mean target application receipt.

Phase rates use cumulative counter differences divided by actual runner sample
elapsed differences. Management producers have separate timestamps and role
reads are sequential. The values are comparable coarse windows, not exact
application-bin or per-byte timing joins.

| Sample window | Aggregate `b0` target-write Mbps | Candidate target-write Mbps |
|---|---:|---:|
| 0–5s | 273.573 | 69.850 |
| 5–15s, before QoS | 321.800 | 67.617 |
| 15–25s | 15.642 | 69.308 |
| Strict interior16–24s | 17.028 | 72.555 |
| 25–40s, restored | 340.910 | 110.545 |
| 40–60s | Already settled | .681 |
| 60–85s | Already settled | 0 |

Candidate target acceptance is already flat at43,661,258B over samples3–5,
and126,171,082B over12–14, before the restriction. It reaches201,957,351B at24s
and422,144,178B at40s, then adds only1,703,608B through60s. From60through85s,
the exact total remains423,847,786B while management generation timestamps
advance from1789033030647to1789033055646ms. This differs fundamentally from
the earlier steadily draining TCP stress upload that merely reached a guard.

The reverse confirmation path also has unresolved delivery. At60s the server
has read1998confirmation bytes, while the client has written1438to its local
socket; at85s those counts are1998and1466. Final server target acceptance exceeds
the probe's final confirmed byte count by55,183,256B. These observations expose
multiple separated progress domains, not the exact blocking frame, location of
queued confirmations or proof that the sink itself stopped reading.

## Native/link utilization and cost

| UP class service | Aggregate `b0`46 /47 | Candidate46 /47 |
|---|---:|---:|
| 0–15s, Mbps | 185.977 /187.386 | 86.899 /78.957 |
| Strict16–24s, Mbps | 9.991 /31.016 | 9.997 /165.060 |
| 25–40s, Mbps | 185.632 /193.366 | 102.378 /95.117 |
| 60–85s, bytes | Already settled | 14,467,931 /12,756,302 |
| Whole sampled UP bytes | 745,256,344 /801,341,232 | 428,663,871 /549,475,737 |
| Whole sampled DOWN return bytes | 15,482,120 /15,889,276 | 7,493,589 /8,344,598 |

The healthy47wire increase during the cut is real; it does not translate into
equivalent ordered target progress. Whole traffic windows are41.011453and
85.010732s and complete different amounts of work, so lower total candidate
wire volume is not improved efficiency. These byte domains include native,
protocol and copy work; ordinary counters do not identify unique repair winners
or establish that additional TCP traffic is necessarily useless duplication.

During16–24s, candidate client TCP native ACKs advance4,111,293/109,643,022B
on46/47and QUIC ACKs2,452,303/41,043,374B. During the final target-flat60–85s,
TCP ACKs still add13,573,600/12,249,496B and QUIC ACKs167,643/190,150B, with
stable exact identities/epochs. The QUIC producer timestamps continue advancing
and RTTs return to approximately100ms; at85s each QUIC native flight is0.
Meanwhile their reported Product flight remains11,318,592/43,696,680B.
Eight paths remain sampled active from5s onward, with no suspect/failed state.
Thus the final interval is not explained by all native carriers being frozen
or the configured cut remaining at10Mbps. It does not yet choose synchronous
actor/scan/lock delay, retained ownership, missing-prefix service or another
local stage as the exact cause.

| Sampled cost | Aggregate `b0` | Candidate |
|---|---:|---:|
| Client RSS peak / final, KiB | 391,104 /385,112 | 370,772 /344,060 |
| Server RSS peak / final, KiB | 125,516 /125,516 | 110,296 /110,296 |
| Client lifetime CPU peak / final, % | 188 /162 | 125 /111 |
| Server lifetime CPU peak / final, % | 90.4 /77.8 | 42.8 /18.0 |
| UP backlog p50 / max over rows0–40, B | 5,697,518 /15,059,114 | 4,052,450 /63,217,268 |
| DOWN backlog p50 / max over rows0–40, B | 21,614 /53,045 | 3,004 /82,381 |

Backlogs sum the two interfaces within each sample. The candidate UP peak is
at27.004367s:38,332,288B on46plus24,884,980B on47, after restoration. Final
candidate UP backlog is199,726B, so the large earlier peak alone cannot explain
the last25seconds of no target progress. Lifetime CPU and sampled RSS are not
critical-stage CPU measurements or post-teardown leak evidence; substantially
less completed work and a much longer observation limit comparisons.

## Disposition and bounded next information

The new service rule's focused reachability and ownership checks are meaningful,
but the practical forecast fails. More cut-phase wire and target service coexist
with pre-cut degradation, a large restored queue, prolonged confirmation gaps
and incomplete settlement. Do not proceed to healthy/other acceptance gates as
if only a small tuning issue remained; no timer, quantum, guard or reserve change
is justified by this result.

Root selects one feature-only timing/count observation of the existing
evaluator/model/clock regions to distinguish synchronous service/lock cost from
other stages, without changing decisions. That capture has not run at report
creation. Preserve this ordinary failure independently; a later observer cannot
retroactively complete its missing bytes or replace its empty confirmation bins.
No public README update, release or performance acceptance follows this trial.

## Separate diagnostic: expensive repeated owner work during confirmation starvation

The subsequent `aggregate-combined-up-authoritative-gap-service-profile-0910`
capture is information-only, not a replacement ordinary comparison. Its one-file
feature overlay adds three aggregate counters around the existing request
evaluator/model/assignment-clock regions, without changing decisions. The exact
patch is `./.tmp/reflection/authoritative-gap-service-profile-0910.patch`;
the optimized feature build takes86s with the same existing dead-code warning.
Root freezes `./.tmp/reflection/bin/authoritative-gap-service-profile-20260910/`
and reverses the overlay before traffic. No per-query or per-call samples run.

This capture **settles eventually but reproduces severe confirmation stalls**:
427,098,112B accepted=confirmed in83.485320s, valid exact accounting,1/1complete,
no probe errors. Whole goodput is40.927Mbps, first write/confirmation
.106131/.417416s, maximum confirmation gap19.443960s and local-write gap2.793370s.
The driver exits0after84.009204s. These outcomes do not clear the ordinary
incomplete trial or establish improvement. There is no concurrent echo workload
in this upload probe. The exact maximum-gap endpoints are not recorded.

All84raw one-second confirmation bins are preserved below, including58zeros.
Rows start at the printed zero-based second; the last bin is partial and not
renormalized. The trimmed array discards three bins at either end and must not
be used as a wall-clock timeline.

```text
0:  5.107,199.460,27.027,0,0,0,0,0,0,4.669
10: 58.341,86.936,400.696,96.233,182.836,163.767,116.107,35.556,16.445,0
20: 0,23.113,391.882,28.358,213.588,26.162,321.742,95.134,232.760,0
30: 0,0,0,0,0,0,0,0,0,0
40: 0,0,0,0,0,0,0,0,49.669,0
50: 0,0,0,0,0,0,0,0,0,0
60: 0,0,0,0,0,0,88.080,0,0,0
70: 0,0,0,0,0,0,0,0,0,120.180
80: 0,0,265.998,166.938
```

Raw-bin means over5–15/15–25/25–40s are82.971/98.882/45.053Mbps;
40–79s averages3.532Mbps with37of39bins zero. Actual target-socket acceptance
over sampled5→15/16→24/25→40s is44.901/79.933/83.296Mbps. These are different
measurement stages, not inconsistent versions of one throughput counter.

### Timing/count attribution and its limits

Only the client emits the three new labels, under one PID. All72rows per label
reconcile interval count/query/time deltas with the cumulative totals. The
generic `bytes` fields are **query counts, not traffic**. Each counter's count
is one completed outer evaluation, including evaluations with no queries;
model/clock durations sum their respective queries within that evaluation.

| Client region | Completed evaluations | Evaluated queries | Aggregate elapsed, s | Mean per evaluation, ms | Maximum per evaluation, ms |
|---|---:|---:|---:|---:|---:|
| `request.gap_service` | 30,766 | 7,776,602 model queries | 55.691934 | 1.810178 | 20.847 |
| `request.gap_service.owner_model` | 30,766 | 7,776,602 | 53.013520 | 1.723120 | 20.487 |
| `request.gap_service.assignment_clock` | 30,766 | 466,829 | .911119 | .029614 | 2.437 |

The nested model region accounts for95.19%of outer elapsed time. **Do not add
these three times**, or interpret the model maximum as one individual query's
time. Each recorder floors elapsed to1us and synchronous timing includes
instrumentation and possible descheduling, not CPU instructions alone. Source
places the evaluated region inside the request Product lock, without an await.
It therefore consumes elapsed time while that owner is held; this is not a
measurement of another task's lock wait or a19-second uninterrupted lock hold.
The outer timer also excludes some surrounding actor and recorder work.

For phase attribution, compare actual `ts_unix_ms` cumulative snapshots. The
printed `interval_ms=1000` is the configured flush interval, not measured
spacing; inactive components can be absent. Process monotonic origins and
probe elapsed time are not interchangeable. The following flush pairs lie
strictly within the indicated management plateau windows. Wall timestamps are
given as milliseconds after1789033600000; outer sequence IDs identify endpoints.

| Management window | Actual flush bounds / outer seq | Wall s | Calls | Model query delta | Outer / model / clock elapsed, s |
|---|---|---:|---:|---:|---:|
| 40–48s | 75510→82533 /561→635 | 7.023 | 767 | 830,023 | 5.637801 /5.414779 /.087715 |
| 49–65s | 84551→99635 /659→821 | 15.084 | 1,739 | 1,849,977 | 12.730043 /12.297976 /.127511 |
| 67–78s | 102646→112677 /859→975 | 10.031 | 1,456 | 1,440,210 | 8.773010 /8.506989 /.027162 |
| Combined40–78s | 75510→112677 /561→975 | 37.167 | 4,561 | 4,752,156 | 31.400284 /30.334409 /.283590 |

The last row overlaps the preceding rows; it is not additional work. Outer
elapsed occupies84.48%of that37.167s span, with at most19.387ms per call and
about1042model queries per evaluation. Outer query deltas differ slightly from
the nested model deltas at some boundaries: the three sequential recorder calls
can straddle a flush. Final counts/query totals agree; do not manufacture a
conservation defect or an exact per-call timeline from those split snapshots.

The late plateau has a decisive direction distinction. At management samples
40through78, target acceptance is exactly414,045,459B. Client source reads equal
that total throughout40–77, then add12,000B at78. Thus this long interval is not
merely an outstanding upload suffix slowly draining into the target. The server
has already read1581bytes of sink confirmations, while the client has delivered
only1301at40,1315at49and1329at67. Those client totals remain flat over40–48,
49–66and67–78respectively. The source/target counters then resume, and all bytes
eventually settle. The raw confirmation history preserves the long holds.

This establishes substantial repeated synchronous model work during the same
capture's withheld-confirmation/new-source episodes. It falsifies an inexpensive
gap-evaluator assumption and supports correcting repeated ownership-query work
before another performance attempt. It does **not** prove which exact reply
range is blocked, how much of its residence is waiting on this lock, or that
eliminating the measured region will eliminate every stall. No per-reply trace,
CPU sampler or lock-acquisition timer is present; other actor/native service
remains a competing contributor. The1.810ms whole mean must not hide the
repeated late work, and the55.692s cumulative time is not a predicted speed gain.

### Physical/native context and collection cost

All84service rows verify the same direct two-link200Mbps profile: only46UPis
10Mbps at samples15–24;47and return directions remain200Mbps. First reporting
restriction/restoration samples are15.001599/25.002671s. Delay30/70ms,
jitter/loss0, burst/cburst65536, netem limit8192 and no UDP blackout are unchanged;
all class/netem drop deltas are0. Physical class accounting spans83.008907s,
not the exact probe or counter-flush window.

| Whole sampled cost | Client UP46 /47 | Server DOWN46 /47 |
|---|---:|---:|
| Class bytes | 287,521,532 /655,949,566 | 5,425,342 /7,378,856 |
| Summed backlog p50 / maximum / final, B | 137,276 /37,339,491 /2,538,529 | 396 /47,387 /7,296 |
| Process RSS peak / final, KiB | 328,268 /299,500 | 90,388 /90,388 |
| Process lifetime CPU peak / final, % | 127 /111 | 58.4 /16.8 |

RSS/CPU describe whole-role snapshots, not per-region CPU or a teardown leak.
There are eight active native paths from sample10onward; all observed path
states are active. Exact native identities/epochs remain stable over40→78,
and producer timestamps advance. TCP ACK bytes add264,520on46and22,075,374on47;
QUIC adds255,652and555. UP classes carry564,603/23,299,135B in that window.
These are not unique useful payload or repair-winner counts. At78both QUIC
native flights are0, while Product flights remain28,593,050/22,410,870B.
Continuing native service does not equate to delivered confirmations.

Logs total870,220B: client459,756B/1087lines, server410,464B/975lines, no
per-call perf samples and empty probe stderr. The two server warnings are
`ApplicationClose: H3_NO_ERROR` at09:48:39.444UTC, after exact settlement and
the stream-close flush; they are not independent failed transfers. The observer
is much smaller than the earlier per-event capture, but still changes execution
cost and is not an ordinary speed comparison. No performance promotion follows.

The [diagnostic archive](AUTHORITATIVE_GAP_SERVICE_PROFILE_20260910.raw.tar.gz)
contains11 regular files plus one directory entry: five result files, build/
driver logs, exact temporary overlay, perf wrapper, driver and shaper. Root
created and listed it before further runtime changes. The ordinary failure's
separate archive and original accounting remain untouched.
