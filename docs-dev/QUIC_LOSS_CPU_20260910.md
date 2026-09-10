# Fixed QUIC-loss CPU discriminator: late collapse is not sustained CPU saturation

Recorded2026-09-10. **Diagnostic only; no performance acceptance or deployed
incident attribution.** At20%configured DOWN loss, both tested MPP modes lose
sustained useful service. Their late process CPU is low, not continuously
saturated. The loss runs nevertheless have server startup intervals around
101%/138%of one core; a user reporting total-process “one core” must not be
dismissed because no individual sampled thread reaches100%.

## Question, fixed intervention and evidence

The reported random CPU incident lacks confirmed role/version/platform here.
The information forecast is to distinguish loss-associated sustained CPU
amplification from lower useful service without high process execution, and
mixed-specific context from QUIC-only behavior. This observation does not name
the native/controller, recovery, crypto or wake-loop cause. It is separate from
the no-loss independent-link request-pilot service failure.

All four runs use the same frozen ordinary executable
`./.tmp/reflection/bin/recovery-target-observation-20260910/mptunnel`, runtime
source subsequently checkpointed as `d44ca8e`. Its existing ordinary build is
5669,3m32s; no feature observer, native tracing or role-specific binary override
is used. The runtime work correction remains unaccepted.

Execution order is QUIC0,QUIC20,mixed0,mixed20. The temporary `loss20_cpu.py`
imports unchanged `run.py`, fixes logical DOWN loss to0or20%and UP loss to0,
and adds one tagged `/proc` observation to each existing tc/ps command. No
second polling loop or runtime policy is added. All cases are40s DOWN bulk
plus64B TCP echo every500ms,3s echo timeout,500Mbps both directions,
70msDOWN/30msUPdelay, zero jitter/QoS/blackhole and no mirror/router. This is
one used physical link47, not aggregation: endpoint addresses and actual class
traffic establish it despite historical configured labels ending`-46`.
Mixed uses three TCP carriers plus one QUIC on that shared cut; configured
loss affects TCP too, not only QUIC. Native/allocation histories and random
realizations differ; these are not packet-identical or equal-work controls.

Result directories under `./.tmp/reflection/results/` are
`{quic,mixed}-combined-down-loss-cpu-{zero,twenty}-{quic,mixed}-0910`, with matching
system names. All four runner return codes are0, elapsed41.004641,41.004346,
41.006792and41.005822s respectively. Runner success is not all-probe success.

[Verified raw evidence](QUIC_LOSS_CPU_20260910.raw.tar.gz) contains27regular
files: five results per case, four driver logs, the exact CPU wrapper, runner
and shaping script. Size633,721B; gzip integrity, tar comparison and every
decompressed member's byte comparison pass. No configs, private credentials,
binaries, links or directory members are included. Runner/shape bytes equal
the preceding ordinary-reuse archive. All311echo attempt records and full
native/queue/CPU histories remain in these raw files.

## User service and full timing history

Q0/Q20 mean QUIC-only with0/20%loss; M0/M20 mean mixed with0/20%loss.
Each bulk request receivesHTTP200 and is intentionally duration-partial, not
an8GiB completed download. The loss cases' final reads extend slightly beyond
40s; whole goodput uses actual elapsed, not40s or the trimmed-bin mean.

| Outcome | Q0 | Q20 | M0 | M20 |
|---|---:|---:|---:|---:|
| Body bytes | 2,152,792,229 | 242,706,592 | 1,927,915,484 | 307,686,102 |
| Body elapsed, s | 40.000084 | 40.232674 | 40.000245 | 40.306659 |
| Whole Mbps | 430.558 | 48.261 | 385.581 | 61.069 |
| First body, s | .490238 | .738950 | .587005 | 1.158503 |
| Max read gap, s | .100763 | .456200 | .330792 | .968167 |
| Gap interval, s | .691459–.792222 | 7.008827–7.465027 | 38.253641–38.584433 | 38.477165–39.445331 |
| Echo success / failed records | 80/0 | 38/34 | 80/0 | 79/0 |
| Successful echo p50/p95/max, ms | 105.874/172.576/291.744 | 215.328/708.349/1420.055 | 232.009/332.105/755.283 | 273.792/519.371/889.592 |
| Max spacing between successful echoes, s | .674887 | 1.630925 | 1.038981 | 1.187188 |
| Overall probe status | ok | loss | ok | ok |

Q20has one actual timed-out echo attempt20.979419→23.982181s, followed by
33`unavailable_after_disconnect` records, not34independent network failures.
Its last success is20.479326→20.581195s; subsequent echo service is censored
by the disconnected test socket. Successful-only p95 excludes that timeout
and cannot represent restored/late echo service. Slowest successful Q20echo
is12.758238→14.178293s; M20is33.953463→34.843055s. The other two cases'
slowest successes are2.002508→2.294253s(Q0) and10.502638→11.257921s(M0).
Quantiles reproduce the probe's sorted index`round((n−1)*p)`.

| Raw body phase, Mbps | Q0 | Q20 | M0 | M20 |
|---|---:|---:|---:|---:|
| 0–5s | 340.382 | 184.956 | 285.847 | 153.781 |
| 5–15s | 447.146 | 81.084 | 422.622 | 151.692 |
| 15–25s | 443.283 | 15.961 | 404.526 | 13.431 |
| 25–40s | 441.069 | 3.073 | 381.501 | 2.749 |
| 30–40s | 441.397 | 1.906 | 371.853 | 3.021 |

These are sustained-loss phases, not QoS/outage transitions. Late30–40s echo
attempts are20successes(Q0),20unavailable(Q20),20successes(M0),19successes(M20).
Available late p95/max are135.450/145.140, no measured successfulQ20value,
331.867/332.105and454.504/889.592ms. Mixed keeps this sparse echo socket alive
while its bulk service collapses; `status=ok` does not make it competitive.

All160raw body bins follow. Each row starts at its zero-based second; no
trimmed series is substituted and the one M20startup zero is preserved.

```text
Q0
 0: 7.177,351.835,461.978,434.923,445.995,471.545,446.854,440.408,453.957,433.451
10: 436.93,445.128,452.33,440.345,450.513,378.452,436.748,458.093,457.659,435.614
20: 448.839,464.416,453.63,436.902,462.477,385.04,449.155,452.961,465.523,449.394
30: 441.755,461.412,451.962,442.334,389.056,442.717,437.956,466.414,437.021,443.342
Q20
 0: 1.145,43.539,451.744,260.002,168.349,186.786,155.573,98.611,91.234,82.271
10: 44.267,48.803,26.694,39.698,36.899,25.402,34.673,21.754,13.682,15.261
20: 15.391,9.29,7.148,11.099,5.914,7.148,7.115,4.29,2.426,6.055
30: 2.717,2.717,2.089,2.097,1.758,1.049,2.339,1.427,1.381,1.482
M0
 0: 2.62,142.487,358.613,585.489,340.027,509.195,331.199,516.847,372.597,438.369
10: 350.245,508.419,221.639,599.121,378.589,451.223,405.912,314.263,456.789,421.004
20: 362.623,472.912,417.056,379.915,363.561,429.258,464.021,462.389,347.782,300.534
30: 323.271,365.172,394.58,353.371,379.774,302.591,343.998,468.92,418.398,368.456
M20
 0: 0,5.86,58.269,259.759,445.017,334.555,127.102,167.665,358.056,59.348
10: 215.468,78.748,91.65,25.402,58.922,28.451,16.772,26.663,6.328,29.249
20: 9.437,3.146,8.209,3.534,2.525,3.242,1.241,3.377,1.545,1.622
30: 1.788,6.146,5.339,.857,4.194,10.486,1.049,.117,.117,.117
```

## Actual interval CPU, not process-lifetime ps percentages

All328tagged observations contain exactly one process, stable boot/PID/process
start-time identity and no collector error. `CLK_TCK=100`. Match task identities
by TID+starttime within that process; initial6/final5threads mean fixed thread
membership must not be assumed. For each stat, its timestamp is the midpoint
of its own monotonic read brackets. Adjacent CPU is
`100 × Δ(utime+stime) / (CLK_TCK × Δseconds)`; whole/late means use total CPU
seconds divided by actual total measured seconds. Waited-child ticks are not
included. Threads are the same work, not added to the process total.

| Case/role | CPU seconds / observed seconds | Whole mean / max, % | Sample30→40 mean / max, % | Largest single-thread interval, % |
|---|---:|---:|---:|---:|
| Q0client | 38.80 /39.821856 | 97.434 /111.095 | 98.343 /111.095 | 30.348 |
| Q0server | 64.19 /39.864091 | 161.022 /180.328 | 166.241 /175.235 | 49.074 |
| Q20client | 2.63 /39.860086 | 6.598 /35.941 | 1.732 /3.636 | 12.628 |
| Q20server | 6.15 /39.867172 | 15.426 /101.198 | 3.867 /7.273 | 31.061 |
| M0client | 34.49 /39.878298 | 86.488 /125.752 | 93.753 /103.721 | 34.864 |
| M0server | 81.80 /39.883848 | 205.096 /235.222 | 218.469 /235.222 | 61.073 |
| M20client | 5.48 /39.819216 | 13.762 /72.388 | 8.347 /50.029 | 41.007 |
| M20server | 11.41 /39.832866 | 28.645 /138.050 | 7.106 /8.985 | 35.982 |

The largest thread column is a maximum one-interval sample, not a thread's
whole-run average or Rust async-task identity. Disappearing short-lived threads
limit thread-level coverage; process ticks remain the aggregate measurement.
Sample30→40is approximately those runner seconds, not exact probe-clock CPU
attribution. The largest individual stat read bracket is2.731398ms and ticks
have10ms granularity. No inference about subsecond transient peaks follows.

Q20process peaks occur around runner2–3s, M20around4–5s. These are also large
work intervals: body bins2/4are451.744/445.017Mbps. Sampled server native ACK
progress adds51,627,245B(Q20) and62,976,425B(M20TCP+QUIC) in those respective
bands. Different counter/timestamp domains make this contextual coexistence,
not per-byte CPU attribution or proof that all startup cost is necessary.

Thus the high-loss captures DO include approximately one-core or greater
total-process startup work. But late collapse to1.906/3.021Mbps occurs with
mean server CPU3.867/7.106%and client1.732/8.347%, not sustained MPP CPU
exhaustion. That falsifies continuously CPU-saturated MPP as the explanation
of this current late collapse; it does not identify its native/MPP cause or
disprove another deployed burst, external scheduling delay or shorter event.

Collector begin→end maxima are8.624ms across all roles/cases, with per-role
41-invocation totals131–163ms. This excludes Python startup, Docker execution,
tc/ps and other collection impact; it is not total observer overhead. MPP CPU
does not include the collector process. No runtime owner timers or samples run.

## Physical drops, resource cost and limits

All41rows per case verify500Mbps rate=ceil,65536Bbursts,8192netem limit,
70/30ms delays and zero jitter; only configured DOWN random loss differs.
There is no QoS or blackhole. Active47is servereth0/clienteth1; the unused
interface is not counted as another payload link. Costs below use sampled
class deltas and include protocol/repair/native work, not just useful body.

| Sampled cost | Q0 | Q20 | M0 | M20 |
|---|---:|---:|---:|---:|
| Window, s | 40.004445 | 40.004130 | 40.006586 | 40.005498 |
| Active DOWN bytes | 2,275,450,434 | 259,009,796 | 2,384,844,645 | 384,954,872 |
| Active UP bytes | 36,897,612 | 4,969,832 | 44,299,291 | 14,149,791 |
| Active DOWN class drops | 0 | 11,045 | 0 | 14,960 |
| Summed DOWN backlog peak, B | 15,479,334 | 9,261,594 | 25,494,664 | 13,246,957 |
| Summed UP backlog peak, B | 39,861 | 40,795 | 55,663 | 99,396 |
| Client RSS peak / final, KiB | 36,684 /36,684 | 81,936 /78,808 | 76,612 /76,612 | 117,908 /109,712 |
| Server RSS peak / final, KiB | 313,508 /301,744 | 255,396 /255,396 | 370,668 /369,348 | 235,108 /235,108 |
| Active DOWN30→40 class Mbps | 458.913 | 1.956 | 478.652 | 5.134 |

UPdrop deltas are0throughout. The inactive server interface adds2drops inM20
(348B/6dequeued packets), none in the other cases; active/inactive counts
must not be conflated. Native packet/offload and class-dequeue accounting
domains differ: configured20%loss is NOT computed as class drops divided by
dequeued packet counters. Larger client RSS with far less useful work is
retained, but these finite in-load samples cannot establish a leak or prove
post-load reclamation. Lower CPU/wire volume with collapsing work is not an
efficiency win.

Probe stderr is empty. Client broken-pipe/reset and server remote-close/
graceful QUIC-close warnings occur near duration-stop/cleanup, not at the
Q20echo timeout around21–24s. They are retained in raw logs, not counted as
additional independently observed mid-run failures. This experiment supplies
no matched raw/Hysteria2/Xray loss comparator and no native-policy ablation;
physical inevitability, a deliberate controller tradeoff and a Product defect
cannot be selected from these four means alone.

### Existing native policy boundary, not permission to retune it

The [existing RFC](../RFC.md) uses preferred loss allowance`p0=.10` and native
residual objective`q=.02`, giving`theta=1−(1−p0)(1−q)=.118`. At constant
volume, its documented envelope can authorize another native response after
approximately four operating rounds at sustained20%loss. Thus20%is outside
the intended10%allowance; this observation does not silently enlarge it.

Q20server native ACK bytes advance every sampled second in one unchanged
epoch. Its exported native inflight limit falls17,237,434B at sample2to854,652
at10,58,667at30and20,000at40; native pacing falls1467.013→67.401→4.640→1.772Mbps.
Displayed RTT is approximately100ms from sample10onward. Mixed QUIC ends at
7,331B and.527Mbps. This directly supports native contraction as part of the
observed declining service, not a CPU-frozen controller. Management does not
record envelope balance, exact phase transitions or raw-authority decisions,
so it cannot attribute every contraction to one branch or prove near-zero
goodput is inevitable or correctly calibrated. No allowance/controller knob,
policy change or fix follows from this four-cell observation alone.

**Disposition:** the information forecast separates current late collapse
from sustained process CPU saturation. It does not resolve the deployed
RAM/CPU report, justify a native or MPP policy change, or establish acceptable
performance under loss. Any next correction needs the exact responsible
state/decision and its own falsifier, not a throughput-only or CPU-threshold
adjustment.

### Current-callsite audit, 2026-09-11

The old all-transactions-empty collection rule introduced by61c2059 is not
present in the active controller path. Its concrete rolling-overlap test
retained8192records with only three live transactions.6636091 replaced it
with finalized-prefix folding/resource authority;42d1b86 delivered terminal
proof to both active and parked rollback owners;7677fd9 made exact pending
packet proof, rather than transaction lifetime, the classification owner.

Current loss insertion, round advance, ACK/expiry batch terminals and episode/
CE transitions reach the prefix compactor. It uses the earliest open, current
batch or late-ACK-pending record, folds finalized chronological prefixes and
reclaims unreferenced consumed storage. The MPP wrapper forwards native packet
terminal callbacks, and Cargo selects the local corrected Quinn fork.20%loss
does not select a legacy collector branch. Checked journal exhaustion falls
back to RawOnly, not repeated growth beyond that journal authority.

This excludes the known bypass, not a new CPU incident. A legitimately mutable
suffix can still require proportional scanning/replay and deep snapshot clones;
the64MiB default bounds journal allocation, not callback CPU or total RSS. No
current stack/interval capture links the user's random burst to those costs.
The audit changed no code and claims no new runtime or performance verification.

### Snapshot work and the remaining measurement boundary, 2026-09-11

Read-only audit of current011b724; the four ordinary CPU captures above remain
d44ca8e evidence. No new runtime fix, test, diagnostic or benchmark was run.
Commit3a6d0ea introduced coherent active-controller/PathData snapshots so an
activation, controller lineage, RTT and flight could not be fused across a
migration or rollback. That useful coherence/fencing requirement remains;
it is not a requirement to copy the controller's mutable history.

The exact snapshot entry is `src/transport/quic/endpoint.rs:241`, not Quinn's
endpoint/rebind code. Quinn's connection-state lock protects the controller
clone; `InstrumentedController::clone_box` delegates to derived `Bbr3::clone`,
including retained journal records, epoch-ID vectors and packet metadata.

| Actual caller | Snapshot work |
|---|---|
| Authority construction / initial server shape | One deep clone per capture attempt |
| Successful per-carrier `UdpPathConnection::tx_metrics` | Two: scheduling shape, then congestion metrics |
| Coalesced native-authority notification → `refresh` | One per capture attempt |
| Final native precommit / ordinary native write | No clone through these APIs |

Final `commit_with_current_scheduling_shape` reads the cached scalar shape under
activation-fence → coordinator → shape locks and must not call Quinn. Thus this
is not a per-write/precommit journal-copy claim. Client/server metric tasks use
`max(SRTT/2, granularity)` while active; a backlog0→positive write wake waits
the ACK interval before another observation. Idle polling instead uses PTO.

That cadence is NOT a global clone-rate cap. Both metric tasks also wait on
the accepted-authority watch and repoll immediately when it changes. Native
activation/terminal changes and changed positive operational bandwidth after
ACK-epoch/congestion/spurious callbacks notify the authority publisher. Its
Notify and the accepted watch coalesce, but an accepted change can still cause
one authority clone plus two metric clones without waiting SRTT/2. Startup
therefore has a reachable high-frequency observation path; these captures do
not measure its frequency or cost. No always-ready loop is established.

Existing `quic_carrier_ack_poll` reports only polls with nonzero ACK/loss,
not all captures, authority refreshes or retained journal occupancy. The real
encrypted `partial_late_original_corrects_its_loss_class_without_undoing_real_loss`
fixture in `crates/quinn-proto/src/tests/mod.rs` already produces retained BBR
loss records at an actual connection snapshot boundary. The existing rolling
callback fixture measures clone work and reclamation; the endpoint snapshot
fixture checks coherent/non-consuming observations. They provide small
reachability/control seams, not evidence of the startup burst's material cost.

Before materiality or a runtime correction is claimed, the missing evidence is
actual capture count, retained size at those captures, and exclusive on-CPU
cost of the copying/projection, distinguished from connection-lock waiting
and unrelated nested work. Operation counts or simulated callback timings
alone cannot supply that attribution. A bounded in-process thread-CPU measure
could supply actual execution evidence without equating elapsed wait with CPU;
none is implemented or selected here. The current container lacks perf/gdb and
has restrictive perf permissions; that is a diagnostic limitation, not a root
cause or permission to install tools/change capabilities. The user incident
and the material contribution of these snapshots remain **not attributed**.

## Complete native-snapshot CPU falsifier, 2026-09-11

**Outcome: complete snapshot execution is not the dominant startup CPU owner
in this capture. No snapshot/API/controller fix is selected. The deployed
incident remains unattributed.** This executes the narrower first measurement,
not the prospective nested-clone/occupancy observer described above.

Build4900 succeeds in1m21s with one existing unused-wrapper warning. The4779B
endpoint-only overlay wraps the three nonnested authority/shape/metrics calls
with Linux`CLOCK_THREAD_CPUTIME_ID`, including projection and temporary-clone
destruction. Recording happens after the ending CPU read and native unlock.
It changes no policy, ACK cursor, queue, await or native authority. Source was
fully reversed before traffic. Frozen binary
`./.tmp/reflection/bin/native-snapshot-cpu-20260911/mptunnel` preserves that build.
The underlying runtime is011b724, not the older d44four-cell comparator.

First run27528, tag`native-snapshot-cpu-twenty-quic-0911`, returns0but is
**invalid for snapshot CPU attribution**: host`MPTUNNEL_LAB_PERF` was not
forwarded into the container. Missing owner records are not zero CPU. Its
243,976,456B/40.076604s,48.702Mbps and.884466s body gap are retained, as are
37echo successes, one timeout and26subsequent unavailable records. Its server
process peak is113.356%; that does not recover the absent owner measurement.

Only the invocation changes: four-line`native_snapshot_cpu_observer.sh` sets
the existing periodic recorder inside the container, with per-call samples
disabled. Run38084, tag`native-snapshot-cpu-recorded-twenty-quic-0911`, returns0
in41.005834s. Both use the declared QUIC-only40s DOWN bulk+64B echo cell,
500Mbps, DOWN70ms/20%loss, UP30ms/0%loss, no jitter/QoS/blackhole. All41shape
rows verify it; actual active47is servereth0/clienteth1. These are diagnostic
runs, not a before/after speed comparison or packet-identical loss realization.

### Measured owner and actual process CPU

| Role / snapshot | Completed calls | Recorded CPU, us | Largest call, us |
|---|---:|---:|---:|
| Server authority | 611 | 27,136 | 674 |
| Server shape | 1,144 | 22,056 | 392 |
| Server metrics | 1,142 | 9,930 | 318 |
| Client authority | 18 | 147 | 29 |
| Client shape | 509 | 4,478 | 210 |
| Client metrics | 508 | 630 | 21 |

There are no clock-error records. Counts and CPU sums of every interval equal
the latest per-component cumulative totals; all byte fields are0. Server total
is59.122ms/2,897calls; client5.255ms/1,035calls. Final metric flushes extend to
41.280/41.279s of recorder lifetime. These scopes do not overlap one another;
they include acquisition/projection/destruction CPU, not exclusively cloning,
and exclude descheduled wall time. No retained-state size was measured.

The recorder truncates/floors each call to at least1us. Its absolute aggregation
error is bounded by1us per call: conservative upper totals are62.019ms(server)
and6.290ms(client). This does not bound all observer overhead or clock accuracy;
clock-read overhead is not subtracted, while recorder/logging work is outside
the measured scopes. Other existing feature perf records are also enabled.

| Role | Process CPU-s / observed-s | Whole mean / max, % | Sample30→40 mean / max, % |
|---|---:|---:|---:|
| Server | 5.54 /39.813852 | 13.915 /99.475 | 2.751 /3.937 |
| Client | 2.62 /39.779696 | 6.586 /49.868 | 1.429 /2.453 |

All82CPU observations have stable PID/starttime/boot identity and no collector
errors; the earlier stat-midpoint/CLK_TCK100formula applies. Server startup
peak is1.01CPU-s over1.015331s, Unix1789073181.306197→1789073182.321528
(approximately runner2–3s): **roughly one core is reproduced**. Its hottest
individual thread interval is26.591%, not a contradiction to process-total
CPU. Client peak is.53CPU-s/1.062814s around runner1–2s; hottest thread15.054%.
The largest stat bracket is.546504ms and collector invocation7.978622ms;
those are not total diagnostic overhead.

The largest server snapshot flush contains34.622ms/674calls, ending at
Unix1789073181465ms; the preceding flush is1789073180464ms. The next contains
5.478ms/66calls and ends1789073182467ms. Thus rapid startup repolling is real,
but small in total execution. The largest client flush is.342ms/26calls.
`interval_ms=1000` is the recorder's configured period, not an exact measured
denominator; sparse/close flushes and interval boundaries must remain visible.
These completion buckets do not give each call's wall interval or exact
overlap with process ticks. Even assigning ALL recorded server snapshot CPU
plus its rounding bound to the single1.01CPU-s peak accounts for only6.14%.
Across the larger capture, that conservative total is1.12%of sampled server
CPU; client is.24%. Window mismatch is conservative for this coarse bound,
not permission to present a precisely matched per-interval fraction.

### Service, costs and disposition

Valid38084receivesHTTP200 but intentionally stops a partial8GiB response:
193,870,592B/40.214757s=38.567Mbps, first body.665752s, maximum gap.660790s
at11.974343→12.635133s. Raw0–5/5–15/15–25/25–40/30–40Mbps are
174.364/57.157/8.269/1.637/1.025. All40raw bins remain in the archive.
Echo has35successes, one timeout23.835588→26.837897s, then27unavailable
records—not28independent timeouts. Successful median/p95/max are
224.100/1568.208/2147.462ms; no successful late echo is invented.

During the approximate peak band, native server ACK samples advance46,344,956B
and body bin2is482.229Mbps. This shows offered/completed native work alongside
CPU, not its instruction-level cause. Native ACKs continue in one epoch;
server window13,775,791B at row2contracts to20,000B at row40. Active sampled
DOWN/UP wire is206,862,636/3,856,477B with9,074/0class drops. Server/client
peak RSS252,616/74,824KiB remains finite in-load evidence, not a leak test.
Probe stderr is empty; logged broken-pipe/reset/HTTP3close warnings occur at
duration teardown, not the echo timeout. No native-policy tuning follows.

The information forecast succeeds narrowly: snapshot calls are too small to
dominate the reproduced process-total startup burst. Do not implement the
proposed nested-clone/occupancy observer or scalar snapshot redesign from this
result. It does not identify the remaining CPU owner, establish acceptable
20%loss service, or attribute the user's deployment. Diagnostic inconvenience
was not treated as a root cause, and elapsed lock time was not called CPU.

[Verified raw evidence](QUIC_NATIVE_SNAPSHOT_CPU_20260911.raw.tar.gz) preserves
both invocations:19regular files/244,029B,2,977,920B uncompressed. Every archive
member matches its source byte-for-byte; no configs, executables or links.
It contains both five-file result sets, drivers, build log, exact overlay,
invocation/CPU wrappers, unchanged runner/shape, and read-only analysis script
`./.tmp/reflection/analyze_native_snapshot_cpu_0911.py` for reproducing tables.

## Loss clear at20s: ordinary QUIC and H2 recovery,2026-09-11

Predeclared QUIC59318 then H299401 both CLOSED0. QUIC uses the unchanged
ordinary011b724 executable
`./.tmp/reflection/bin/ordered-feedback-20260911/mptunnel`, not the snapshot
diagnostic left in target/release. H2 retains its existing explicit500Mbps
up/down prior; MPP has dynamic discovery. No build, runtime observer, native
policy change, role override or favourable rerun occurred. This transaction
tests recovery after removing loss, not whether high-loss backoff is fixed
or whether the user's deployed CPU incident is explained.

Both40s DOWN bulk+64B echo cells use500Mbps each direction, DOWN70ms/UP30ms,
20%configured DOWN loss until the existing first5s epoch at/after20s, then0;
UP loss is0 throughout. No jitter/QoS/blackhole/router/mirror. The existing
loss20_cpu.py wrapper now applies the change to both46and47 and matches the
actual H2 process name for tick collection. No second polling loop is added.
Active traffic is47, verified from endpoints and class counters. Both sides'
41 service samples retain500Mbps and the declared delays; server netem first
shows0loss at row20 in both runs, with no further active-cut drops afterward.

### Transition timing and full service

The driver's `profile_elapsed_s` is sampled before issuing the shape commands:
20.003483114996925s QUIC and20.002201351802796s H2. The successful47 command
timestamps are monotonic1111310716342114ns/1111423022140336ns and
Unix1789076549210882675ns/1789076661516680757ns, respectively. The immediately
preceding46-success stamps bound47's actual update to approximately86.37ms
and83.79ms intervals, ending at those47-success times. These are measurement
brackets, not a profile fault or an exact packet-level clearance instant.
QUIC management row20 was generated atUnix1789076549106ms, before that change;
the row's later tc/CPU reads already observe the new profile. Keep those
sequential clocks separate rather than assigning every field the row elapsed.

| Outcome | MPP QUIC | Hysteria2 |
|---|---:|---:|
|Exact body bytes|1010139170|1493974976|
|Bulk elapsed,s|40.000323713|40.000246175|
|Whole goodput,Mbps|202.026|298.793|
|Driver elapsed,s|41.141486|41.004637|
|First body,s|.669212|.905815|
|Maximum body gap,s|.509998|.310781|
|Echo successes/failures|74 /0|80 /0|
|Successful echo p50/p95/max,ms|115.604 /690.206 /2478.386|109.578 /202.653 /307.684|

Both probes report `ok`/HTTP200; each intentionally stops a partial8GiB object,
not full-object completion. All74/80 echo exchanges succeed,4,736/5,120 exact
bytes each direction; no timeout/unavailable/restart is hidden. QUIC's fewer
exchanges reflect long successful service, not discarded failed attempts.
Its worst echo#31 is16.831422–19.309808s,2.478386s, before clearance; H2's
worst#23 is11.502265–11.809949s,.307684s. Success-to-success maxima are
2.875398/.706859s and include the500ms probe cadence.

QUIC's worst body gap16.567709–17.077707s advances206,721,922→206,787,458B;
H2's18.143187–18.453968s advances299,810,808→299,843,576B. These are body
offsets, not relay DSNs. The probe exports the worst interval rather than
every body read; `bulk_recovery_gap=0` is not a measured zero loss-clear delay
because this mode has no configured failover trigger.

| Probe-clock band | QUIC rawMbps | H2 rawMbps | QUIC echoes;p95/max,ms | H2 echoes;p95/max,ms |
|---|---:|---:|---:|---:|
|0–5s|179.386|108.868|10;716.919/716.919|10;218.050/218.050|
|5–15s|72.309|141.673|18;690.963/888.810|20;237.997/307.684|
|15–20s|14.121|132.435|6;2478.386/2478.386|10;209.197/209.197|
|20–25s|31.190|459.957|10;465.721/465.721|10;118.482/118.482|
|25–30s|402.485|471.716|10;153.482/153.482|10;117.385/117.385|
|30–40s|422.198|467.003|20;129.350/155.434|20;114.075/115.514|

Echo bands use request starts; quantiles use sorted successes at
round((n−1)×quantile). Raw body bins use the probe's own clock, not exact
qdisc-update boundaries. Complete40-bin series, no zeros in either:

```text
QUIC 0– 9:   1.433  43.872 398.974 324.308 128.344 185.503 111.778  87.370 105.084  60.036
QUIC10–19:  34.175  54.025  37.259  30.512  17.348  25.034   8.721  14.488  12.355  10.005
QUIC20–29:  15.153  17.590  17.538  19.922  85.747 281.768 419.335 430.887 445.124 435.309
QUIC30–39: 432.373 408.783 396.777 424.670 436.898 431.049 441.146 445.129 434.649 370.504
H2   0– 9:    .406 104.810 147.884 154.665 136.577 172.595 149.946 152.055 115.856 159.121
H2  10–19: 134.742 123.377 137.705 145.400 125.931 125.307 160.590 142.972  97.780 135.528
H2  20–29: 413.401 472.359 469.683 472.487 471.857 471.929 471.791 472.998 472.817 469.043
H2  30–39: 469.956 472.040 473.251 469.762 470.286 469.875 473.043 426.988 473.683 471.143
```

No arbitrary recovery threshold is needed to see the timing difference:
QUIC's20–23bins remain15–20Mbps, then24/25/26 rise85.747/281.768/419.335;
H2's20/21bins are already413.401/472.359Mbps. These sampled windows, not an
invented exact recovery millisecond or percentage cutoff, define the observed
several-second MPP delay. Different native policies/rate priors and random
packet histories prevent calling H2 a controller-neutral oracle.

### QUIC native reopening versus Product receipt

Both QUIC-role native epochs stay unchanged, paths active, and ACK counters
monotonic. Server ACK progress never stops at the clear. The exact server
epoch is9749182878209669413; management rows below show the native owner:

| Row /approx runner second | Native window B | Native flight B | Cumulative native ACKed B |
|---|---:|---:|---:|
|19|151987|143962|216928448|
|20,management precedes clear|97333|93588|217937459|
|21|229108|228000|219645047|
|22|229108|228000|221831447|
|23|229108|228000|224145047|
|24|301714|301200|226722647|
|25|2513251|2018400|235817447|
|26|9409651|5568000|268452647|
|27|7998204|4970400|323695847|
|30|7998204|4538400|493324247|
|40|7998204|5823600|1034667047|

Native24→25/25→26/26→27 ACK increments are9.095/32.635/55.243MB. Those
native-window and delivery increases coincide at sample resolution with the
body's24–26 acceleration. After reopening, native and body service both
continue at high rates; this is not observed high native throughput stranded
behind a persistent Product receive hole. It is also not a permanent frozen
controller/epoch or connection restart. The several seconds of low native
window after clear remain an actual service cost, not waived by eventual
recovery. Final native ACK totals are1,034,667,047B server and5,367,957B client;
these include transport framing/copies and are not application payload totals.
Server SRTT stays100.115–144.656ms. H2 exports no corresponding native window/
ACK/epoch telemetry in this capture: those quantities are unknown, not zero.

### Actual process/thread CPU and wire cost

Each role has41 successful CPU records, one stable boot/PID/starttime identity,
CLK_TCK100, and no collector/process/thread error. Each adjacent interval uses
its own `/proc/stat` read's monotonic midpoint:
CPU% =100×delta(utime+stime)/CLK_TCK/delta(midpoint). Whole/band means divide
summed CPU seconds by summed elapsed seconds, not an average of percentages.
Rows are not exactly1s apart: shaping/collection shifts intervals, so use
actual timestamps and retain short compensating intervals. Bands below span
the indicated row endpoints; all row20 CPU readings are after successful47
clear, unlike QUIC's earlier row20 management generation.

| Role | Measured CPU s /observed s | Whole mean% | Maximum interval% | Hottest single-thread interval% |
|---|---:|---:|---:|---:|
|QUIC client|16.88 /39.737672|42.479|111.549|29.530|
|QUIC server|30.19 /39.787072|75.879|174.687|47.985|
|H2 client|22.38 /39.674385|56.409|97.817|21.741|
|H2 server|19.95 /39.695404|50.258|87.077|18.933|

Process totals and thread observations overlap and must not be added.
Threads are OS identities, not Rust/Go tasks or code attribution. QUIC's
startup server maximum103.561% is1.02CPU-s/.984923s at Unix
1789076531.151982–1789076532.136905, approximately runner2–3s; one-core-total
execution is reproduced alongside substantial early native work. Its overall
maximum174.687% is1.74CPU-s/.996067s at Unix1789076555.194179–1789076556.190246,
approximately26–27s, AFTER recovery. Client maximum111.549% is.68CPU-s/
.609598s at Unix1789076559.932416–1789076560.542014. None identifies the user's
reported incident or proves every executed instruction necessary.

| Row band | QUIC client mean/max% | QUIC server mean/max% | H2 client mean/max% | H2 server mean/max% |
|---|---:|---:|---:|---:|
|0–5|21.361/37.442|54.276/103.561|20.146/23.957|17.474/18.912|
|5–15|7.764/16.291|17.177/37.417|20.260/24.999|19.556/25.240|
|15–20|4.166/6.729|7.541/9.403|21.294/27.546|18.906/20.471|
|20–25|15.191/37.828|38.651/89.458|90.586/93.819|78.689/82.095|
|25–30|96.028/106.790|164.169/174.687|94.995/97.817|84.147/86.539|
|30–40|95.698/111.549|156.288/172.737|93.133/96.236|83.077/87.077|

QUIC's initial post-clear20→24 server intervals are14.773/13.069/28.838/
18.041%; the larger24→25 interval is89.458%. Thus sustained local CPU
saturation is not the observed low-window recovery delay's owner. High CPU
returns with high delivered work; this does not establish optimal per-byte
cost. Maximum stat-read brackets are2.641/.227ms QUIC client/server and
.550/.784ms H2; maximum complete collector invocations6.802/5.400ms and
6.145/5.686ms. These are collector brackets, not total observer overhead.

| Capture | Active DOWN /UP wire B | Active DOWN /UP drops | Peak DOWN /UP backlog B | Peak client/server RSS,KiB |
|---|---:|---:|---:|---:|
|QUIC|1082486452 /18606130|8948 /0|6082074 /36863|83756 /297484|
|H2|1572069584 /11463289|6473 /0|5167437 /17476|46672 /45476|

Wire/drop values are first→last class deltas. After row20 active drop counters
remain8,955QUIC/6,476H2; these cumulative counts include seven/three before row0.
No further configured loss or class/netem drops occur. QUIC's unused46 adds1,529server
bytes/two drops and42client bytes; H2's unused cut adds0. These do not form a
second data capacity. Native counters, class wire and body windows differ;
do not call their ratios exact framing or retransmission amplification.
All RSS values are in-load observations, not post-load reclamation/leak tests.
Probe stderr is empty. QUIC broken-pipe/reset/H3closure warnings occur at
duration teardown; H2 disconnects and closes gracefully, with no failed echo.

Disposition: eventual native/body reopening rejects permanent stuck
state in this tested QUIC episode, but not its several-second recovery cost
or very low pre-clear service. H2 returns substantially sooner under the same
configured cut with a different native policy and explicit rate prior. The
measured post-clear delay precedes native reopening and is not explained by
sustained process CPU saturation; no native cause is proven by that exclusion.
The original CPU report remains unattributed. No loss threshold, larger
window, snapshot redesign or claim of fixed backoff/promotion follows.

[Verified raw archive](QUIC_LOSS_CLEAR_20260911.raw.tar.gz):15 regular files,
164,954B compressed/1,882,238B uncompressed. It contains both complete five-file
result sets, both closed drivers and the exact loss20_cpu.py/run.py/shape.sh.
Every member was compared byte-for-byte with its source, without hashes.
No executable/config/secret or new harness is included. Result tags are
quic-combined-down-loss-clear-quic-0911 and h2-combined-down-loss-clear-h2-0911
under `./.tmp/reflection/results/`; the ordinary binary path is stated above.
