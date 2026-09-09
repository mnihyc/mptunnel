# Live-hedge service ablation — 2026-09-09

Status: **unsafe diagnostic intervention, not a recovery fix or release candidate**.
The same-build healthy mixed pair removes all observed accepted response repair
and duplicate receipt. Useful goodput rises420.434→450.065Mbps and echo median/
p95 falls434.334/603.703→289.985/422.512ms. This supports a material cost of live
hedge service and its downstream allocation effects in this pair, without
reducing useful load. It does not make suppression safe or isolate one wire/
CPU substage. Echo completion spacing worsens, CPU and UP traffic increase,
and substantial native RTT remains. No runtime change is retained.

## Predeclared contract and provenance

[The successor characterization](LIVE_REPAIR_SUCCESSOR_20260909.md) establishes
a model-constrained response `FinalDrain` boundary, not the cause of a measured
stall. [The fixed window](REPAIR_WINDOW_20260909.md) proves late copies in one
healthy interval, not their counterfactual impact. CURRENT_CLOSURE_PLAN therefore
selected one separate cost discriminator before this build or pair: does live
response hedge service materially contribute to mixed latency, or does mixed
native/shared service remain similarly slow when that category is absent?

Information forecast: lower echo/native RTT at comparable useful load supports
that category as a contributor; no change despite verified suppression moves
attribution away from it. Lower latency only through lower useful service is
not a performance correction. Structural/stale replacement traffic must remain
visible. The earlier196–318MB copy volume is roughly39–64Mbps/40s of payload
service equivalent, not promised removable bandwidth. The prior mixed/QUIC
median difference is a comparison envelope, not a measured removable delay.
No favorable repeat, controller adjustment or public claim is authorized.

Both cells use ordinaryb2aa215 plus the same frozen six-file feature overlay:

```text
src/lab_diagnostics.rs
src/runtime/relay/client.rs
src/runtime/relay/server.rs
src/runtime/sender/response/dispatch.rs
src/runtime/sender/response/prepared.rs
src/runtime/sender/response/service.rs
```

The intervention is a process-fixed `MPTUNNEL_LAB_SUPPRESS_LIVE_RESPONSE_REPAIR`
flag, read once. It suppresses only server live persistent-gap, active/final
retained-frontier and generic live ACK-gap production before enqueue, and
excludes the resulting nonexistent recovery-only wait. It does not falsify
alternative-path facts or gate dispatch. Original placement policy, ACK/MAX,
accepted debt, native writes, structural stale/failed/unknown-owner recovery,
requalification and cleanup remain unchanged. Client policy is unchanged.
Independent source review checked the generic branch and expired-deadline
service so the intervention does not introduce a replacement live producer
or deliberate deadline busy loop. It still weakens required recovery and is
not a proposed permanent behavior.

Both cells enable the same periodic Original/copy-cause/unique-receipt observer
and disable per-frame samples. The counter recorder's zero-duration/1us-floor
bookkeeping is not service timing. Control runs first with the flag unset;
suppressed runs second with it set. Each server emits exactly the expected
`live_response_repair_ablation` marker, false/true respectively; neither client
emits it. Both wrappers unset the previous echo-membership and bulk-placement
interventions. The process markers and actual accepted counters verify the
intervention, rather than relying only on wrapper intent.

The clean optimized build took3m37s. The six-file overlay was frozen and fully
removed before traffic; ordinary source/RFC and executable were restored and
byte-compared by the parent. Frozen diagnostic executable:
`./.tmp/reflection/bin/live-hedge-ablation-20260909/mptunnel`.
Inputs in that reflection directory are `live-hedge-ablation-0909.patch`,
`live-hedge-ablation-build-0909.log`, `live_hedge_{control,suppressed}.sh` and
`live-hedge-{control,suppressed}-0909-run.log`. Results are
`results/mixed-combined-down-live-hedge-{control,suppressed}-0909/`.

[Raw archive](LIVE_HEDGE_SERVICE_ABLATION_20260909.raw.tar.gz),518654B, retains
17 regular files: both complete five-file results, patch, build, two run logs,
two wrappers and `live-hedge-costs-0909.json`. Parent gzip integrity and
decompressed byte comparisons pass. No binary is archived. Both runners exit0,
elapsed41.004853/41.005031s. Existing HTB quantum warnings are preserved;
no profile change was made to silence them.

## Complete useful service and timing

Both40s mixed DOWN cells retain500Mbps in each direction,30ms DOWN/70ms UP,
zero configured jitter/loss and no blackhole. All82 effective service rows
confirm that profile, HTB burst65536 and netem limit8192; all class/qdisc drop
deltas are zero. There is no15/25s QoS transition. The8GiB HTTP response supplies
backlog alongside64B echoes scheduled every500ms, unchanged3s timeout.

| Outcome | Control | Live hedges suppressed |
|---|---:|---:|
| Received body B | 2102181943 | 2250340522 |
| Body duration s | 40.000242 | 40.000243 |
| Whole useful goodput Mbps | 420.433842 | 450.065370 |
| First body s | .607728 | .578144 |
| Maximum body-read gap s | .537601 | .413086 |
| Maximum-gap start → end s | 31.100501→31.638102 | 21.951594→22.364680 |
| Bytes before / after gap | 1667264067 /1667276067 | 1186052282 /1186117818 |
| HTTP code / complete / partial requests | 200 /0 /1 | 200 /0 /1 |
| Echo success / attempts / failures | 77 /77 /0 | 80 /80 /0 |
| Echo request / response B | 4928 /4928 | 5120 /5120 |
| Echo p50 /p95 /maximum ms | 434.334 /603.703 /841.278 | 289.985 /422.512 /760.980 |
| Maximum successive echo-completion gap s | .885998 | 1.017568 |

The body is intentionally duration-stopped, not a completed8GiB transfer.
Both `probe.err` files are empty. All157 recorded attempts have unique ordered
indices, finite valid start/end/latency values and successful outcomes. Slow
control exchanges reduce the number generated;77/77 does not hide three failed
attempts. Last echoes finish40.341244/40.048559s, after the bulk window, and
remain included. No success-only filtering or trimmed body mean is used here.

| Fixed interval | Control body Mbps | Suppressed body Mbps | Control echo p50/p95/max ms | Suppressed echo p50/p95/max ms |
|---|---:|---:|---|---|
| 0–5s | 271.260 | 286.785 | 227.587 /549.367 /549.367 (10) | 148.475 /326.638 /326.638 (10) |
| 5–15s | 478.257 | 478.275 | 499.403 /603.703 /800.067 (19) | 252.309 /510.560 /760.980 (20) |
| 15–25s | 452.808 | 474.676 | 446.808 /731.235 /755.488 (19) | 307.300 /454.661 /491.363 (20) |
| 25–40s | 410.004 | 469.279 | 388.334 /564.732 /841.278 (29) | 292.738 /369.852 /422.512 (30) |

Body means include every raw bin; echo phases use attempt start and the
rounded `(n−1)p` index convention, with attempt counts in parentheses.
These are healthy chronology slices, not independently controlled conditions.
The5–15s useful load is nearly identical while echo median falls; lower useful
load is not an explanation for that phase's improvement. Control also has
42.546/1.168Mbps bins at31/32s and later buffered release; the suppressed
chronology does not repeat that excursion. This is one realization, not a
repeatability or universal stability guarantee.

| Cell / attempt | Start → end s | Whole echo ms |
|---|---|---:|
| Control53 | 27.989235→28.830514 | 841.278 |
| Control19 | 9.838731→10.638798 | 800.067 |
| Control44 | 23.047796→23.803284 | 755.488 |
| Suppressed23 | 11.518261→12.279240 | 760.980 |
| Suppressed13 | 6.505245→7.015805 | 510.560 |
| Suppressed31 | 15.783503→16.274867 | 491.363 |

The adverse completion-spacing metric is genuine, not omitted: control's
largest completion-to-completion interval is29.459996→30.345994s (attempts55→56),
whereas suppression's is11.261672→12.279240s (22→23). It includes both scheduled
spacing and the exchange itself and is not interchangeable with whole-echo
latency. Better average/percentile latency does not make every timing measure
better.

Client Broken-pipe occurs at each bulk stop,08:53:31.060/08:54:27.421UTC;
server RemoteClosed follows at31.133/27.494 within those respective minutes.
H3_NO_ERROR shutdown follows at08:53:32.029/08:54:28.456. These retained closure
records are not probe failures or an inferred new lifecycle defect.

## Actual intervention, receipt and allocation accounting

Independent producer/consumer accounting verifies all interval count, byte and
time sums against cumulative values: server798/707 rows, client857/839 rows,
one PID per role/cell and no invalid-receipt component. Absence of a component
means no observed successful event in the flushed capture, not a fabricated
zero-valued row. Actual accepted work is:

| Source payload B | Control | Suppressed |
|---|---:|---:|
| TCP persistent-gap copies | 54899855 | 0 |
| TCP live tail copies | 34557440 | 0 |
| QUIC persistent-gap copies | 5097529 | 0 |
| QUIC live tail copies | 5462843 | 0 |
| All accepted repair | 100017667 | 0 |
| TCP Originals | 766329628 | 1265693527 |
| QUIC Originals | 1354653143 | 1009611203 |
| All Originals | 2120982771 | 2275304730 |
| TCP Original share | 36.1309% | 55.6274% |

There is no observed structural, unknown-owner, completion or other repair
substitution, nor requalification, in either flushed capture. Thus actual
copies disappear, not merely change cause labels. Original placement policy
is unchanged but realized placement shifts materially toward TCP; this is a
mediated effect of changing recovery service, not a bit-identical allocation
control or proof that the improvement equals removed-copy wire cost alone.

| Receiver payload B | Control | Suppressed |
|---|---:|---:|
| TCP new /duplicate | 764641092 /83535415 | 1260795143 /0 |
| QUIC new /duplicate | 1337587523 /10167036 | 991835699 /0 |
| All new /duplicate | 2102228615 /93702451 | 2252630842 /0 |
| All input = new +duplicate | 2195931066 | 2252630842 |
| Ordered-triggered total | 2102228615 | 2250369850 |
| New not yet ordered at final observation | 0 | 2260992 |

Ordered-triggered data can release buffered bytes from another ingress; it is
not carrier receipt attribution or completed local delivery. The suppressed
2260992B difference describes the observation boundary, not a post-teardown
leak. Original admission, receiver totals and completed body have different
cancellation/transit/flush tails; their differences are not silently charged
as loss or corruption. Aggregate duplicate totals do not identify which
individual control copies won before their Original arrived.

Server successful encoded TCP plaintext is856420250→1266593224B; QUIC encode
and successful write-wait agree within each cell at1369494565→1012764375B.
Total encoded bytes rise2225914815→2279357599B despite removing100017667B of
accepted copies, because Originals rise154321959B. Encoded-minus-Original/copy
residual is not native retransmission attribution. Client return encoded TCP
is14949324→14716813B and QUIC5717971→5631551B. Native class traffic includes
other bytes and has different snapshot windows; it need not follow that small
encoded-return decrease.

## Physical and resource cost

Parent accounting verifies all82 profiles and stable per-role session/epoch,
including QUIC physicalinstance1 through all41 samples per cell. Cost windows
span40.004650/40.004846s, not exactly the body or final perf-flush windows.

| Sampled cost | Control | Suppressed |
|---|---:|---:|
| DOWN /UP class byte deltas | 2350079404 /77346185 | 2393973834 /82204002 |
| DOWN /UP class packet-counter deltas | 1722581 /672548 | 1731857 /750721 |
| Peak DOWN /UP backlog B | 35909196 /282790 | 18912346 /254589 |
| Client peak /final RSS KiB | 125236 /108032 | 93560 /93560 |
| Server peak /final RSS KiB | 319368 /319096 | 312384 /296760 |
| Client peak /final ps CPU % | 111 /111 | 119 /119 |
| Server peak /final ps CPU % | 188 /188 | 195 /195 |
| Client QUIC RTT p50 /p95 /max ms | 421.405 /572.057 /625.255 | 246.100 /345.323 /371.787 |
| Server QUIC RTT p50 /p95 /max ms | 452.043 /573.750 /669.501 | 249.440 /325.620 /365.798 |
| Server QUIC flight p50 /p95 /max B | 17259871 /24281636 /25696010 | 6112920 /12706452 /14668104 |

All class/qdisc drop deltas are zero. Queues/native flight and RTT decrease,
but class bytes and reported CPU increase alongside useful work. Packet counters
are offload-sensitive, not physical-wire packet counts. Sampled queue peaks
are not continuous maxima or the precise position of a delayed echo. Process
ps CPU is lifetime multicore utilization, not interval attribution; sampled RSS
does not prove cleanup or leak freedom. Server median QUIC RTT remains249ms,
well above configured100ms propagation, so this is not full latency recovery.

## Outcome versus forecast and disposition

The intervention is verified and produces lower echo/native latency with more
useful work, supporting live-hedge service and its allocation effects as a
material contributor in this pair. That is the diagnostic information gained;
neither a high Mbps mean nor zero observed copies constitutes a safe model.
The adverse completion spacing, higher CPU/UP traffic and remaining latency
prevent an all-benefit claim. Same executable and declared profile do not make
independent native histories, realized allocations or task scheduling identical.
One ordered pair does not establish a causal magnitude with statistical confidence.

No blackhole, sudden QoS or required failover is exercised here. Withholding
live recovery can expose severe stalls in those conditions; it cannot ship
based on healthy evidence. No pipelined successor, threshold, protocol
preference or controller change follows automatically. Ordinary source/RFC
and release executable remain restored, with no accepted runtime patch.
CURRENT_CLOSURE_PLAN owns the next bounded model decision; README, wider
competitiveness and release acceptance remain deferred.

## All one-second body bins

Mbps, all80 raw bins, including startup and adverse intervals. Application
bursts above500Mbps are not sustained physical-link capacity. Every echo's
original-precision start/end/outcome remains in the archived probe JSON.

```text
control = [1.049,130.311,279.245,682.133,263.563,624.298,503.045,413.473,496.336,485.352,217.852,541.726,303.345,759.367,437.78,497.514,470.62,437.967,458.931,398.074,468.465,468.332,454.25,381.951,491.973,232.459,672.056,437.16,273.464,607.46,406.248,42.546,1.168,683.793,506.846,456.8,469.825,465.303,471.739,423.195]
suppressed = [2.62,112.411,127.83,786.173,404.892,614.361,480.725,416.09,452.789,545.699,486.731,315.382,509.914,497.12,463.94,482.092,435.506,278.462,693.096,482.869,469.02,430.694,482.008,516.05,476.961,469.048,471.937,468.958,388.904,547.174,471.696,478.294,371.477,498.452,547.705,464.096,478.052,453.717,317.462,612.219]
```
