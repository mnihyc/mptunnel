# Echo membership and mixed-carrier residence — 2026-09-09

Status: completed diagnostic counterfactual and one predeclared carrier ablation,
**not a runtime correction, ordinary performance acceptance or release**.
Adding QUIC really changes the echo's membership and winning carrier, but its
winning QUIC replies still take median228ms from native write to client decode.
The same-build QUIC-only ablation reduces that median to31ms while delivering
more useful bulk bytes. Mixed context matters; membership alone is insufficient.
This does not yet separate TCP data, feedback, native queues or allocation policy.

## Question, forecast and provenance

The origin, startup/lifecycle review and pre-run forecast are preserved in
[ECHO_OWNER_SERVICE_20260909](ECHO_OWNER_SERVICE_20260909.md).
Commit282b8e1's recurring rebalance serves Throughput traffic; the current small
echo never enters that branch. Commit9ab3cbcb still permits additional recovery
attachments, so this is not a permanent two-member deadlock. Merely ungating
bulk geometry or assuming C→S scores describe S→C would not be a justified fix.

The diagnostic waits for this echo's received response and two existing TCP
members, then requests one ordinary asynchronous QUIC attachment to an already
active exact incarnation. It preserves normal open classification, retry,
selection, data, feedback, native control, FIN and cancellation. The intervention
does not force the selected output. Its feature-only flag is unset in the
control and set only in the intervention. One injected job is not a promise of
one physical open attempt. No production protocol preference is proposed.

The prior461ms response residence offered an upper removable envelope of~431ms
if an eligible alternative incurred only the configured30ms return leg. Separate
request-side waits are outside that envelope. At most5120B of echo payload does
not forecast a meaningful direct bulk gain. Accepted membership without actual
selection would be inconclusive; selected QUIC with similar delay would reject
membership alone as the dominant explanation. The latter outcome occurs here.

All three cells use ordinaryb2aa215 plus the same frozen feature observer,
`./.tmp/reflection/bin/echo-membership-20260909/mptunnel`. Its clean optimized
build took3m35s. The complete12-source-file overlay was reversed before traffic;
the ordinary executable was restored afterward by verified byte comparison.
The two mixed cells run control then intervention; the additional QUIC-only
cell reuses the same executable with the flag unset and existing QUIC config.
There were no controller, queue-size or impairment changes between cells.

The [mixed-pair raw archive](ECHO_MEMBERSHIP_COUNTERFACTUAL_20260909.raw.tar.gz)
contains15 regular files: ten result files, build log, two run logs, exact source
overlay and wrapper. Size436824B; gzip integrity and byte comparisons pass.
The separate [QUIC-only raw archive](ECHO_QUIC_CONTEXT_20260909.raw.tar.gz)
contains11 regular files: five result files, run log, three configs, runner and
shaping script. Size67774B; the same integrity/byte checks pass.
Results are respectively under `./.tmp/reflection/results/`:

```text
mixed-combined-down-echo-membership-control-0909/
mixed-combined-down-echo-membership-quic-0909/
quic-combined-down-echo-membership-quic-only-0909/
```

## Full workload and timing

Each40s cell downloads an8GiB HTTP object concurrently with64B TCP echoes every
500ms, with the unchanged3s echo observation timeout. The object is intentionally
duration-stopped: one HTTP200 partial request, zero complete requests in each
cell, not a completed8GiB transfer. Runner exit0 and empty `probe.err` in all.
All echoes succeed, including the final in-progress echoes completing slightly
after40s in the mixed cells; no failed attempts are excluded from quantiles.

| Metric | Mixed control | Mixed + echo QUIC | QUIC-only ablation |
|---|---:|---:|---:|
| Received body bytes | 2006450680 | 2053881344 | 2140748488 |
| Body duration s | 40.000194 | 40.000056 | 40.000077 |
| Whole goodput Mbps | 401.288190 | 410.775694 | 428.148869 |
| First body s | .578840 | .580081 | .409319 |
| Maximum body-read gap s | .305283 | .401585 | .104166 |
| Gap interval s | 31.694649–31.999931 | 25.747548–26.149133 | .409319–.513485 |
| Bytes before/after gap | 1622191336 / 1622227336 | 1301088316 / 1301102916 | 11792 / 47792 |
| Echo successful / attempted | 78 / 78 | 80 / 80 | 80 / 80 |
| Echo p50 / p95 / max ms | 305.536 / 536.377 / 1192.203 | 311.685 / 477.474 / 749.794 | 106.536 / 159.800 / 274.824 |
| Maximum success-to-success gap s | 1.335131 | .863396 | .610905 |
| Body0–5s mean Mbps | 285.598 | 275.055 | 348.104 |
| Body5–15s mean Mbps | 442.421 | 439.438 | 443.248 |
| Body15–25s mean Mbps | 435.336 | 428.463 | 431.921 |
| Body25–40s mean Mbps | 389.724 | 425.112 | 442.246 |

These nominal phase means include every raw one-second bin. There is **no QoS
transition** at15/25s in these healthy cells. The mixed intervention improves
some tails but worsens the median and body gap; neither its mean nor the later
ablation is ordinary-build acceptance. The same-build ablation changes all TCP
data/control/membership together, not one isolated packet category.

The client Broken-pipe warnings occur at06:31:35.149,06:32:38.876 and06:42:12.314
UTC, respectively at the duration-stop boundary. Server RemoteClosed follows
~71–73ms later and H3_NO_ERROR closure~1.026s later. These are recorded teardown,
not failed echo attempts during the workload; they are retained in full logs.

## Actual identities, membership and byte conservation

C/S below mean physical client/server log lines. Cross-process joins use Unix
milliseconds and exact logical ranges, not local monotonic origins or sequence
numbers. Millisecond equality does not imply zero work. Native successful write
is transport acceptance, not packet departure or application delivery.

All echo streams are logicalstream0. The mixed control session is
`1159682636944778469`, intervention `1158948676669372681`, and QUIC-only
`14977748263365763762`. Client runtime indexes/attachments and server wire IDs
have different namespaces and must not be numerically equated:

| Cell / carrier | Server wire / physical / attachment | Client runtime index / physical / attachment |
|---|---|---|
| Control TCP | 1 / 4 / 1 | 0 / 3 / 0 |
| Control TCP | 2 / 2 / 2 | 1 / 2 / 1 |
| Intervention TCP | 1 / 4 / 1 | 0 / 4 / 0 |
| Intervention TCP | 2 / 3 / 2 | 1 / 3 / 1 |
| Intervention QUIC | 0 / 1 / 3 | 0 / 1 / 2 |
| QUIC-only | 0 / 1 / 1 | 0 / 1 / 0 |

In the control the session's QUIC remains physically present but is never an
echo member or committed candidate. In the intervention, the second TCP member
attaches at1788935519653ms (C11); the one injected job is recorded at the same
time (C13), and actual QUIC attachment completes at1788935519757ms (C14),104ms
later. The first selected QUIC Original is[192,256) at1788935520450ms (S47).
All its QUIC writes use native HTTP/3 requeststream12; QUIC-only uses stream4.

Every one of the41 samples per cell preserves QUIC physicalinstance1 and a
single role-local native epoch. Client/server epochs are intentionally distinct:
control6911669284898641080/8225059862769371656;
intervention13936433764830560242/13117297753632879932;
QUIC-only6680828443539227485/5322794307676453003.
There is no physical replacement that invalidates this counterfactual.

| Joined response accounting | Control | Intervention | QUIC-only |
|---|---:|---:|---:|
| Reads / source enqueues / Original claims | 78 each | 80 each | 80 each |
| Unique bytes / ordered local bytes | 4992 / 4992 | 5120 / 5120 | 5120 / 5120 |
| Native positive writes / decodes / mux applies | 86 each | 86 each | 80 each |
| Native positive-write bytes | 5504 | 5504 | 5120 |
| Winning TCP / QUIC ranges | 78 / 0 | 35 / 45 | 0 / 80 |
| Winning Original / recovery-copy ranges | 77 / 1 | 80 / 0 | 80 / 0 |
| Nonadvancing arrivals / bytes | 8 / 512 | 6 / 384 | 0 / 0 |

All ranges are64B; Original claims cover[0,4992) or[0,5120) without gaps or
overlap. Every enqueue starts at the claimed frontier with an empty prior
source; all decoder/writer records join, every mux before/after frontier agrees
with first byte coverage, and every winning range reaches local delivery once.
Control's eight nonadvancing arrivals are seven losing copies plus one losing
Original. Intervention's six are five QUIC copies and one TCP copy.

The intervention has78 committed QUIC candidate views:45 Active/ready/selected,
28 Active/not-ready/unselected, and five ready but `Failed`/no-native-authority/
unselected views (S34,175,357,421,781). Physical QUIC remains stable. A snapshot's
`Failed` value here is not proof of physical carrier failure or a new defect.
In particular, the worst whole echo selects TCP at[1472,1536) while its QUIC
candidate lacks native authority; it did not ignore a demonstrated better
eligible QUIC opportunity. Initial selection attempts are not an exhaustive
retry log; committed claims and the actual winning copy determine these counts.

## Exact response residence, not estimated bandwidth

Quantiles below use the same nearest order-statistic rounding as the probe.
There are78 control winners,35 intervention TCP winners,45 intervention QUIC
winners and80 QUIC-only winners. All figures are milliseconds.

| Stage, p50 / p95 / max | Control winners | Intervention TCP | Intervention QUIC | QUIC-only |
|---|---:|---:|---:|---:|
| Winning native write → client decode | 234 / 416 / 483 | 223 / 382 / 677 | 228 / 327 / 426 | 31 / 89 / 177 |
| Source enqueue → Original claim, max | 2 | 3 | 8 | 11 |
| Original claim → Original native write, max | 1 | 1 | 1 | 3 |
| Decode → mux, max | 2 | 1 | 1 | 3 |
| Mux → local delivery, max | 1 | 1 | 1 | 1 |

The winning recovery copy must be distinguished from its Original: control
[192,256) Original is written on wireTCP2 at1788935456719 (S48), but a copy
written on wireTCP1 at1788935456920 (S50) wins at1788935457194 (C19–21).
The201ms interval is **not** Original-writer service time. Its274ms copy
residence controls delivery. The Original decodes72ms later (C23–24) and adds
no bytes; its547ms residence is not the winning response latency.

Critical whole-echo boundaries remain explicit:

- Control worst attempt26,[1664,1728),13.047779→14.239982s:1192.203ms.
  Request read1788935468196 (C116); first target write8904 (S320), enqueue8904
  (S322), claim/positive TCP write8905 (S326/331), decode/mux/local9388
  (C117–119), with suffix times relative to1788935460000.
  Approximately708ms is request-side/target arrival and483ms is postwrite;
  a later repeated target frontier (S332) must not replace the first one.
- Intervention worst attempt23,[1472,1536),11.504688→12.254482s:749.794ms.
  Request1788935530380 (C107), target0451 (S346), enqueue/claim/TCPwrite0452
  (S348/353/359), decode/mux1129 (C108–109), local1130 (C110).
  Native-write→decode is677ms. A QUIC recovery copy written1074 (S361)
  decodes1294 (C112–113), after the winning Original, and cannot explain
  user-visible improvement. The unselected QUIC candidate at S357 lacks authority.
- Intervention longest winning QUIC, attempt26,[1664,1728):497.742ms.
  Request1788935532130 (C121), target2200 (S395), enqueue2201 (S397),
  claim/write2202 (S402/408), decode/mux/local2628 (C124–126):426ms after write.
  Another QUIC winner,[1792,1856),has624.021ms whole latency including210ms
  request→target and412ms postwrite; changing response placement cannot remove both.
- QUIC-only worst attempt4,[256,320),2.000665→2.275489s:274.824ms.
  Request1788936094314 (C18), target/enqueue/claim/write4411 (S47/49/52/56),
  decode/mux4588 (C19–20), local4589 (C21). This is97ms request-side plus
  177ms postwrite and1ms local, not a completely delay-free QUIC result.

These exact joins place the large mixed residual after native acceptance and
before authenticated decode. They do not split native pacing/queue, emitted
network queue, peer transport/read/framing or CPU scheduling into exact causes.
They do exclude source-claim and postdecode work as the dominant individual
intervals in these captures. Stable incarnation does not imply stable queueing.

## Actual profile, native state and cost

All123 service rows verify500Mbps in both router classes,30ms DOWN and70ms UP,
zero configured jitter/loss, no blackhole, HTB burst65536 and netem limit8192.
Every observed HTB/netem drop delta is zero. There are41 rows per cell spanning
40.010720/40.005296/40.004823s; client management Unix endpoints are
1788935455080–1788935495080,1788935518817–1788935558817 and
1788936092259–1788936132258. Their clock origins differ from probe offsets.

| Sampled whole-run cost | Control | Intervention | QUIC-only |
|---|---:|---:|---:|
| DOWN class byte delta | 2409110874 | 2413972642 | 2272428362 |
| UP class byte delta | 86466491 | 91114565 | 36203451 |
| DOWN class packet-counter delta | 1767905 | 1774143 | 1521044 |
| UP class packet-counter delta | 761718 | 805852 | 336796 |
| Peak DOWN backlog B | 29637764 | 25770134 | 8193096 |
| Peak UP backlog B | 270491 | 295849 | 93392 |
| Client peak RSS KiB / last ps CPU % | 89412 / 115 | 94312 / 118 | 38324 / 99.5 |
| Server peak RSS KiB / last ps CPU % | 350732 / 199 | 343592 / 199 | 366160 / 154 |
| Client QUIC RTT sample p50 / p95 ms | 290.604 / 486.093 | 281.468 / 428.351 | 101.354 / 144.958 |
| Server QUIC RTT sample p50 / p95 ms | 297.943 / 494.806 | 293.029 / 426.739 | 101.233 / 151.674 |
| Server QUIC native-flight sample p50 / max B | 9230206 / 26889396 | 8078503 / 16780764 | 5889312 / 15687408 |

Class counters include bulk, echo, native acknowledgements, Product feedback,
recovery and protocol overhead; they are not independently attributable echo
costs. Offload-sensitive packet counters are not a physical-wire packet count.
Periodic backlog samples are neither continuous maxima nor an exact echo's
queue position; queue/C gives a conditional drain time, not measured per-byte
delay. Native-flight and RTT distributions cover all service samples, not only
selected echo decisions. `ps %CPU` is process-lifetime multicore utilization,
not interval CPU consumption; RSS is a sampled process value, not a leak proof.
QUIC-only's server peak RSS is higher, so no universal resource reduction claim.

## Outcome and next boundary

The valid membership intervention wins45 QUIC replies, yet median return
residence228ms is close to control TCP234ms and intervention TCP223ms. This
falsifies membership alone as the dominant remedy in this mixed loaded case.
It does not justify a hardcoded QUIC preference, bulk-rebalance ungating or a
controller adjustment. Some better tails coexist with a worse body gap/median.

The predeclared same-build QUIC-only cell then wins all80 replies and reduces
postwrite residence to31/89/177ms, with greater useful goodput and lower class
traffic/backlog. That supports a material mixed-carrier composition effect,
not generally slow QUIC at this load. Removing TCP simultaneously changes data,
control fanout and membership, so this is not unique attribution to any one.
The next bounded decision must distinguish those existing owners before a model
correction; it cannot infer a general shared-bottleneck partition or native
priority fix from these totals. No production change is accepted by this report.

## Complete one-second body series

Mbps, all40 raw bins in order, including startup; no trimming/interpolation.
Every original-precision echo start/end/outcome and probe field remains in the
linked raw JSON. A one-second application bin may exceed500Mbps when previously
queued bytes arrive together; no claim of a sustained link-rate excess follows.

```text
mixed_control = [2.621,107.671,245.795,666.512,405.39,515.827,238.063,595.835,392.866,410.547,456.438,322.734,626.535,382.133,483.228,450.548,395.823,258.493,611.86,409.998,427.362,452.789,465.005,412.625,468.857,319.122,610.232,391.084,435.883,386.48,392.099,237.365,509.192,257.35,439.652,352.247,132.313,627.262,351.819,403.76]
mixed_extra_quic = [2.62,107.456,270.629,646.573,347.995,546.967,297.806,544.736,433.475,394.309,472.704,297.744,542.314,446.061,418.263,194.397,636.02,431.687,431.59,456.215,360.474,447.974,427.521,441.211,457.537,354.43,543.312,317.958,374.89,453.189,463.01,320.684,487.993,428.363,453.577,451.991,479.647,295.446,504.897,447.292]
quic_only = [9.607,381.394,453.797,442.691,453.029,451.464,450.889,419.718,398.264,441.603,441.342,455.489,454.278,458.687,460.75,381.394,438.836,452.822,462.794,436.143,429.628,462.16,453.14,440.453,361.837,456.966,457.268,452.88,438.209,432.869,442.206,373.655,441.173,441.909,466.504,413.62,441.517,470.105,453.641,451.161]
```
