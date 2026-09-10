# Confirmed return service: ordinary comparison

Recorded:2026-09-10. Category: existing mixed return-feedback service owner.
**Performance promotion held: useful restricted throughput improves, but
healthy/restored echo tails worsen in the return-cut pair.** The separate
healthy reverse-order check below does not repeat the echo penalty, but loses
throughput. Neither pair is release acceptance or proof of a causal latency
regression. The adverse values remain part of the
decision; no controller, cadence, pool or timeout adjustment follows from an
average alone. CURRENT_CLOSURE_PLAN owns the next discriminator/disposition.

## Contract, forecast and provenance

The [scoped feedback model](SCOPED_ACK_SERVICE_MODEL.md) defines the complete
contract. Prerequisite `cdf0f87` finishes an immutable unsent ACK tail despite
new generations and fairly serves independently due MAX. Its real publisher
REDs expose missing higher sparse facts and starving credit;220focused checks
pass after the correction. The immediate service path retains no extra tail.
Worst-case tail retention is explicit in the model, not a whole-process bound.

Candidate `0cab2b5` keeps ordinary ACK/MAX cadence but initially discovers a
return output through full fanout and an ordered logical-owner Probe/Receipt.
Only a timely exact-output proof selects one output. A frozen current/oldest
successor deadline restores latest AND future full fanout; new DATA cannot
renew it. First/terminal feedback still fans out. Receipt changes publication
routing, never byte receipt, receive credit or estimator authority. Actual
peer MAX gates a reply; neither registry admission nor the reply's carrier is
proof of the locally probed output. Both actors retain a single pending local
write/flush while servicing expiry. Wire15 is an explicit incompatible change.

The forecast was material return-traffic reduction, not the unsafe withholding
diagnostic's promised throughput. Finite failure-detection delay replaces
immediate alternate publication deliberately; no zero-cost equivalence was
claimed. Sparse-tail, real-credit, capacity-one ordering, exact replacement,
terminal and retained-I/O controls give339unique passes across the focused
filter and nine affected groups. Review corrected a shrinking-native-RTT successor deadline
and client skipped-sibling pending flags before ordinary testing. Component
proof does not establish good composed timing. The first focused compile
omitted two exhaustive diagnostic labels; corrected before tests. Optimized
candidate build took64s, with one warning for the now production-unused
`apply_and_write_ready_stream_data_batch` helper. No build ran with this pair.

Root ran frozen ordinary scoped-ACK `b2aa215` then candidate `0cab2b5`, both
ordinary optimized binaries without observers or suppression. Both runners
returned0 in approximately41s. Inputs are the five files per cell under
`./.tmp/reflection/results/mixed-combined-down-confirmed-return-{control,candidate}-0910/`.
The complete raw artifact is
[CONFIRMED_RETURN_ORDINARY_20260910.raw.tar.gz](CONFIRMED_RETURN_ORDINARY_20260910.raw.tar.gz)
(root created and verified its listing:33files, comprising six cells × five
files and three build/mechanism logs; no configs or keys). Build/mechanism logs use
the `confirmed-return-{release-build,focused,affected}-0910.log` names under
`./.tmp/reflection/`. No README/PERFORMANCE claim is changed here.

## Effective profile and accounting boundaries

One40s HTTP download runs beside serial64B echoes at nominal500ms cadence,
3s timeout. Three TCP carriers plus QUIC share one cut. All82service samples
verify DOWN500Mbps, UP500→10→500Mbps,30/70ms delay, no configured random loss,
zero jitter and no blackhole. HTB rate equals ceil; burst/cburst65,536B;
netem limit8,192packets. Different native histories are not packet-identical
traces despite the matching profile. All class drop deltas are0.

| Cell | First UP10 sample, s | First UP500 restored sample, s | Whole sampled interval, s |
|---|---:|---:|---|
| Control | 15.002744 | 25.003841 | 0.000055–40.006570 |
| Candidate | 15.001739 | 25.002765 | 0.000054–40.004827 |

Probe phases use nominal probe-clock bins/attempt starts. The runner starts its
own monotonic clock after launching the probe, without a shared wall anchor;
cached management timestamps are not retrieval timestamps, and native/router
commands are sampled serially. Nearby service rows give phase context, not an
exact per-echo queue/packet join. Cost windows use their own elapsed endpoints.
Count one HTB child per direction, never parent plus child/netem. Kernel class
packet units are offload-sensitive, not exact physical packets or codec frames.

## Complete user outcomes

Both status values are `ok`, HTTP200, with declared8GiB content length. Each
is one intentionally duration-stopped **partial** response and zero completed
full objects. Both probe stderr files are empty. No recorded echo attempt
fails, disconnects or mismatches; the worker is serial, so slow replies reduce
attempt count rather than creating unrecorded fixed-schedule failed slots.

| Cell | Body bytes / elapsed s | Whole Mbps | First body, s | Echo successes / failures |
|---|---|---:|---:|---|
| Control | 1,682,953,151 /40.001491 | 336.578 | 0.616126 | 79 /0 |
| Candidate | 1,728,165,454 /40.000130 | 345.632 | 0.584589 | 78 /0 |

| Cell | Max body gap, s | Gap interval, s | Counter endpoints, B | Echo p50 / p95 / max, ms | Max success spacing, s |
|---|---:|---|---|---|---:|
| Control | 0.502562 | 16.876591–17.379154 | 791,394,529–791,397,129 | 261.029 /465.621 /955.674 | 0.957387 |
| Candidate | 0.452162 | 16.946945–17.399108 | 769,576,914–769,579,514 | 283.477 /567.032 /964.550 | 1.136857 |

Phase entries below are body raw-bin mean Mbps and echo
`attempt count / p50 / p95 / max ms`. Quantiles use the probe's sorted
`round((n−1)*rank)` index, not interpolated percentiles.

| Phase | Control body / echo | Candidate body / echo |
|---|---|---|
| 0–5s | 290.214;10 /168.799 /347.040 /347.040 | 287.676;10 /201.463 /576.807 /576.807 |
| 5–15s | 440.255;20 /276.969 /467.054 /560.703 | 420.434;19 /282.400 /520.893 /964.550 |
| 15–25s | 180.238;19 /353.608 /498.580 /955.674 | 226.058;20 /317.847 /553.990 /567.032 |
| 25–40s | 387.140;30 /232.256 /341.525 /452.349 | 394.795;29 /283.706 /731.948 /805.945 |

Restricted goodput rises25.42% and worst echo improves there, but healthy
goodput falls4.50% and its worst echo rises560.703→964.550ms. The candidate
whole maximum is11.103049–12.067599s, before restriction; restored large echoes
are28.196287–28.928235s and37.432181–38.238126s. Thus its adverse timing cannot
be assigned only to the restriction transition. Candidate attempt48 crosses
25s and attempt77 finishes at40.017880s; both retain start-phase membership.
Control has no phase-boundary-crossing attempts.

Every raw one-second body bin follows. These are buffered application-read
bins, not instantaneous physical-link capacity; values above500Mbps are not
marketed as link rates. No optional trim is applied.

```text
second control candidate (Mbps)
 0       1.049     2.620
 1     161.865   129.763
 2     218.104   233.116
 3     672.438   685.589
 4     397.614   387.294
 5     513.275   529.103
 6     346.957   420.191
 7     563.573   351.957
 8     366.399   389.725
 9     388.918   505.983
10     521.888   347.902
11     327.191   385.021
12     502.696   449.967
13     387.748   413.175
14     483.908   411.316
15     300.371   350.315
16     177.161   163.578
17     108.395    92.078
18     278.246   432.050
19     131.654   215.654
20     185.043   184.597
21     135.831   258.698
22     108.134   167.548
23     247.665   127.594
24     129.884   268.467
25     301.936   322.028
26     400.469   409.295
27     345.886   443.146
28     476.452   358.982
29     349.725   420.041
30     391.856   293.458
31     376.888   491.570
32     373.791   367.433
33     382.367   411.722
34     429.083   463.555
35     246.743   442.480
36     484.343   402.446
37     423.558   374.136
38     379.160   359.462
39     444.836   362.171
```

## Traffic, queues and resources

| Cell | DOWN bytes / packet units | UP bytes / packet units | DOWN / UP peak backlog, B | DOWN / UP drops |
|---|---|---|---|---|
| Control | 2,087,452,769 /1,534,719 | 76,760,209 /692,732 | 17,126,272 /621,948 | 0 /0 |
| Candidate | 2,175,338,194 /1,584,493 | 64,383,835 /593,512 | 18,264,802 /656,669 | 0 /0 |

Whole return class bytes fall16.12% and packet units14.32%, while useful body
bytes rise2.69%. This ordinary build has no per-kind encoding observer: do not
assign all savings to ACK, MAX or Probe overhead, or infer actual selected
outputs from these totals. Forward class bytes rise4.21%; traffic is not
universally lower.

Strict-interior cost windows use rows2–14,16–24 and26–39, not probe bins:

| Cell / phase | Exact sampled interval, s | DOWN bytes / Mbps | UP bytes / Mbps | UP backlog min / median / max, B |
|---|---|---|---|---|
| Control healthy | 2.000280–14.002629 | 744,431,009 /496.190 | 17,532,552 /11.686 | 65,379 /106,185 /162,215 |
| Candidate healthy | 2.000269–14.001625 | 737,983,233 /491.933 | 16,710,205 /11.139 | 44,656 /99,141 /192,505 |
| Control restricted | 16.002859–24.003732 | 239,929,004 /239.903 | 9,994,343 /9.993 | 97,153 /410,316 /621,948 |
| Candidate restricted | 16.001853–24.002659 | 309,859,528 /309.828 | 9,690,016 /9.689 | 46,488 /347,614 /654,584 |
| Control restored | 26.003949–39.006465 | 801,395,829 /493.071 | 39,477,979 /24.289 | 32,211 /213,627 /247,693 |
| Candidate restored | 26.002873–39.004711 | 804,726,688 /495.146 | 29,094,110 /17.902 | 72,109 /182,518 /284,711 |

The restricted return remains near its10Mbps service rate; lower median
backlog does not eliminate its burst pressure. Both greatest body gaps span
the nominal17s region. Service rows16/17/18 show UP backlog respectively
621,948/97,153/462,082B for control and654,584/46,488/203,634B for candidate;
these are phase context, not proof the stalled body byte sat in that queue.

| Cell | Server RSS peak / final, KiB | Client RSS peak / final, KiB | Server / client peak reported %CPU |
|---|---|---|---|
| Control | 446,888 /446,888 | 92,932 /79,980 | 190 /100 |
| Candidate | 429,784 /403,832 | 94,528 /94,528 | 196 /93.1 |

Each of41samples contains one MPP process per role. `%CPU` is `ps` lifetime
average multicore utilization, not instantaneous saturation. No management
retrieval error or admission rejection is recorded. Final snapshots show
fouractive carriers and two logical flows, not a post-teardown leak check.
Both cells log end-of-workload client reset, server RemoteClosed and H3_NO_ERROR
connection closure. These are retained teardown observations, not hidden echo
failures or proof of a live-state failure; both echo series have zero failures.

## Existing native evidence and attribution limit

All sampled TCP sockets report kernel `bbr`. Three established tuples per role
remain stable through each interior window. The entries below are medians of
39/27/42socket observations for TCP and13/9/14cached QUIC observations, not
independent experiments or exact one-way latency.

| Phase | Server TCP RTT control→candidate, ms | Server QUIC RTT, ms | Client TCP RTT, ms | Client QUIC RTT, ms |
|---|---|---|---|---|
| Healthy rows2–14 | 264.523→239.162 | 234.571→266.494 | 269.978→256.294 | 241.724→275.353 |
| Restricted rows16–24 | 385.943→313.079 | 403.285→300.216 | 353.133→347.187 | 396.682→340.529 |
| Restored rows26–39 | 218.463→264.959 | 226.395→287.999 | 222.714→283.795 | 214.591→290.441 |

Server BBR min-RTT medians stay near100ms, while smoothed RTT rises under load.
Restored aggregate server TCP Send-Q peak is10,637,286→15,311,392B and notsent
peak5,459,522→8,211,016B. QUIC flight peak13,456,442→14,518,548B. Restored DOWN
backlog median is10,160,770→12,890,770B, peak15,268,039→18,264,802B. At a
constant500Mbps, that2.73MB median difference is43.68ms drain arithmetic; it
does not measure a particular echo's delay or explain its full tail.

Candidate service samples around its11.103–12.068s echo show DOWN backlogs
9,392,296B at11.001285s and9,887,506B at12.001400s, versus control4,836,146B
and5,395,451B near its own11.003–11.563s echo. Near the candidate's37.432–38.238s
echo, DOWN backlog is16,101,352B/12,890,770B at37.004150/38.004269s; server QUIC
RTT is311.871/321.152ms. Common queue/native service remains relevant, but
unmatched times, cached sampling, unknown echo winner and unavailable proof
events prevent assigning an individual critical wait to route expiry, receipt
processing or a particular native queue. This pair establishes a real cost/
throughput change with adverse composed timing, not its precise mechanism.

## Complete echo sequence

All157attempts succeeded. Rows are ordinal within each cell, not matched
simultaneous requests. Entries are `start_s–end_s (latency_ms)` rounded only
for display; full precision and all outcomes remain in each raw `probe.json`.

```text
 i       control                            candidate
 0   .105379–.207308 (101.929)          .104686–.205216 (100.530)
 1   .500407–.616065 (115.658)          .500617–.684953 (184.335)
 2  1.000496–1.169295 (168.799)        1.000697–1.264548 (263.851)
 3  1.500562–1.802373 (301.812)        1.500776–2.077583 (576.807)
 4  2.000649–2.336423 (335.774)        2.077609–2.466841 (389.233)
 5  2.500727–2.814695 (313.968)        2.577680–2.779143 (201.463)
 6  3.000783–3.142289 (141.506)        3.077759–3.226462 (148.703)
 7  3.501025–3.635880 (134.855)        3.577837–3.678542 (100.705)
 8  4.001105–4.348145 (347.040)        4.080358–4.345118 (264.760)
 9  4.501183–4.846735 (345.553)        4.580450–4.937929 (357.479)
10  5.001254–5.269549 (268.295)        5.080513–5.362913 (282.400)
11  5.501332–5.813739 (312.407)        5.580590–5.856918 (276.328)
12  6.001407–6.325616 (324.209)        6.080694–6.364170 (283.477)
13  6.501498–6.906184 (404.686)        6.580781–6.856840 (276.059)
14  7.001613–7.252483 (250.870)        7.080849–7.601742 (520.893)
15  7.501693–7.777557 (275.864)        7.601758–8.023097 (421.339)
16  8.001765–8.301271 (299.505)        8.101843–8.423992 (322.149)
17  8.501845–8.787084 (285.239)        8.601954–8.895782 (293.828)
18  9.001944–9.142794 (140.850)        9.102006–9.262171 (160.165)
19  9.501972–9.681150 (179.178)        9.602096–9.845496 (243.401)
20 10.002060–10.195418 (193.359)      10.102196–10.282389 (180.192)
21 10.502619–10.711367 (208.747)      10.602265–10.930741 (328.476)
22 11.002692–11.563394 (560.703)      11.103049–12.067599 (964.550)
23 11.563420–11.665857 (102.436)      12.067617–12.314025 (246.408)
24 12.063514–12.240831 (177.317)      12.567696–12.948104 (380.408)
25 12.563584–12.726316 (162.732)      13.067939–13.348271 (280.332)
26 13.063680–13.530733 (467.054)      13.568009–13.846503 (278.495)
27 13.564100–13.841070 (276.969)      14.068083–14.464228 (396.145)
28 14.064715–14.375359 (310.644)      14.568155–14.847067 (278.911)
29 14.564799–14.872434 (307.635)      15.068231–15.537249 (469.018)
30 15.064871–15.368114 (303.243)      15.568310–16.025609 (457.299)
31 15.564959–16.063539 (498.580)      16.069116–16.623106 (553.990)
32 16.065252–17.020926 (955.674)      16.623125–17.190157 (567.032)
33 17.020952–17.415103 (394.151)      17.190209–17.418954 (228.745)
34 17.520992–17.734522 (213.530)      17.690307–17.812140 (121.833)
35 18.021064–18.318144 (297.080)      18.190414–18.292401 (101.987)
36 18.521154–18.925817 (404.663)      18.690481–18.865016 (174.536)
37 19.021189–19.407873 (386.684)      19.193569–19.487000 (293.431)
38 19.521266–19.912256 (390.990)      19.693645–20.052496 (358.851)
39 20.021353–20.428374 (407.021)      20.197377–20.555618 (358.241)
40 20.521432–20.942142 (420.710)      20.694009–21.070800 (376.791)
41 21.021551–21.375159 (353.608)      21.194083–21.573012 (378.929)
42 21.521602–21.874947 (353.345)      21.694153–21.997637 (303.484)
43 22.021689–22.292028 (270.339)      22.194229–22.461055 (266.826)
44 22.521758–22.987378 (465.621)      22.694635–22.931776 (237.141)
45 23.021852–23.127080 (105.228)      23.194794–23.365292 (170.499)
46 23.521927–23.758502 (236.576)      23.694904–23.797812 (102.908)
47 24.022021–24.325878 (303.857)      24.195438–24.531144 (335.706)
48 24.522095–24.836667 (314.572)      24.695714–25.013561 (317.847)
49 25.022209–25.124361 (102.152)      25.195786–25.296533 (100.747)
50 25.522553–25.623821 (101.268)      25.695872–25.796591 (100.719)
51 26.022632–26.136057 (113.425)      26.195932–26.412014 (216.082)
52 26.522715–26.657717 (135.002)      26.696005–26.947616 (251.612)
53 27.022871–27.276143 (253.272)      27.196138–27.554204 (358.065)
54 27.522935–27.756986 (234.051)      27.696209–28.041052 (344.843)
55 28.023016–28.254283 (231.267)      28.196287–28.928235 (731.948)
56 28.523112–28.723260 (200.148)      28.928250–29.211743 (283.493)
57 29.023188–29.261912 (238.724)      29.429143–29.689117 (259.974)
58 29.523274–29.779187 (255.912)      29.929265–30.160687 (231.421)
59 30.023397–30.185063 (161.666)      30.429355–30.549923 (120.568)
60 30.523894–30.701764 (177.870)      30.929408–31.141123 (211.716)
61 31.024230–31.233896 (209.666)      31.429459–31.674553 (245.093)
62 31.524183–31.760101 (235.917)      31.929574–32.213280 (283.706)
63 32.024259–32.269172 (244.913)      32.429645–32.754161 (324.517)
64 32.524327–32.788860 (264.533)      32.929978–33.234590 (304.611)
65 33.024426–33.285455 (261.029)      33.430057–33.711370 (281.313)
66 33.524502–33.775485 (250.983)      33.930146–34.407302 (477.156)
67 34.024528–34.476877 (452.349)      34.430229–34.706519 (276.290)
68 34.525762–34.835244 (309.482)      34.930773–35.217631 (286.858)
69 35.025834–35.258090 (232.256)      35.430864–35.873369 (442.505)
70 35.525930–35.717173 (191.244)      35.930982–36.037392 (106.409)
71 36.026002–36.216230 (190.228)      36.431076–36.722979 (291.903)
72 36.526102–36.754505 (228.404)      36.931136–37.306030 (374.894)
73 37.027125–37.238293 (211.167)      37.432181–38.238126 (805.945)
74 37.527336–37.735280 (207.944)      38.238147–38.556483 (318.336)
75 38.027414–38.294528 (267.114)      38.738225–39.091980 (353.755)
76 38.527487–38.771398 (243.911)      39.240002–39.551981 (311.980)
77 39.027637–39.369163 (341.525)      39.740077–40.017880 (277.803)
78 39.527708–39.823068 (295.360)      —
```


## Separate healthy reverse-order discriminator

The predeclared follow-up runs candidate then control, unchanged binaries and
ordinary policy, using the existing `healthy`25s workload. All52service samples
verify constant500/500Mbps,30/70ms delay, zero jitter/configured loss/blackhole,
HTB burst/cburst65,536B and netem limit8,192packets. This is a separate pair,
not the restoration phase above or a repeat selected to replace it. Directories
are `./.tmp/reflection/results/mixed-healthy-down-confirmed-return-healthy-{candidate,control}-0910/`;
all99echo attempts and full precision remain in their raw artifacts.
Both runners complete; both probes are `ok`, HTTP200, one partial8GiB response
and zero full-object completions. Probe stderr is empty and no echo fails.

| Cell | Body bytes / elapsed s | Whole Mbps | First body, s | Echo successes / failures |
|---|---|---:|---:|---|
| Control | 1,225,239,946 /25.000498 | 392.069 | 0.584016 | 49 /0 |
| Candidate | 1,182,578,412 /25.000154 | 378.423 | 0.610712 | 50 /0 |

| Cell | Maximum gap / interval, s | Body endpoints, B | Echo p50 / p95 / max, ms | Max success spacing, s |
|---|---|---|---|---:|
| Control | 0.277598 /0.584016–0.861614 | 58,192–123,728 | 348.907 /687.778 /781.019 | 0.932191 |
| Candidate | 0.271791 /0.610712–0.882503 | 58,192–123,728 | 247.177 /426.991 /538.751 | 0.824128 |

| Phase | Control body Mbps; echo count / p50 / p95 / max, ms | Candidate body Mbps; echo count / p50 / p95 / max, ms |
|---|---|---|
| 0–5s | 294.915;10 /189.787 /651.171 /651.171 | 288.804;10 /140.793 /399.525 /399.525 |
| 5–15s | 423.719;19 /319.812 /707.589 /781.019 | 423.064;20 /250.934 /408.567 /412.088 |
| 15–25s | 409.006;20 /399.143 /521.400 /687.778 | 378.545;20 /254.981 /472.901 /538.751 |

The control worst echo is11.228036–12.009055s; candidate worst is
15.505749–16.044500s. Control attempts28/48 cross15s/25s, with the last ending
25.332085s; candidate has no boundary crossing. These remain start-phase
members, not omitted tails. Whole candidate throughput is3.48% lower; late
15–25s throughput is7.45% lower. Thus better echo latency is not equal-work
proof, and the original pair's adverse tails are not erased.

| Cell | Sampled interval, s | DOWN bytes / packet units | UP bytes / packet units | DOWN / UP peak backlog, B |
|---|---|---|---|---|
| Control | 0.000050–25.003788 | 1,484,862,518 /1,081,163 | 48,116,389 /422,914 | 25,476,068 /250,878 |
| Candidate | 0.000054–25.003258 | 1,467,442,587 /1,044,451 | 40,771,023 /379,033 | 26,212,484 /247,986 |

Both directions have zero class drop deltas. DOWN backlog median falls
13,553,970→9,127,190B; UP106,925→88,333B, alongside15.27% fewer return class
bytes. No per-kind/selection observer exists in these ordinary captures.
No management retrieval error or admission rejection is recorded. Server RSS
peak/final is330,060/325,420→355,376/355,376KiB; client93,992/93,992→89,484/79,564KiB.
Lifetime reported peak/final CPU is server188→189%, client101→97.7%.
End-of-workload client reset/BrokenPipe, server RemoteClosed and H3_NO_ERROR
warnings remain in both logs; zero echo failures is not an error-free teardown
or post-teardown reclamation claim.

The discriminator does **not** show a consistently adverse echo effect across
pairs. It also does not establish safe improvement: workload duration differs,
healthy throughput is lower, actual native queues/placement differ and no
selection/deadline trace identifies each critical episode. Continue only with
the separately declared affected gate; do not tune a parameter to rescue this
mixed result or declare practical promotion from the restricted gain.

All50healthy body bins (no trim):

```text
second control candidate (Mbps)
 0     2.620     0.990
 1   122.852   220.618
 2   114.723    90.082
 3   715.424   751.993
 4   518.957   380.336
 5   387.494   498.886
 6   474.103   448.938
 7   367.734   389.261
 8   239.871   263.460
 9   613.963   581.932
10   382.604   428.339
11   268.247   280.644
12   586.153   526.896
13   458.294   381.275
14   458.725   431.009
15   402.840   298.488
16   365.129   459.711
17   297.101   279.869
18   560.246   550.751
19   367.445   346.220
20   411.047   405.062
21   441.463   336.371
22   403.916   330.972
23   430.497   410.736
24   410.377   367.265
```

Complete healthy echo sequence, same notation as above:

```text
 i       control                            candidate
 0 0.102827–0.203387 (100.561)        0.102967–0.203523 (100.555)
 1 0.500549–0.684222 (183.674)        0.500470–0.610599 (110.129)
 2 1.000658–1.275952 (275.294)        1.000551–1.247729 (247.177)
 3 1.500714–1.870008 (369.294)        1.500623–1.900148 (399.525)
 4 2.000791–2.651961 (651.171)        2.000722–2.390393 (389.672)
 5 2.651978–2.783669 (131.691)        2.500811–2.789085 (288.275)
 6 3.152055–3.259158 (107.103)        3.000890–3.139479 (138.589)
 7 3.652129–3.937408 (285.279)        3.500958–3.602152 (101.195)
 8 4.152212–4.726728 (574.516)        4.001047–4.141840 (140.793)
 9 4.726743–4.916530 (189.787)        4.501135–4.696359 (195.224)
10 5.226827–5.407589 (180.761)        5.001204–5.236356 (235.152)
11 5.726946–6.028123 (301.177)        5.501249–5.708825 (207.575)
12 6.227014–6.663588 (436.574)        6.001407–6.196947 (195.540)
13 6.727211–7.022082 (294.871)        6.501485–6.750160 (248.675)
14 7.227429–7.592480 (365.051)        7.001570–7.252504 (250.934)
15 7.727514–8.099033 (371.519)        7.501812–7.913901 (412.088)
16 8.227585–8.437357 (209.772)        8.002887–8.240813 (237.926)
17 8.727667–8.988755 (261.088)        8.502974–8.826811 (323.837)
18 9.227720–9.424312 (196.592)        9.003038–9.204163 (201.125)
19 9.727788–9.876180 (148.392)        9.503122–9.678795 (175.673)
20 10.227880–10.547692 (319.812)      10.003209–10.264445 (261.237)
21 10.727957–11.076864 (348.907)      10.503329–10.798913 (295.585)
22 11.228036–12.009055 (781.019)      11.003399–11.243885 (240.486)
23 12.009081–12.147162 (138.081)      11.503474–11.848918 (345.444)
24 12.509383–12.744881 (235.498)      12.003584–12.181775 (178.191)
25 13.009491–13.470542 (461.050)      12.504326–12.804377 (300.051)
26 13.509629–14.217218 (707.589)      13.004949–13.413515 (408.567)
27 14.217259–14.673240 (455.981)      13.505053–13.891001 (385.949)
28 14.717288–15.086783 (369.495)      14.005121–14.253939 (248.817)
29 15.217369–15.609730 (392.360)      14.505593–14.879157 (373.564)
30 15.717910–16.157809 (439.899)      15.005681–15.478582 (472.901)
31 16.217997–16.905775 (687.778)      15.505749–16.044500 (538.751)
32 16.905795–17.405858 (500.063)      16.044521–16.470358 (425.838)
33 17.405875–17.735232 (329.356)      16.544614–16.753622 (209.008)
34 17.906237–18.187523 (281.286)      17.044674–17.147624 (102.950)
35 18.407039–18.826492 (419.453)      17.544761–17.971752 (426.991)
36 18.907132–19.225599 (318.467)      18.044826–18.343061 (298.235)
37 19.410228–19.713444 (303.216)      18.544923–18.846798 (301.876)
38 19.910322–20.309464 (399.143)      19.044995–19.327139 (282.145)
39 20.410394–20.833863 (423.469)      19.545070–19.849632 (304.562)
40 20.910536–21.269760 (359.224)      20.045149–20.289711 (244.562)
41 21.410641–21.753057 (342.416)      20.545243–20.896312 (351.069)
42 21.910729–22.239697 (328.968)      21.045312–21.300293 (254.981)
43 22.410796–22.932196 (521.400)      21.545522–21.744737 (199.215)
44 22.932219–23.189431 (257.211)      22.045600–22.199436 (153.836)
45 23.432320–23.812031 (379.711)      22.545688–22.695374 (149.686)
46 23.932351–24.396745 (464.393)      23.045856–23.232270 (186.415)
47 24.432419–24.900610 (468.192)      23.547005–23.752276 (205.271)
48 24.932733–25.332085 (399.352)      24.047087–24.256296 (209.210)
49 —                                  24.547170–24.778660 (231.490)
```


## Separate UDP-outage ablation

Root ran control then candidate with unchanged ordinary binaries, constant
500/500Mbps,30/70ms delay, zero jitter/configured random loss and no rate
restriction. The sole impairment is the existing bidirectional UDP INPUT drop
rule around30–33s. This tests mixed service under a UDP outage, **not confirmed
loss of the selected feedback output**: no ordinary event records which output
owned each token or whether this stream selected QUIC. Directories are
`./.tmp/reflection/results/mixed-combined-down-confirmed-return-outage-{control,candidate}-0910/`.

All80effective service rows verify the declared rate/delay/queue parameters.
First active-drop samples are30.010868s control/30.003470s candidate; first
cleared samples33.147784s/33.133220s. Both have40samples, ending39.256043s and
39.208515s: class costs omit roughly the last0.75s of the40s body workload.
Do not call these complete40s wire totals. Zero qdisc drops does not mean zero
loss: deliberate iptables discard is a separate domain. The existing serial
sampling and clock caveats remain.

| Cell | Body bytes / elapsed s | Whole Mbps | First body, s | Echo success / failure |
|---|---|---:|---:|---|
| Control | 1,850,127,471 /40.002676 | 370.001 | .585335 | 73 /0 |
| Candidate | 1,876,455,199 /40.000309 | 375.288 | .586200 | 77 /0 |

Both probes are `ok`, HTTP200, one partial8GiB response, zero full completions,
empty stderr and no echo disconnect/mismatch/error.

| Cell | Max body gap / interval, s | Body endpoints, B | Echo p50 / p95 / max, ms | Max success spacing, s |
|---|---|---|---|---:|
| Control | .782613 /37.839216–38.621829 | 1,757,904,028–1,757,969,564 | 286.851 /585.031 /2496.604 | 2.496622 |
| Candidate | .594647 /33.327459–33.922106 | 1,581,763,147–1,581,777,747 | 328.362 /567.280 /1119.198 | 1.489566 |

| Phase | Control body Mbps; echo count / p50 / p95 / max, ms | Candidate body Mbps; echo count / p50 / p95 / max, ms |
|---|---|---|
| 0–5s | 284.192;10 /292.989 /494.920 /494.920 | 276.741;10 /161.863 /729.825 /729.825 |
| 5–15s | 418.624;20 /274.616 /419.859 /585.031 | 427.122;20 /299.877 /559.743 /699.805 |
| 15–25s | 398.509;20 /318.264 /406.446 /408.223 | 422.571;19 /416.148 /548.971 /789.818 |
| 25–30s | 381.301;10 /218.010 /375.223 /375.223 | 433.179;10 /372.371 /552.551 /552.551 |
| 30–33s | 213.988;4 /1179.049 /2496.604 /2496.604 | 202.488;6 /101.146 /567.280 /567.280 |
| 33–40s | 379.962;9 /255.664 /708.445 /708.445 | 336.794;12 /318.311 /511.342 /1119.198 |

Control's worst echo is32.844059–35.340663s, crossing the nominal clear time.
Candidate's worst is34.977896–36.097094s; its later-start phase legitimately
retains this recovery-side tail. Candidate attempts9/29/48/58/64 cross
5/15/25/30/33s; control63 crosses33s. Overall/worst outcomes improve, but
candidate pre-outage echo medians/tails worsen and33–40s body service falls
11.36%. No uniformly faster recovery claim follows.

| Cell | DOWN bytes / packet units | UP bytes / packet units | DOWN / UP peak backlog, B | DOWN / UP qdisc drops |
|---|---|---|---|---|
| Control | 2,304,530,722 /1,678,076 | 77,068,370 /679,399 | 21,238,514 /249,143 | 0 /0 |
| Candidate | 2,295,724,735 /1,619,574 | 51,503,901 /477,995 | 27,040,412 /218,868 | 0 /0 |

Over their slightly different sampled windows, DOWN backlog median is
10,954,487→12,611,656B, UP146,695→86,423B. Server RSS peak/final is
357,308/352,684→314,264/310,952KiB; client104,888/100,132→112,504/112,504KiB.
Reported server CPU peak/final197/176→187/175%; client118/103→85.1/80.8%.
No management error or admission rejection is recorded. Teardown logs client
reset/BrokenPipe, server RemoteClosed and H3_NO_ERROR; no live error is joined
to an echo failure. These final cached samples do not prove reclamation.

QUIC physical instance1 remains labelledActive in every sampled row at both
ends, including during the block; a lifecycle label is not current service
proof. Control server QUIC RTT holds297.523ms and flight roughly13.66MB through
36.256s before changing at37.256s. Candidate server flight reaches26.36MB at
32.133/33.133s then falls to5,808B at34.208s; RTT later rises to504.606ms at
36.208s. This establishes differing native service histories, not which logical
copy/feedback output made an echo succeed. No exact selection/expiry or winner
events exist in the ordinary capture. The ablation therefore cannot close the
selected-output-loss or confirmation-leg-loss proof gate.

All80outage body bins and150actual echo attempts follow; full precision remains
in raw JSON. Recovery bins above500Mbps retain the application-buffering
interpretation.

```text
second control candidate (Mbps)
 0     2.621     2.097
 1   102.418   155.688
 2   274.727   171.442
 3   756.453   731.010
 4   284.739   323.470
 5   529.679   567.139
 6   273.820   346.247
 7   550.729   493.156
 8   359.957   292.650
 9   407.699   577.683
10   442.847   388.218
11   329.510   380.471
12   461.486   399.741
13   424.241   420.118
14   406.273   405.794
15   350.485   430.105
16   467.933   424.880
17   350.793   193.433
18   425.289   633.530
19   465.467   452.923
20   430.176   441.615
21   436.907   396.163
22   351.224   371.958
23   340.034   539.680
24   366.785   341.418
25   416.433   406.289
26   254.078   462.159
27   492.854   436.269
28   100.507   437.491
29   642.634   423.685
30    82.469    54.486
31     5.323     1.168
32   554.172   551.811
33   357.213    15.304
34   100.097   427.806
35   809.158    64.487
36   119.130   416.462
37   536.871   689.961
38     1.495   395.601
39   735.769   347.935
```

Complete outage echo sequence:

```text
 i       control                            candidate
 0 0.102723–0.203269 (100.546)        0.103264–0.203865 (100.601)
 1 0.500974–0.601736 (100.763)        0.500480–0.686782 (186.302)
 2 1.001055–1.233702 (232.647)        1.000576–1.162439 (161.863)
 3 1.501144–1.996065 (494.920)        1.500665–2.230490 (729.825)
 4 2.001215–2.385830 (384.615)        2.230510–2.604726 (374.216)
 5 2.502164–2.726726 (224.562)        2.730618–2.834839 (104.221)
 6 3.002245–3.295234 (292.989)        3.230701–3.339063 (108.362)
 7 3.502326–3.857004 (354.678)        3.730774–3.833448 (102.674)
 8 4.002402–4.383552 (381.150)        4.230824–4.510829 (280.005)
 9 4.502481–4.812140 (309.659)        4.730893–5.032611 (301.718)
10 5.002574–5.213032 (210.458)        5.230974–5.495550 (264.575)
11 5.502653–5.754001 (251.348)        5.731020–6.135616 (404.596)
12 6.002870–6.265691 (262.821)        6.231092–6.530969 (299.877)
13 6.503044–6.795662 (292.618)        6.731203–7.019405 (288.202)
14 7.003123–7.247788 (244.665)        7.231360–7.482918 (251.558)
15 7.503815–7.717531 (213.716)        7.731441–7.912174 (180.732)
16 8.003880–8.232788 (228.908)        8.231559–8.503791 (272.232)
17 8.503957–8.793895 (289.938)        8.731611–8.971120 (239.509)
18 9.004035–9.327463 (323.429)        9.231690–9.574894 (343.204)
19 9.504152–9.778768 (274.616)        9.731828–9.933099 (201.271)
20 10.004212–10.141433 (137.222)      10.231901–10.560263 (328.362)
21 10.504852–10.769732 (264.880)      10.731973–11.059224 (327.252)
22 11.004916–11.375393 (370.477)      11.233938–11.793681 (559.743)
23 11.505351–12.090381 (585.031)      11.793704–12.012975 (219.271)
24 12.090409–12.325043 (234.634)      12.293849–12.600564 (306.715)
25 12.590498–12.777002 (186.504)      12.793898–13.294524 (500.626)
26 13.090561–13.441007 (350.446)      13.294543–13.559186 (264.642)
27 13.590648–14.010507 (419.859)      13.794577–14.494382 (699.805)
28 14.090730–14.466687 (375.958)      14.494402–14.753747 (259.346)
29 14.590928–14.919149 (328.221)      14.994479–15.301505 (307.026)
30 15.091066–15.335391 (244.325)      15.494750–15.945570 (450.820)
31 15.591120–15.722213 (131.093)      15.994835–16.254332 (259.497)
32 16.093437–16.303773 (210.336)      16.494916–16.774096 (279.180)
33 16.593514–16.853710 (260.195)      16.995090–17.209865 (214.775)
34 17.093619–17.380470 (286.851)      17.495153–17.865764 (370.611)
35 17.593725–17.891934 (298.209)      17.995223–18.215301 (220.078)
36 18.093781–18.377461 (283.680)      18.495298–18.826204 (330.907)
37 18.593852–18.913232 (319.380)      18.995373–19.445099 (449.726)
38 19.093936–19.412200 (318.264)      19.495453–19.902825 (407.372)
39 19.594012–19.940165 (346.153)      19.995523–20.499286 (503.763)
40 20.094104–20.363523 (269.419)      20.499350–20.830527 (331.177)
41 20.594189–20.953050 (358.861)      21.002861–21.391330 (388.468)
42 21.096244–21.232469 (136.225)      21.502940–21.952403 (449.463)
43 21.596315–21.873368 (277.053)      22.003018–22.792836 (789.818)
44 22.096392–22.467402 (371.011)      22.792859–23.209007 (416.148)
45 22.596473–23.004697 (408.223)      23.292932–23.841904 (548.971)
46 23.096506–23.475009 (378.503)      23.841928–24.276882 (434.954)
47 23.596582–24.003028 (406.446)      24.342269–24.838608 (496.339)
48 24.096741–24.502008 (405.267)      24.843255–25.310842 (467.587)
49 24.596824–24.947453 (350.629)      25.345646–25.898197 (552.551)
50 25.096901–25.322737 (225.836)      25.898220–26.326086 (427.866)
51 25.596974–25.777718 (180.745)      26.398291–26.856713 (458.422)
52 26.097075–26.332091 (235.016)      26.898380–27.186404 (288.023)
53 26.597139–26.709104 (111.965)      27.398454–27.770825 (372.371)
54 27.097217–27.311582 (214.365)      27.898815–28.141765 (242.951)
55 27.597299–27.886922 (289.623)      28.398891–28.733837 (334.945)
56 28.097373–28.472596 (375.223)      28.898967–29.323621 (424.653)
57 28.597474–28.933465 (335.991)      29.399044–29.907877 (508.833)
58 29.097551–29.208444 (110.893)      29.907895–30.262929 (355.033)
59 29.597622–29.815631 (218.010)      30.407950–30.760290 (352.340)
60 30.097737–30.628622 (530.886)      30.908056–31.009203 (101.146)
61 30.628643–31.664967 (1036.325)     31.408180–31.508892 (100.712)
62 31.664993–32.844042 (1179.049)     31.908259–32.009100 (100.840)
63 32.844059–35.340663 (2496.604)     32.408346–32.975626 (567.280)
64 35.340694–35.542922 (202.228)      32.975652–33.324074 (348.422)
65 35.840769–36.549214 (708.445)      33.475785–33.922064 (446.278)
66 36.549239–36.824847 (275.608)      33.975871–34.076517 (100.646)
67 37.049334–37.345817 (296.483)      34.475997–34.607528 (131.531)
68 37.549429–37.650059 (100.629)      34.977896–36.097094 (1119.198)
69 38.049522–38.283268 (233.746)      36.097120–36.316292 (219.173)
70 38.549613–38.805278 (255.664)      36.597208–36.739341 (142.133)
71 39.049691–39.151594 (101.903)      37.097292–37.510309 (413.017)
72 39.549822–39.935086 (385.264)      37.597370–37.721548 (124.178)
73 —                                  38.097445–38.431480 (334.035)
74 —                                  38.597551–39.108893 (511.342)
75 —                                  39.108912–39.359848 (250.936)
76 —                                  39.608992–39.927303 (318.311)
```
