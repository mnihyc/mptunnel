# Independent return rounds: ordinary DOWN comparison

Recorded:2026-09-10. Category: existing mixed return-service model.
**Useful DOWN improvement in this pair; not blanket non-regression or release
acceptance.** Restricted goodput increases90.41%, whole return bytes decrease
24.46%, and maximum body gap and whole echo p95 improve. The25ms larger worst
echo, longer maximum success spacing, and worse restored echo median/p95 remain
explicit. One pair does not prove either universal improvement or a causal
latency regression. CURRENT_CLOSURE_PLAN owns subsequent gates/disposition.

## Question, correction and provenance

The [transition capture](CONFIRMED_RETURN_OBSERVER_20260910.md) proved a real
policy consequence: after a timely proof returns, its successor is first sent
with a deadline already partly spent. Two prompt100ms exchanges can therefore
clear selection despite a175ms native interval. This is distinct from the
same capture's genuinely slow native/ordered service and queue overflow.

Candidate `364d417` removes that per-fact successor state. Confirmation has
route-liveness authority, not byte-receipt or credit authority. A round's
deadline stays fixed; facts, retries and changed native RTT cannot renew it.
After timely confirmation, newer feedback starts a separate round with its own
frozen native interval. Missing/late proof still restores latest AND future
full fanout. The intentional failure bound now permits the remaining old
round plus one new round after an in-flight pre-failure receipt, plus actual
actor/alternate service. It is not the former per-fact bound and not zero-cost
equivalence to full fanout. ACK/MAX cadence, native intervals/controllers,
stream credit, admission and receipt reclamation are unchanged.

The predeclared forecast was fewer impossible proof expiries, potentially
materially reduced redundant return traffic; no numeric Mbps gain was promised.
The actual-policy RED failed its intended second-receipt assertion while six
old checks passed. After correction,52feedback checks pass, as do affected
stream/client/server groups290/32/92 (overlapping, not an additive unique count).
The real client check also verifies a next-round wake with newer MAX immediately
after receipt. Source/model review preceded ordinary testing. Optimized build
took1m05s, retaining the existing unused
`apply_and_write_ready_stream_data_batch` warning. Logs are the
`return-round-{model-red,model-green,model-adapter-green,stream-controls,client-controls,server-controls,release-build}-0910.log`
files under `./.tmp/reflection/`.

Parent ran frozen ordinary `0cab2b5` CONTROL then ordinary `364d417` CANDIDATE.
Both runners completed with exit0; candidate runner elapsed41.006s. No diagnostic
feature, runtime/profile adjustment or build ran during either cell. Inputs are
the five files per cell under
`./.tmp/reflection/results/mixed-combined-down-return-round-{control,candidate}-0910/`.
All raw attempts, body bins and service rows remain the primary evidence. No
README/PERFORMANCE change, release or comparison with a favorable older control
is implied. The corresponding UP mirror is a separate gate, not part of this
DOWN report.

## Matched profile and observation boundaries

Both41-row service captures verify the intended shared cut: three TCP carriers
plus QUIC; DOWN500Mbps, UP500→10→500Mbps,30/70ms delay, zero configured random
loss/jitter and no blackhole. HTB rate equals ceil, burst/cburst65,536B and
netem limit8,192packets. Actual class and qdisc drop deltas are0in both cells.
Equal inputs are not packet-identical native histories or allocation outcomes.

| Cell | Service span, s | First UP10 sample, s | First restored UP500 sample, s |
|---|---|---:|---:|
| Control | 0.000054–40.006365 | 15.001974 | 25.003078 |
| Candidate | 0.000054–40.006208 | 15.001786 | 25.004531 |

Probe phases use its own nominal clock, one-second body bins and echo attempt
starts. Router/native commands are sampled serially; cached management Unix
timestamps are not retrieval times. Nearby queue rows provide context, not an
exact per-byte/per-echo residence join. Interior restricted costs use service
rows16–24, entirely within the sampled UP10 interval. Whole class costs use
each capture's actual endpoints, not the slightly different body/echo windows.
Count one HTB child per direction, never parent plus child/netem. Kernel packet
counters are offload-sensitive; these totals do not separate ACK/MAX/proof,
native ACK, data or repair traffic.

## Complete user outcomes

Both probes report `ok`, HTTP200, one intentionally duration-stopped partial
8GiB response and zero fully completed objects. Each stderr file is empty.
Every recorded echo succeeds with no disconnect, mismatch or failure. Echoes
are serial64B transactions at nominal500ms cadence with3s timeout: slower
replies change attempt count, not an invented count of failed scheduled slots.

| Metric | Control | Candidate |
|---|---:|---:|
| Body bytes | 1,665,134,743 | 1,944,735,614 |
| Body elapsed, s | 40.000282 | 40.000259 |
| Whole goodput, Mbps | 333.025 | 388.945 |
| First body, s | 0.585577 | 0.582687 |
| Maximum body gap, s | 0.574399 | 0.322268 |
| Gap interval, s | 16.794460–17.368860 | 11.334290–11.656559 |
| Gap byte endpoints | 784,183,193–784,195,193 | 544,332,094–544,397,630 |
| Echo successes / failures | 79 /0 | 80 /0 |
| Echo p50 / p95 / max, ms | 274.087 /602.299 /701.524 | 254.800 /422.633 /726.535 |
| Maximum successful-completion spacing, s | 0.859635 | 0.936363 |

Quantiles throughout use sorted index `round((n−1)*rank)`, without interpolation.
Phase body entries are means of all raw bins; no trimming.

| Probe phase | Control body Mbps; echo n / p50 / p95 / max ms | Candidate body Mbps; echo n / p50 / p95 / max ms |
|---|---|---|
| 0–5s | 292.958;10 /281.317 /602.299 /602.299 | 302.675;10 /262.294 /422.633 /422.633 |
| 5–15s | 431.449;20 /363.902 /569.222 /701.524 | 437.859;20 /342.807 /425.616 /726.535 |
| 15–25s | 202.095;19 /393.144 /637.335 /681.403 | 384.810;20 /210.193 /457.761 /565.052 |
| 25–40s | 368.050;30 /178.677 /347.933 /489.948 | 387.848;30 /207.340 /400.385 /405.815 |

Whole goodput rises16.79%; healthy/restored body means rise1.49%/5.38% and
restricted goodput90.41%. Maximum body gap falls43.89% and whole echo p9529.83%.
This is not a throughput-only win: restricted median/p95/max echo all improve
with more delivered bytes and less return traffic. It is also not improvement
in every dimension. Healthy maximum echo rises701.524→726.535ms; restored
median/p95 rise178.677/347.933→207.340/400.385ms although its maximum falls.
Maximum success spacing increases76.728ms.

Control's worst echo27spans13.704378–14.405902s; candidate's worst21spans
10.503551–11.230086s. Candidate's maximum body gap follows during11.334290–
11.656559s, overlapping echo22but not the worst echo21. These pre-restriction
events cannot be explained solely by the15s shaper change. Control attempts29
and48cross15/25s; candidate49crosses25s. Classification follows attempt start.
Last echoes finish at40.131502/40.001455s and remain included.

At nearby service10–12s, candidate DOWN backlog samples are13.268/0.940/
10.533MB; UP0.122/0.062/0.120MB. They show variable shared service, not the
worst echo's precise queue position. Candidate server QUIC RTT in healthy
samples5–14has median291.847ms/max386.659ms. No ordinary selection/marker
events were enabled, so route residence or its exact causal share is not
reconstructed from these native snapshots.

## Traffic, queue and resource costs

| Whole sampled metric | Control | Candidate |
|---|---:|---:|
| DOWN class bytes | 2,125,154,748 | 2,349,559,778 |
| UP class bytes | 62,130,618 | 46,931,247 |
| DOWN / UP class drop deltas | 0 /0 | 0 /0 |
| Peak DOWN backlog, B | 18,845,916 | 19,821,330 |
| Peak UP backlog, B | 808,549 | 481,536 |
| Client RSS peak / final, KiB | 91,696 /83,672 | 85,344 /85,344 |
| Server RSS peak / final, KiB | 411,364 /366,920 | 386,052 /374,660 |
| Client lifetime ps CPU peak / final, % | 91.6 /91.6 | 90.4 /90.4 |
| Server lifetime ps CPU peak / final, % | 193 /193 | 196 /196 |

| Strict restricted interior, service rows16–24 | Control | Candidate |
|---|---:|---:|
| DOWN bytes | 274,707,494 | 468,725,660 |
| UP bytes | 9,756,479 | 9,857,319 |
| DOWN backlog median / maximum, B | 1,026,580 /1,830,476 | 4,449,994 /11,216,074 |
| UP backlog median / maximum, B | 474,853 /808,549 | 160,132 /481,536 |

Whole return bytes decrease24.46% while body bytes increase16.79%. During the
restricted interior both return cuts remain near their configured service
rate, yet candidate useful delivery is substantially greater and sampled
return queues smaller. More forward queued work and server CPU remain part
of the cost; this is not lower pressure everywhere. Sampled RSS endpoints
are not post-teardown leak/reclamation measurements, and lifetime `ps` CPU
is not exact per-task CPU or proof of CPU saturation.

| QUIC native41-sample metric | Control | Candidate |
|---|---|---|
| Client RTT p50 / p95 / max, ms | 265.575 /492.758 /610.814 | 234.822 /361.281 /413.430 |
| Server RTT p50 / p95 / max, ms | 239.466 /497.811 /604.479 | 235.368 /386.128 /396.702 |
| Server flight p50 / p95 / max, B | 5,222,844 /10,155,288 /17,631,636 | 6,624,024 /12,029,820 /13,444,068 |
| Server restricted RTT p50 / max, ms | 354.968 /604.479 | 250.307 /345.779 |
| Server restored RTT p50 / max, ms | 173.009 /298.048 | 180.892 /264.628 |

Each role has one stable observed QUIC identity/epoch and `active` in all41
samples. Native RTT is carrier-level timing, not logical-feedback selection,
application goodput or exact cause of an echo. Endpoint logs contain only the
Broken pipe/RemoteClosed/H3_NO_ERROR closure warnings after duration teardown;
the probe still records every attempt as successful. No observer counter was
added to infer ACK savings or selected-route service from byte totals.

## Full timing series

All80raw one-second body bins follow (Mbps). Buffered application-read bins
above500Mbps are not instantaneous physical capacity.

```text
second control candidate
 0    2.621    2.620
 1  120.853   99.689
 2  201.205  278.205
 3  721.611  735.118
 4  418.498  397.743
 5  509.474  479.035
 6  310.148  303.415
 7  606.322  487.095
 8  297.689  519.864
 9  556.794  468.793
10  436.872  450.750
11  287.208  300.322
12  546.954  499.680
13  372.085  477.582
14  390.943  392.058
15  271.573  455.177
16  222.616  415.109
17  128.690  450.946
18  282.024  359.502
19  138.746  396.530
20  221.663  371.269
21  190.545  410.364
22  168.726  402.942
23  263.730  375.690
24  132.636  210.567
25  293.515  373.875
26  369.352  424.978
27  351.408  324.620
28  303.516  386.517
29  404.849  376.277
30  380.212  402.247
31  389.682  404.660
32  394.961  373.689
33  359.140  404.435
34  412.136  406.646
35  382.273  397.032
36  377.780  389.119
37  394.883  347.204
38  359.822  349.336
39  347.227  457.089
```

All159echo attempts follow: `start–end seconds / latency ms`, all successful.
Aligned indices are independent serial attempts, not identical packet epochs.
The dash at control79means no such attempt, not a failed or omitted reply.

```text
index control | candidate
 0 0.104221–0.205470/101.249 | 0.102600–0.203191/100.592
 1 0.500310–0.685741/185.431 | 0.500399–0.601086/100.687
 2 1.000401–1.281718/281.317 | 1.000481–1.229300/228.819
 3 1.500478–2.015958/515.480 | 1.500547–1.923180/422.633
 4 2.015973–2.497291/481.317 | 2.000869–2.343708/342.839
 5 2.516070–3.003642/487.572 | 2.500954–2.763248/262.294
 6 3.016146–3.151895/135.749 | 3.000994–3.281888/280.894
 7 3.516239–3.759030/242.792 | 3.501062–3.692232/191.170
 8 4.016367–4.618666/602.299 | 4.001369–4.360700/359.331
 9 4.618683–4.914445/295.761 | 4.501449–4.890876/389.427
10 5.118777–5.481607/362.830 | 5.001527–5.344334/342.807
11 5.618871–6.033617/414.746 | 5.501612–5.897670/396.058
12 6.118893–6.622654/503.762 | 6.002579–6.385924/383.345
13 6.622669–6.862863/240.194 | 6.502647–6.880233/377.587
14 7.122765–7.486668/363.902 | 7.002763–7.289217/286.455
15 7.623024–7.929214/306.190 | 7.502810–7.763366/260.556
16 8.123192–8.448484/325.291 | 8.002915–8.294437/291.521
17 8.623265–9.031183/407.917 | 8.502990–8.860108/357.119
18 9.123346–9.583460/460.114 | 9.003060–9.367723/364.663
19 9.623423–10.133012/509.589 | 9.503396–9.789895/286.499
20 10.133029–10.513846/380.817 | 10.003474–10.293723/290.249
21 10.634556–11.017487/382.931 | 10.503551–11.230086/726.535
22 11.134696–11.425742/291.046 | 11.230108–11.655724/425.616
23 11.634827–11.997574/362.747 | 11.730182–11.836017/105.835
24 12.134970–12.371308/236.338 | 12.230274–12.525420/295.147
25 12.635048–12.994744/359.696 | 12.730337–12.960719/230.382
26 13.135130–13.704352/569.222 | 13.231464–13.551971/320.508
27 13.704378–14.405902/701.524 | 13.731496–14.075397/343.901
28 14.405928–14.680015/274.087 | 14.231574–14.577114/345.540
29 14.906009–15.173214/267.205 | 14.731602–14.963793/232.191
30 15.406030–15.990922/584.892 | 15.231648–15.441841/210.193
31 15.990946–16.628281/637.335 | 15.731736–15.991500/259.764
32 16.628306–17.309709/681.403 | 16.231803–16.460735/228.932
33 17.309771–17.479478/169.707 | 16.731884–16.996646/264.762
34 17.810540–17.920457/109.917 | 17.232420–17.418817/186.397
35 18.310615–18.700660/390.045 | 17.732559–18.060876/328.317
36 18.810685–19.323755/513.070 | 18.232659–18.554438/321.779
37 19.323800–19.802062/478.262 | 18.732734–18.974146/241.411
38 19.823851–20.177692/353.841 | 19.232846–19.367539/134.692
39 20.324209–20.647028/322.820 | 19.732966–19.922338/189.372
40 20.824286–21.189079/364.793 | 20.233555–20.448658/215.103
41 21.325374–21.762509/437.135 | 20.733639–20.939701/206.062
42 21.825457–22.453572/628.115 | 21.233780–21.436700/202.920
43 22.453596–22.867450/413.854 | 21.733862–21.862641/128.779
44 22.953689–23.112342/158.653 | 22.233948–22.394652/160.704
45 23.453889–23.561258/107.369 | 22.734016–22.934023/200.007
46 23.953962–24.347106/393.144 | 23.234732–23.374284/139.552
47 24.454033–24.928753/474.720 | 23.734802–23.941171/206.369
48 24.954108–25.059481/105.373 | 24.234892–24.692653/457.761
49 25.454185–25.555642/101.456 | 24.734990–25.300043/565.052
50 25.956422–26.096626/140.204 | 25.300063–25.705878/405.815
51 26.456471–26.559073/102.601 | 25.800146–25.917427/117.280
52 26.956623–27.058310/101.687 | 26.300275–26.507447/207.171
53 27.456941–27.576213/119.272 | 26.800304–26.951627/151.322
54 27.957269–28.069736/112.467 | 27.300398–27.417733/117.335
55 28.460033–28.563790/103.757 | 27.800462–27.991716/191.254
56 28.960066–29.074493/114.427 | 28.300535–28.572470/271.935
57 29.460146–29.602059/141.912 | 28.800618–29.069908/269.290
58 29.960284–30.106559/146.276 | 29.300692–29.565595/264.903
59 30.460363–30.640202/179.839 | 29.800765–30.055565/254.800
60 30.960430–31.111860/151.430 | 30.300854–30.529239/228.385
61 31.462908–31.667931/205.023 | 30.800901–31.035601/234.700
62 31.962978–32.141654/178.677 | 31.300993–31.536630/235.638
63 32.463296–32.709251/245.955 | 31.801071–32.201456/400.385
64 32.963363–33.213201/249.838 | 32.301405–32.499003/197.598
65 33.463442–33.953390/489.948 | 32.801482–32.962342/160.859
66 33.963553–34.070045/106.492 | 33.301562–33.454191/152.629
67 34.464173–34.717768/253.596 | 33.801604–34.004384/202.780
68 34.964268–35.183404/219.136 | 34.301666–34.581847/280.181
69 35.466759–35.724761/258.003 | 34.801739–35.068250/266.511
70 35.966832–36.210363/243.531 | 35.303326–35.574590/271.264
71 36.466904–36.685930/219.026 | 35.803425–36.092833/289.408
72 36.966986–37.197811/230.825 | 36.303528–36.591890/288.361
73 37.467064–37.777400/310.336 | 36.803559–37.041606/238.047
74 37.967113–38.226169/259.056 | 37.303653–37.510993/207.340
75 38.467205–38.706635/239.430 | 37.803889–37.942475/138.586
76 38.967576–39.315509/347.933 | 38.303980–38.410675/106.694
77 39.467673–39.613596/145.924 | 38.804229–38.949068/144.838
78 39.967744–40.131502/163.758 | 39.304170–39.468350/164.180
79 - | 39.804244–40.001455/197.211
```

## Outcome versus forecast

The tested composition delivers materially more useful DOWN service through
the restricted return cut, with lower return bytes/queue and improved main
gap/loaded-latency measures. This supports pursuing the candidate through its
remaining affected gates. It does not measure how long selection remained
active or establish that every speed gain comes exclusively from that state.
Preserve the adverse healthy maximum/restored echo distribution and changed
native/queue costs; no native interval, controller, cadence or profile rescue
is justified. UP settlement/cost, selected-output failure, broader conditions
and matched competitive evidence remain separate requirements.

## Separate UDP-outage gate: promotion held

Parent subsequently ran the same ordinary binaries, CONTROL0cab2b5 then
CANDIDATE364d417, without source/build/observer changes: the existing40s mixed
DOWN workload at500/500Mbps,30/70ms, zero configured random loss/jitter/QoS,
and whole UDP outage nominally30–33s. This tests retained same-request service
under the deliberately changed failure bound, not a promised throughput gain.
Inputs are the five files per cell at
`./.tmp/reflection/results/mixed-combined-down-return-round-outage-{control,candidate}-0910/`.
The root-created, tar-listed
[ordinary raw archive](RETURN_ROUND_ORDINARY_20260910.raw.tar.gz) retains37files:
six five-file cells plus seven mechanism/build logs. All149outage echo attempts,
including their exact start/end times and outcomes, are preserved there.

**Near-identical mean throughput does not pass this gate.** Candidate maximum
body gap grows41.21% and whole echo p9526.46%; adverse recovery timing holds
promotion pending attribution. Pre-outage echoes, whole median and success
spacing improve, and restored mean body throughput increases. Those benefits
remain explicit; this is not uniformly worse service or proof that the new
proof deadline caused the worst byte gap.

### Effective profile and outcomes

All41rows per cell verify constant500/500Mbps, identical delay/burst/queue
configuration, and zero configured random loss/jitter. Recorded
`udp_blackhole=true` occurs at rows30/31/32 only. Loop timestamps at first
true/first false are30.012378/33.121736s control and30.003614/33.113562s candidate.
These elapsed values precede serial firewall insertion/removal: they are not
exact packet-level onset/end times or proof of identical loss durations. Both
endpoint UDP filters are applied before writing that row. Class/qdisc drop
deltas are0; those counters exclude the intentional firewall drops. Sample
windows end40.146854/40.152832s. The existing independent-clock/cached-snapshot
accounting limits apply.

Both probes are `ok`, HTTP200, one duration-stopped partial8GiB response,
zero completed full objects, and empty stderr. Every recorded echo succeeds,
without mismatch/disconnect. Serial timing explains76versus73attempts, not
three failed replies. Last echoes finish40.423905/40.088084s and are included.
Logs contain the same duration-teardown Broken pipe/RemoteClosed/H3_NO_ERROR
warnings, not recorded probe failures. `bulk_recovery_gap_s=0` is inapplicable
because no probe-side failover timestamp was configured; it is **not** a zero
recovery-time measurement.

| Outage gate metric | Control | Candidate |
|---|---:|---:|
| Body bytes / elapsed s | 1,920,641,848 /40.000041 | 1,917,079,129 /40.000800 |
| Whole Mbps | 384.128 | 383.408 |
| First body, s | 0.578778 | 0.579218 |
| Maximum body gap, s | 0.777480 | 1.097872 |
| Gap interval, s | 36.844780–37.622260 | 36.937321–38.035192 |
| Gap byte endpoints | 1,772,953,880–1,772,965,880 | 1,752,691,977–1,752,703,977 |
| Echo successes / failures | 76 /0 | 73 /0 |
| Echo p50 / p95 / max, ms | 361.186 /697.271 /1,534.222 | 289.874 /881.787 /1,641.343 |
| Maximum success spacing, s | 1.832109 | 1.641366 |

| Nominal phase | Control body Mbps; echo n / p50 / p95 / max ms | Candidate body Mbps; echo n / p50 / p95 / max ms |
|---|---|---|
| 0–5s | 289.942;10 /189.301 /591.943 /591.943 | 241.446;10 /140.245 /355.684 /355.684 |
| 5–30s | 427.605;49 /363.562 /535.254 /825.272 | 422.741;50 /272.360 /373.730 /414.723 |
| 30–33s | 199.915;5 /138.297 /803.284 /803.284 | 194.484;3 /881.787 /1,641.343 /1,641.343 |
| 33–40s | 375.064;12 /390.711 /797.039 /1,534.222 | 425.334;10 /576.151 /1,112.618 /1,112.618 |

Control worst echo70spans36.358219–37.892441s, covering its worst body gap.
Candidate worst echo62spans31.517666–33.159009s; its later1.098s body gap spans
parts of echo67/68/69. Thus one scalar worst latency cannot identify a common
blocking event. Candidate's full37th body bin is0, followed by684.203/630.798Mbps
bins38/39: later draining does not erase the stall. Restored body mean is
13.40%higher, but restored echo median/p95 are worse.

### Native evidence narrows but does not identify the cause

Both roles retain one stable QUIC physical instance/native epoch, labelled
`active` in every sample. Actual server QUIC native acknowledged-byte progress
stops through samples31–35:747,150,701B control and798,970,889B candidate.
Both first increase at sample36, after sampled firewall restoration, while
server flight remains about22.31MB/17.49MB during the plateau. This is a measured
pause in native acknowledgment progress in both models, not merely a stale
displayed rate. Candidate client-side QUIC native ACK progress resumes at35,
control at36: the directions do not show uniformly later candidate recovery.
Native acknowledgment is neither Product ACK/Receipt nor unique application
delivery and cannot locate the missing ordered prefix or selected proof expiry.

| Nearby recovery sample | Control | Candidate |
|---|---|---|
| Row36 elapsed s; server native ACK bytes | 36.146351;747,170,525 | 36.152168;799,016,849 |
| Row37 elapsed s; server native ACK bytes | 37.146463;770,996,393 | 37.152495;801,452,801 |
| Row38 elapsed s; server native ACK bytes | 38.146586;788,722,409 | 38.152597;832,258,433 |
| Row36/37/38 DOWN backlog, B | 24,689,264 /15,112,169 /946,312 | 17,731,142 /30,020,329 /22,987,792 |
| Row36/37/38 UP backlog, B | 47,390 /36,238 /22,501 | 18,276 /38,722 /46,319 |
| Row36/37/38 server QUIC RTT, ms | 226.934 /446.374 /468.815 | 341.003 /421.265 /579.771 |

Native recovery/shared queue pressure is a real competing explanation alongside
route-proof and retained-DATA recovery service. Ordinary logs have no selected
token/deadline, marker application, range receipt, Original/copy dispatch or
exact native packet boundary. Even30MBqueued at500Mbps yields only a conditional
drain calculation, not this byte's measured delay. These records cannot assign
the1.098s gap to a proof round, native PTO/retransmission or retained ordered
prefix, nor prove that failed UDP was the selected return output. The next
bounded capture must join actual owners, not infer causality from scalar labels.

### Costs and complete body series

| Whole sampled outage metric | Control | Candidate |
|---|---:|---:|
| DOWN / UP class bytes | 2,375,540,766 /54,052,353 | 2,368,100,931 /32,897,236 |
| Peak DOWN / UP backlog, B | 24,689,264 /296,077 | 30,066,969 /153,753 |
| Client RSS peak / final, KiB | 100,108 /100,108 | 99,124 /99,124 |
| Server RSS peak / final, KiB | 350,660 /322,160 | 371,304 /351,132 |
| Client lifetime CPU peak / final, % | 97.9 /87.8 | 72.0 /67.7 |
| Server lifetime CPU peak / final, % | 187 /173 | 191 /178 |
| Class bytes rows30→33 DOWN / UP | 155,250,382 /1,580,352 | 154,501,063 /985,012 |
| Class bytes rows33→40 DOWN / UP | 400,154,149 /4,827,337 | 406,279,477 /4,427,638 |

Return bytes fall39.14%, but that does not compensate for worse recovery tails.
Interval costs use sampled rows, not exact firewall activation boundaries; no
firewall-drop packet counts are included. Larger forward queue/server RSS and
lower client CPU are observed costs, not an attributed leak or CPU cause.

All80raw outage body bins (Mbps), untrimmed; all149echo attempts are in the raw
archive linked above. Buffered bins above500Mbps are not physical capacity.

```text
second control candidate
 0    2.620    2.620
 1  106.308  114.558
 2  260.147  159.524
 3  689.002  643.655
 4  391.631  286.874
 5  483.034  596.902
 6  295.076  415.121
 7  557.025  407.152
 8  230.968  307.196
 9  637.302  475.942
10  386.618  430.057
11  348.472  411.486
12  527.460  373.136
13  305.881  410.139
14  548.834  400.638
15  458.899  414.010
16  447.921  334.087
17  287.715  504.133
18  524.665  427.523
19  382.790  430.102
20  400.033  441.929
21  424.100  389.349
22  388.843  461.356
23  475.911  371.020
24  451.692  394.580
25  424.474  466.520
26  465.625  467.086
27  424.317  329.445
28  398.900  471.503
29  413.580  438.103
30   55.022   59.521
31   41.113  138.215
32  503.610  385.717
33   95.581  176.451
34  443.911  575.880
35  696.407  613.813
36  208.142  296.195
37  156.238    0.000
38  269.484  684.203
39  755.686  630.798
```

This gate exposes material recovery harm despite successful transfers and
nearly unchanged whole goodput. It stops promotion, not authorized diagnosis.
Existing snapshots establish native service pauses but cannot choose the
causal owner. Retain this pair while one attribution capture joins proof and
DATA/native timelines; no controller or timeout adjustment rescues the result.
