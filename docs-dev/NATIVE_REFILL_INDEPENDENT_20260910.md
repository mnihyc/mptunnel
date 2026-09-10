# Native refill: one versus two independent 200 Mbps links

Recorded: 2026-09-10. Ordinary working MPP `b0baca2`, unchanged native refill
reserve and dynamic discovery. **Both directions show material, measured
healthy two-link aggregation, but the subsequent asymmetric QoS gate fails
ordered-service expectations; there is no overall performance or release acceptance.**
Useful DOWN rises148.236→302.319Mbps and exact UP169.362→315.648Mbps. Actual
class counters show substantial traffic on each independent cut. All148DOWN
echoes and both upload terminal settlements succeed. Higher CPU costs and an
increased maximum local upload write gap remain explicit.
When only link46UP is reduced to10Mbps, aggregate confirmation averages
16.203Mbps during seconds16–24 despite link47remaining200Mbps; the one-link47
negative control gives185.887Mbps in that interval. Exact eventual completion
and249.391Mbps whole-run aggregate goodput do not erase this sustained collapse.

## Bounded question and topology proof

After the [combined stress matrix](NATIVE_REFILL_COMBINED_20260910.md), this
predeclared gate asks whether ordered useful service consumes two independent
cuts and materially exceeds the one-cut control without failed completion or
large new gaps. The configured sum is400Mbps before overhead, not a promised
goodput. No new runtime fix, reserve, rate hint or controller adjustment is
introduced to obtain aggregation.

The same two endpoint containers attach directly to two existing Docker
networks, named46and47 here. Root verifies destination routes are direct,
without the previous router gateway; that router is idle and not part of the
measured route. These are separate shaped cuts on one VM, not independent
physical hosts: endpoint CPU, target services and host scheduling remain shared.
Existing target processes and default kernel TCP BBR remain unchanged.

| Physical link | Client interface / address | Server interface / address | Configured carriers after startup |
|---|---|---|---|
| 46 | eth0 / subnet46 `.10` | eth1 / subnet46 `.20` | Three TCP plus one QUIC in two-link mode |
| 47 | eth1 / subnet47 `.10` | eth0 / subnet47 `.20` | Three TCP plus one QUIC in either mode |

The one-link `client-mixed.toml` points both transport endpoints to subnet47,
despite its historical `tcp-46`/`quic-46` names. `client-aggregate.toml` points
TCP and QUIC independently to each subnet. Direct socket samples confirm
three TCP connections to47only in the control and three to each subnet in
aggregation; management QUIC endpoints match the configured subnets. The
default TCP pools fill during startup: observed total native path counts grow
to4and8, rather than all carriers being ready at the first sample. Four
configured transport endpoints or eight eventual carriers do **not** mean
four/eight independent physical links.

Both endpoint interfaces are shaped at200Mbps in each direction, with rate
equal to ceil, burst/cburst65536B and netem limit8192. Server egress DOWN delay
is30ms; client egress UP delay is70ms. Jitter, random loss, QoS changes and UDP
outage are disabled for this gate. All169service rows independently verify
these settings on all four endpoint classes; every class drop delta is0.
There is no router row or inherited500Mbps restoration in these captures.

The small runner preparation uses `REFLECTION_LINK_RATE=200mbit` for initial
shaping **and** scheduled restoration, keeps old aggregate defaults when the
override is absent, and rejects aggregate mode with routed topology. This gate
uses `REFLECTION_MIRROR_IMPAIRMENT=1`, `REFLECTION_ROUTED` absent and
`REFLECTION_NO_LOSS`, `REFLECTION_NO_JITTER`, `REFLECTION_NO_QOS` and
`REFLECTION_NO_BLACKHOLE` all set to1. The twelve-cell stress archive was preserved
before these driver/topology changes. No compiler or feature observer runs
with traffic.

Closed raw inputs are five files per cell under
`./.tmp/reflection/results/{mixed,aggregate}-combined-{down,up}-native-refill-independent-200-0910/`.
The driver log is `./.tmp/reflection/native-refill-independent-200-0910.log`.
Order is one-link DOWN, two-link DOWN, one-link UP, two-link UP, once each;
all runners return0. The `combined` filename is a reused scenario label,
not evidence that loss/QoS/outage remained enabled.

The [durable raw archive](NATIVE_REFILL_INDEPENDENT_20260910.raw.tar.gz),
created and tar-listed by root, retains34files: all six cells' five raw files,
both driver logs, and the exact `run.py`/`shape.sh`. The final two cells are
the separate QoS gate below, not additional healthy repeats.

## Complete useful service and timing

| Metric | One link DOWN | Two links DOWN |
|---|---:|---:|
| Body bytes / elapsed, s | 741,189,604 /40.000456 | 1,511,614,600 /40.000474 |
| Whole body Mbps | 148.236232 | 302.319338 |
| First body, s | .607197 | .594420 |
| Maximum read gap, s | .426563 | .352519 |
| Gap interval, s | 29.437653–29.864216 | 12.557916–12.910435 |
| Gap body counters | 561,479,200→561,481,800 | 431,038,388→431,052,988 |
| Successful echoes / all attempts | 68/68 | 80/80 |
| Echo p50 / p95 / max, ms | 477.123 /917.722 /1188.649 | 149.800 /262.265 /419.339 |
| Maximum successful-reply spacing, s | 1.188721 | .714817 |

Both DOWN probes are`ok`, HTTP200, with one intentionally duration-partial8GiB
response, not a full-object completion. Two-link goodput is2.03944× the
one-link result and exceeds one200Mbps cut. Slightly more than2× is not a
superlinear-capacity claim: the one-link realization underuses its cut and
has different native/queue/placement history. Neither is a theoretical optimum.
The worst one-link echo28.671715–29.860364s overlaps its largest read gap;
the worst two-link echo11.504979–11.924318s is separate from its read-gap peak.

| Metric | One link UP | Two links UP |
|---|---:|---:|
| Target-confirmed = locally accepted bytes | 920,518,656 | 1,662,058,496 |
| Exact completed streams | 1/1 | 1/1 |
| Elapsed including settlement, s | 43.481809 | 42.124288 |
| Exact whole Mbps | 169.362 | 315.648 |
| First local write / target confirmation, s | .105406 /.409486 | .109356 /.416032 |
| Maximum target-confirmation gap, s | .410819 | .420405 |
| Maximum local-write gap, s | .316886 | .527719 |

Both UP probes have valid sink-ACK accounting, no probe error, no censorship
and complete terminal settlement. Two links improve exact goodput1.86375×;
they also complete substantially more accepted work sooner. First confirmation
is6.546ms later, maximum confirmation gap9.586ms greater, and maximum local
write gap210.833ms greater. These adverse measurements are retained, not hidden
by the aggregate speed gain. The probe stores no exact upload gap endpoints;
do not infer them from a selected low bin. No concurrent echo test runs in UP.

The40s offered-window phase means use all untrimmed wall-clock bins. DOWN
echo phases group actual request starts; quantiles use sorted index
`round((n−1)*rank)`. Slow serial requests change attempted count, not the
denominator of an assumed80successful-request population.

| Window | One-link DOWN Mbps; echoes / p50 / p95 ms | Two-link DOWN Mbps; echoes / p50 / p95 ms | One-link / two-link UP confirmed Mbps |
|---|---:|---:|---:|
| 0–5s | 73.243;10 /266.209 /621.710 | 225.353;10 /145.729 /292.442 | 73.919 /262.265 |
| 5–15s | 151.722;19 /435.769 /649.163 | 329.005;20 /237.123 /312.058 | 180.479 /313.071 |
| 15–25s | 181.429;17 /509.192 /667.430 | 312.441;20 /160.987 /248.201 | 175.291 /324.333 |
| 25–40s | 148.780;22 /627.542 /1036.085 | 303.440;30 /119.840 /178.944 | 183.393 /316.576 |

## Both independent cuts actually carry traffic

Counters below are each endpoint class's last-minus-first byte totals, mapped
by physical subnet, not configuration names. They include native/protocol/copy
traffic, not per-link unique Product bytes. The unused control subnet's42B is
background traffic, not meaningful aggregation. A large configured rate or
eight visible carriers alone would not establish these results.

| Cell | DOWN46 / DOWN47 class bytes | UP46 / UP47 class bytes | Sample rows / end, s |
|---|---:|---:|---:|
| One-link DOWN | 42 /966,583,093 | 42 /15,585,439 | 41 /40.004665 |
| Two-link DOWN | 935,945,019 /939,106,391 | 24,012,109 /13,564,174 | 41 /40.004575 |
| One-link UP | 42 /22,910,796 | 42 /992,961,590 | 44 /43.005438 |
| Two-link UP | 24,131,090 /18,744,917 | 1,002,930,321 /990,364,097 | 43 /42.007092 |

The aggregate DATA direction uses both cuts substantially and approximately
equally in both directions. Independently, the useful ordered total exceeds
one cut. Together these support real aggregation rather than duplicated
traffic alone. They do not identify the unique-byte share won by each path or
prove that every repair was useful. Late DOWN25→40class deltas are
361,011,155B on46and358,303,136B on47, so dual use is not startup-only.
For UP, the common row0→40data totals are921,093,644B on the one active cut
versus957,907,054/954,068,796B on two cuts; whole totals include unequal
settlement windows and should not be treated as exactly matched durations.

Sequential telemetry timing, buffered delivery and native offload accounting
remain relevant. A one-second application bin above200or400Mbps is a burst
of buffered receipt/confirmation, not proof of sustained physical capacity
above the configured cuts. Use exact whole bytes/time and per-link counters,
not the highest sample or the helper's trimmed mean, for the aggregation claim.

## Resource and native-service tradeoffs

| Sampled cost | One-link DOWN | Two-link DOWN | One-link UP | Two-link UP |
|---|---:|---:|---:|---:|
| Client RSS peak / final, KiB | 87,236 /87,236 | 118,104 /111,820 | 531,216 /531,216 | 373,528 /352,360 |
| Server RSS peak / final, KiB | 379,328 /379,328 | 411,448 /411,448 | 69,260 /69,260 | 81,860 /81,860 |
| Client lifetime CPU peak / final, % | 33.2 /33.2 | 84 /84 | 171 /166 | 196 /193 |
| Server lifetime CPU peak / final, % | 120 /120 | 216 /216 | 52.7 /52.6 | 106 /106 |
| Summed DATA-direction backlog p50 / max, B | 10,877,974 /23,456,580 | 3,857,026 /15,939,874 | 4,237,980 /10,318,502 | 6,242,510 /14,923,800 |
| Summed RETURN-direction backlog p50 / max, B | 26,203 /39,640 | 67,429 /104,395 | 15,125 /39,489 | 25,800 /56,956 |

Backlog sums are formed across the two interfaces within each row before
quantiles/maxima; they are not sums of unrelated individual peaks. On DOWN,
server TCP RTT p50/p95/max falls478.989/896.694/993.760→126.866/259.530/431.293ms;
QUIC falls515.287/897.918/967.241→148.066/286.837/561.321ms. Server TCP
NOTSENT p50/max changes130,240/441,640→0/223,446B. These are sampled native
service facts, pooling all observed sockets including sparse members, not
exact per-echo stage attribution or a universal effect of
adding links. Aggregate UP instead has a larger peak DATA backlog and local
write gap despite its useful speed/memory benefits.

The two-link run does more work and uses more CPU. `ps` percentages are
lifetime averages, not interval CPU or proof of a critical bottleneck; RSS
is sampled process memory, not post-teardown leak evidence. The single VM and
target processes remain shared. All probe stderr files are empty. Recorded
remote-close/`H3_NO_ERROR` messages occur around duration-stop/stream cleanup;
there is no failed probe, byte mismatch or missing upload terminal receipt.

## Full raw timing and disposition

All167raw bins follow:40per DOWN run and44/43UP bins including settlement.
Each value is received/confirmed Mbit per fixed one-second bin, expressed as
Mbps; the final partial interval is not renormalized. DOWN bins cover the40s
offered window, whereas its whole counter can include the last read just
after40s. All148full-precision DOWN echo attempts remain in raw records.

```text
One-link DOWN: 1.048,127.045,85.887,86.508,65.728,93.227,107.147,482.485,153.737,110.861,85.651,94.180,94.207,98.666,197.057,429.656,155.228,83.746,106.763,122.587,113.962,383.351,200.418,100.089,118.489,119.660,124.784,366.677,138.929,44.081,125.370,106.774,130.439,306.281,158.199,148.972,86.112,118.308,112.420,144.691
Two-link DOWN: 2.097,94.964,90.894,338.166,600.642,322.234,327.275,365.040,253.805,385.599,312.884,332.015,22.810,614.976,353.410,364.060,285.316,318.996,296.252,319.454,313.096,328.530,302.713,260.800,335.197,312.093,317.337,282.088,325.053,304.918,322.031,293.927,317.127,289.663,284.227,292.420,301.425,328.679,279.780,300.830
One-link UP: 10.172,114.915,86.175,78.783,79.551,95.516,100.957,371.662,180.783,157.981,149.872,177.785,167.559,229.467,173.207,205.455,137.059,181.325,235.788,168.125,140.606,52.857,149.709,278.530,203.455,174.307,220.414,167.367,148.898,157.971,220.040,131.788,155.210,189.240,267.179,130.599,126.744,200.146,318.988,142.009,129.232,176.072,246.438,134.206
Two-link UP: 13.507,163.290,493.879,315.689,324.962,233.737,286.549,320.200,429.356,341.587,315.457,233.832,257.343,374.365,338.282,353.141,388.914,281.654,260.460,150.149,413.093,302.948,431.494,379.224,282.256,210.419,544.042,323.643,309.401,321.774,220.188,353.420,296.830,331.693,258.447,452.962,350.737,247.378,333.146,194.562,377.153,394.798,90.507
```

The gate supports healthy independent-cut aggregation in both directions,
with exact settlement and materially improved DOWN timing. It does not prove
400Mbps useful service, throughput optimality, aggregation under changing
impairments, native/shared contention neutrality or Cloudflare experience.
No matched external baseline is run in this gate. Retain the UP write-gap,
CPU/return-traffic costs and all prior mixed-mode findings. This healthy result
permitted the next predeclared gate below; it did not promote public documentation,
change the native reserve, erase previous failures or authorize a release.

## Subsequent asymmetric UP QoS gate: sustained ordered collapse

The unchanged ordinary binary and direct two-network topology now run one-link
UP followed by aggregate UP, once each. Only client eth0/link46egress changes
200→10→200Mbps at nominal15–25s; link47and both server return interfaces remain
200Mbps. The one-link control uses47only, so it is a negative control for the
unused46restriction, not another implementation or a reduced-work target.
Loss, jitter and UDP blackout remain disabled. `REFLECTION_NO_QOS` is no longer
set; other healthy profile settings, target services and native policy remain
unchanged. Raw tags are
`{mixed,aggregate}-combined-up-native-refill-independent-qos-0910`, and the
driver log is `./.tmp/reflection/native-refill-independent-qos-0910.log`.
Both runners return0; there is no guard censorship or observer overlay.

The forecast was useful service on the remaining healthy link after an initial
in-flight/reordering transient, not an instantaneous210Mbps guarantee. All87
service rows independently confirm the intended four-class profile, delays,
burst/ceil/limit and zero class/netem drop deltas. The first rows reporting the
46UP restriction/restoration are15.001820/25.002867s in the control and
15.002944/25.004147s in aggregation. These are sampled runner times, not exact
atomic shape-change or application-event timestamps. The control's46UP class
carries only42B over the whole observation; its47service remains substantial.

| Metric | One-link47 negative control | Two-link aggregate |
|---|---:|---:|
| Target-confirmed = locally accepted bytes | 975,437,824 | 1,294,925,824 |
| Exact completed streams / failed | 1/1 /0 | 1/1 /0 |
| Elapsed including settlement, s | 43.867147 | 41.538802 |
| Exact whole Mbps | 177.889 | 249.391 |
| First local write / confirmation, s | .105625 /.409011 | .105451 /.409741 |
| Maximum confirmation / local-write gap, s | .828446 /.519904 | .634001 /.545897 |
| Raw confirmation bins / service rows | 44 /45 | 42 /42 |

Sink accounting is valid and exact in both cases. UP has no concurrent echo
workload, and the probe does not retain upload gap endpoints. Frequent small
confirmations can keep the maximum gap below a second while sustaining very
poor useful speed; neither that gap nor successful terminal settlement clears
the throughput-stability failure.

| Raw wall-clock phase | One-link / aggregate confirmed Mbps |
|---|---:|
| 0–5s startup | 145.766 /262.228 |
| 5–15s before restriction | 185.055 /308.068 |
| 15–25s entire restricted phase | 182.924 /34.658 |
| 16–25s, excluding transition bin15 | 185.887 /16.203 |
| 25–40s after restoration | 178.530 /328.092 |

The aggregate's nine raw bins16–24 range6.573–24.005Mbps, averaging91.28% below
the healthy one-link control. This is sustained ordered-service harm, not one
unlucky first-second sample. The615.580Mbps bin25is buffered confirmation after
restoration, not sustained capacity exceeding400Mbps or compensation for the
preceding user-visible slowdown. These are **untrimmed** bin indices; the
helper's trimmed array starts three seconds later and is not a wall clock.

### Physical and native context, not an invented root cause

| Measured class bytes | One link | Aggregate |
|---|---:|---:|
| Whole UP46 / UP47 | 42 /1,057,560,702 | 745,256,344 /801,341,232 |
| Whole DOWN46 / DOWN47 | 42 /30,565,190 | 15,482,120 /15,889,276 |
| Strict interior rows16→24 UP46 / UP47 | 0 /195,731,308 | 9,992,252 /31,019,459 |
| Same-window UP46 / UP47 class Mbps | 0 /195.711 | 9.991 /31.016 |
| Strict interior rows16→24 DOWN46 / DOWN47 | 0 /5,609,112 | 93,469 /422,177 |

Whole class windows end44.008162s and41.011453s, so they include unequal
settlement observations. Strict interior rates use each run's actual elapsed
row16→24duration. Class traffic includes native/protocol/copy bytes, not unique
Product receipt or exact target confirmation. Sequential role/class reads and
offload accounting prevent per-bin equality claims. In particular, no sampled
counter here assigns the missing useful service to an exact DATA range.

During aggregate rows16–24, healthy47QUIC native ACKs advance35,090,268B on one
unchanged epoch. Its producer stamps advance at every row from16,627,705to
24,628,503us, and sampled RTT p50/max is100.248/100.788ms. Healthy47TCP ACKs
add10,560B. Meanwhile restricted46QUIC ACKs add9,188,256B, with RTT
p50/max2353.675/4240.100ms; its TCP ACKs add608B. The aggregate's47egress
backlog decreases1,756,550→126,868B across these endpoints, while46changes
5,267,604→9,620,178B. In the one-link control,47QUIC/TCP ACKs instead advance
184,951,404/4,364,484B, with QUIC RTT p50/max168.813/214.495ms.

Thus the other cut is not accidentally shaped to10Mbps, and its native path
remains responsive; substantial service is demonstrably available in the
negative control. This supports the existing allocation/ordered-recovery
question, but does not yet prove which owner, blocking prefix, admission or
recovery decision causes the aggregate decline. Native ACKs may cover control
or repeated bytes and do not prove ordered target progress. There is no exact
DSN/winner trace in this ordinary pair. All sampled native path states remain
active within one session per role; aggregate startup still grows2→8carriers,
not eight simultaneous connections from the first sample.

| Sampled cost | One link | Aggregate |
|---|---:|---:|
| Client RSS peak / final, KiB | 563,064 /563,064 | 391,104 /385,112 |
| Server RSS peak / final, KiB | 49,228 /49,228 | 125,516 /125,516 |
| Client lifetime CPU peak / final, % | 189 /180 | 188 /162 |
| Server lifetime CPU peak / final, % | 60.8 /60.8 | 90.4 /77.8 |
| Summed UP backlog p50 / max, B | 4,038,856 /18,608,020 | 5,554,524 /15,059,114 |
| Summed DOWN backlog p50 / max, B | 17,488 /42,069 | 21,614 /53,045 |

All probe stderr and client logs are empty. The control has one server
`H3_NO_ERROR` remote-close message at teardown; aggregate server log is empty.
No byte mismatch, failed terminal settlement or spontaneous reset is reported.
RSS/CPU retain the same sampled/lifetime limitations as the healthy panel.

All86raw confirmation bins, including settlement, are preserved below. Final
partial bins are not renormalized; sums agree with exact bytes within the
stored0.001Mbps rounding precision.

```text
QoS one-link UP: 10.172,108.816,146.705,194.319,268.819,177.830,187.739,195.087,197.701,189.004,141.777,129.019,173.635,216.339,242.420,156.255,185.293,188.142,232.613,190.136,160.788,125.665,188.463,185.258,216.627,179.742,122.311,183.218,159.623,273.370,153.277,210.586,120.815,228.405,184.365,195.271,201.652,114.922,163.544,186.845,250.489,163.437,142.053,160.953
QoS two-link UP: 13.180,156.430,190.937,632.912,317.683,316.906,324.666,299.717,203.084,415.820,381.253,310.207,242.961,246.327,339.739,200.751,8.196,6.573,7.147,13.352,21.267,22.069,21.711,24.005,21.506,615.580,244.748,192.473,521.098,352.558,254.036,181.071,399.803,425.559,344.231,254.380,332.209,221.387,250.461,331.784,465.154,234.477
```

**Disposition:** healthy independent aggregation remains measured, but this
heterogeneous-link gate is not accepted. Stop adding simultaneous impairments
and attribute the existing exact ordered/allocation/recovery owner first.
Do not change a rate hint, reserve, deadline or observation guard to make the
table pass. The whole249Mbps result is not an adequate performance claim for
an upload that spends nine seconds around16Mbps with an unaffected200Mbps cut.
