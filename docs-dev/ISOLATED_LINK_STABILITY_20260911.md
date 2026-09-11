# Protocol-isolated links: bounded healthy and adverse stability evidence

Recorded 2026-09-11. Category: ordinary current-candidate topology/experience
comparison, not published-release nonregression or an allocator change.
Runtime is frozen `04f1e56`, executable
`./.tmp/reflection/bin/clipped-range-20260911/mptunnel`; no diagnostic overlay.
The user excludes a speed deficit within20% or an isolated gap below3s as a
tuning target, without waiving recurring disruption, failed service or costs.

## Scope, inputs and validity

The six valid result directories are under `./.tmp/reflection/results/`:

| Direction / mode | Exact directory |
|---|---|
| DOWN / TCP46 | `aggregate-combined-down-isolated-tcp46-healthy-0911` |
| DOWN / QUIC47 | `aggregate-combined-down-isolated-quic47-healthy-serial-0911` |
| DOWN / split | `aggregate-combined-down-isolated-tcp46-quic47-healthy-serial-0911` |
| UP / TCP46 | `aggregate-combined-up-isolated-tcp46-healthy-0911` |
| UP / QUIC47 | `aggregate-combined-up-isolated-quic47-healthy-0911` |
| UP / split | `aggregate-combined-up-isolated-tcp46-quic47-healthy-0911` |

Each retains `probe.json`, `probe.err`, `service.jsonl`, `client.log` and
`server.log`; UP additionally retains `probe.started`. The two unsuffixed DOWN
directories `aggregate-combined-down-isolated-quic47-healthy-0911` and
`aggregate-combined-down-isolated-tcp46-quic47-healthy-0911` are INVALID because
of the root-recorded execution overlap. They are excluded, not adverse or
favourable repeats. No active stress/UP work was inspected before closure.

Existing configurations are `client-isolated-{tcp46,quic47,tcp46-quic47}.toml`.
Actual management and socket records, not names alone, verify TCP destination
`10.238.46.20:7443` and QUIC destination `10.238.47.20:7443`. The default TCP
pool has three carriers in both TCP-only and split; QUIC has one. All sampled
active counts are3/1/4, respectively, with no suspect/failed path state. Server
configured listeners do not count as active carriers. Split has no TCP47 or
QUIC46 carrier. These are independent shaped cuts, not independent hosts:
endpoint CPU, VM scheduling and target services remain shared.

All250 service rows (DOWN40/40/40, UP44/44/42) retain rate=ceil200Mbps on both
interfaces in both directions. Client eth0/eth1 map to46/47; server eth1/eth0
map to46/47. `MIRROR_IMPAIRMENT=1` means physical UP70ms/DOWN30ms throughout
these healthy cases. Jitter0, no configured loss, no QoS reduction or UDP
blackhole, no router samples, and class/netem drops0 are verified. Startup and
every raw bin remain included. Stable management state is not proof of Product
qualification or current native capacity.

## User service

DOWN is HTTP200, one40s duration-partial8GiB object per cell, not full-object
completion. All80 echoes per cell succeed, with no disconnect or probe error.

| DOWN metric | TCP46 | QUIC47 | Split |
|---|---:|---:|---:|
| Body bytes |896416300|894290312|1546907184|
| Actual body duration,s |40.001117|40.000266|40.000542|
| Whole Mbps |179.278|178.857|309.377|
| First body,s |.581684|.408173|.616430|
| Maximum body gap,s |.400919|.100559|.273874|
| Echo p50/p95/max,ms |225.755/340.989/579.961|103.851/172.520/284.730|315.765/418.214/481.216|
| Maximum successful-echo spacing,s |.875253|.677974|.861226|

Split DOWN is72.57% faster than the best observed singleton; its sustained
5–15/15–25/25–40s means are332.585/330.351/312.111Mbps. Both cuts carry material
traffic; sampled later physical service is about192–197Mbps on EACH cut.
This demonstrates real useful aggregation, not just duplicate traffic. It
does not establish an optimum: split is13.61% below the sum of two sequential
singleton means, which is not a simultaneous oracle or mandatory target.
The higher split echo p95 remains a tradeoff, not a failed echo or new tuning
task. A459.084Mbps receive bin is buffered delivery, not sustained400+ capacity.

UP offers40s of source work, then retains actual terminal settlement. All
three complete exactly1/1, accepted=target-confirmed, no errors or censoring.
There is no concurrent echo in UP; confirmation gaps are not echoed latency.

| UP metric | TCP46 | QUIC47 | Split |
|---|---:|---:|---:|
| Accepted=confirmed bytes |987037696|983891968|973602816|
| Elapsed including settlement,s |43.906054|43.418827|42.758246|
| Whole Mbps |179.845|181.284|182.160|
| First local write/confirmation,s |.106063/.409267|.106426/1.167844|.105523/.409022|
| Maximum write/confirmation gap,s |.427571/.506480|.965711/.340495|.275974/.807813|
| Raw bins/zero bins |44/0|44/1|43/0|

Split UP has no material aggregation gain, but is not slower than the best
singleton. Both interfaces transmit substantial work; unused second capacity
is not inferred from the goodput. No3s write/confirmation gap, failed stream or
sustained late collapse appears. The QUIC zero is its first bin, consistent
with first confirmation1.168s. Do not turn the sum of singleton means into a
promised allocator ceiling, or infer a runtime fix from the missing speedup.

## Complete raw one-second series and phases

No trimming; UP final bins include partial settlement. Labels T/Q/S mean
TCP46/QUIC47/split. Values are Mbps; rows start at the stated zero-based second.

```text
DOWN T 00: 2.097 151.115 177.089 186.779 191.877 190.196 191.362 189.391 191.245 191.485
DOWN T 10: 189.792 131.829 188.392 190.959 190.197 192.010 189.852 191.187 190.958 191.879
DOWN T 20: 189.159 185.718 122.984 191.420 190.486 190.850 191.745 189.403 191.668 190.414
DOWN T 30: 190.236 189.552 128.531 177.048 191.149 189.500 189.319 190.683 190.332 190.916
DOWN Q 00: 9.607 178.162 185.392 182.039 183.596 187.060 184.520 182.547 189.210 158.623
DOWN Q 10: 187.228 187.010 189.157 189.097 186.648 187.628 187.346 160.560 187.030 185.768
DOWN Q 20: 188.707 182.474 187.134 187.171 186.741 186.337 187.652 156.635 186.912 186.980
DOWN Q 30: 186.304 184.761 161.317 189.068 179.890 189.265 184.744 186.240 187.502 180.165
DOWN S 00: .990 192.447 222.726 258.334 389.642 393.406 280.805 318.572 413.486 289.293
DOWN S 10: 253.460 459.084 279.820 261.918 376.010 350.153 302.789 311.343 389.295 331.786
DOWN S 20: 301.099 341.760 344.089 322.964 308.227 332.974 323.018 281.244 297.958 320.977
DOWN S 30: 351.363 332.046 323.873 205.919 309.161 390.308 275.073 356.778 286.304 294.670
UP T 00: 18.229 186.122 197.133 192.937 155.189 200.803 193.461 198.179 193.991 195.557
UP T 10: 154.665 139.460 196.084 195.557 194.515 197.131 157.285 195.036 197.671 195.746
UP T 20: 197.980 118.490 187.498 194.707 159.526 195.942 195.559 196.085 198.657 194.935
UP T 30: 193.780 157.689 118.441 191.129 198.416 195.036 193.986 196.372 191.746 156.262
UP T 40: 195.678 193.319 153.116 167.201
UP Q 00: 0 189.339 187.695 189.364 189.460 187.503 189.364 189.888 189.172 189.512
UP Q 10: 189.076 168.488 189.600 189.652 189.172 189.216 189.563 161.902 189.460 189.563
UP Q 20: 189.741 189.275 168.053 187.075 189.312 189.844 189.504 156.526 170.682 189.172
UP Q 30: 189.320 189.357 189.416 189.556 189.453 189.416 189.172 169.869 178.502 189.453
UP Q 40: 188.847 189.364 188.456 74.783
UP S 00: 10.582 239.888 175.444 257.189 219.915 183.149 171.888 309.404 206.969 210.737
UP S 10: 119.734 111.694 258.514 78.119 37.865 231.591 266.541 191.531 206.305 234.392
UP S 20: 153.874 116.315 264.702 63.761 75.451 76.256 342.397 121.903 267.268 146.651
UP S 30: 153.361 232.755 155.981 225.474 212.610 320.878 179.825 187.271 76.066 78.361
UP S 40: 156.317 298.006 161.889
```

| Untrimmed phase mean,Mbps |0–5|5–15|15–25|25–40|40+|
|---|---:|---:|---:|---:|---:|
| DOWN T |141.791|184.485|183.565|185.423|—|
| DOWN Q |147.759|184.110|184.056|182.251|—|
| DOWN S |212.828|332.585|330.351|312.111|—|
| UP T |149.922|186.227|180.107|184.936|177.329|
| UP Q |151.172|187.143|184.316|183.949|160.363|
| UP S |180.604|168.807|180.446|185.137|205.404|

## Cost and source-pressure limits

Class counters are last-minus-first sampled bytes, not identical to full probe
lifetimes. DATA/RETURN are DOWN/UP for downloads and UP/DOWN for uploads.

| Cell |DATA46/47 B|RETURN46/47 B|DATA summed backlog peak,B|Client/server peak RSS,KiB|Client/server peak lifetime CPU,%|
|---|---:|---:|---:|---:|---:|
| DOWN T |922151789/0|10367092/0|6688068|22564/150196|16.1/31.9|
| DOWN Q |42/925342187|42/18865276|3860496|35796/312176|47.1/122|
| DOWN S |926675365/922386986|17255553/9152873|9402451|71168/303400|60.6/152|
| UP T |1034258183/0|10708982/0|7018598|151744/21964|24.0/15.9|
| UP Q |42/1039459707|42/20838151|5139360|450688/33356|61.0/50.4|
| UP S |506633426/897839738|20931914/13397326|25006568|338408/54644|156/76.0|

Split UP sampled DATA/confirmed is1.442552 versus1.047841TCP and1.056478QUIC;
RETURN/confirmed is.035260 versus.010850/.021179. These are observed cost
ratios, not exact framing/repair/retransmission decomposition. Client peak
native flight is24,639,244B versus7,431,136/5,034,084; Product flight peaks
66,460,076B versus9,180,976/67,108,864. Greater split queues/wire/process cost
is real despite similar useful UP service. Lifetime CPU is not instantaneous
CPU, exclusive hot-path attribution or a per-byte efficiency measurement.

Source/target row subtraction is not an exact retained-debt measurement: the
largest apparent split lead97,575,908B pairs client generation34.987346s with
server33.985346s, and the TCP90,305,988B maximum likewise spans about1s.
Do not call these greater-than-window policy breaches. A near-contemporaneous
QUIC pair at27.973920/27.978920s has source693,560,736B and target626,451,872B,
exactly64MiB apart. Split target still advances2,638,976B over its own
14.981346–15.983346s and3,626,056B over25.981346–26.981346s; those slow sampled
intervals do not establish a3s stall or a specific head/assignment gate.
Final management snapshots precede full settlement; exact probe ACKs, not
unfinished target snapshots, establish all accepted bytes completed.

Repeated client management timestamps occur1/2/6times in DOWN T/Q/S and
1/0/0times in UP T/Q/S; server stamps do not repeat in these cells. Repeated
rows are not extra plateau evidence. In-load RSS is not a post-load leak test.
All probe stderr files are empty; duration-limited DOWN closure warnings and
final QUIC teardown warnings must not be counted as additional failed probes.

Disposition: current protocol isolation provides useful healthy DOWN
aggregation and complete healthy service in both directions, but not a
healthy UP aggregation gain or zero extra cost. No severe user-impact failure
is established by these six cells; no runtime policy follows from the missing
UP speedup alone. Adverse QoS/outage, broader experience and published-v0.4.8
nonregression are separate gates, not silently passed here.

## Adverse split cells and matching controls — 2026-09-11

Category: closed ordinary physical-isolation stress and control evidence.
These eight additional cells are serial, not the invalid overlapping healthy
attempts above. A/B/T use current04f; R is the published v0.4.8 control, not an
intermediate candidate. No runtime or diagnostic change was made for this set.
Aliases preserve the actual path placement, which matters under the cut:

| Alias | Runtime and configured pathset | DOWN / UP exact directory suffix after `aggregate-combined-{direction}-` |
|---|---|---|
| A |current04f, TCP46 + QUIC47|`isolated-tcp46-quic47-qos-outage-0911`|
| B |current04f, TCP47 + QUIC46|`isolated-tcp47-quic46-qos-outage-0911`|
| T |current04f, TCP47 only|`isolated-tcp47-qos-outage-control-0911`|
| R |published v0.4.8, TCP47 + QUIC46|`isolated-tcp47-quic46-qos-outage-release-0911`|

All are under `./.tmp/reflection/results/`. Actual management verifies three
TCP carriers, ordinals1/2/3, in EVERY cell, including the release. A/B/R also
have exactly one QUIC carrier. All sampled physical identities remain fixed;
every sample reports4active paths for A/B/R or3 for T, zero suspect/failed.
TCP endpoints are `tcp://10.238.{46|47}.20:7443`, QUIC the other link's
`udp://10.238.{47|46}.20:7443`. No unlisted same-link protocol is present.
Management-active still does not prove Product qualification or admission.

The existing runner is `run.py aggregate combined {down|up}` with
`REFLECTION_LINK_RATE=200mbit`, `REFLECTION_NO_LOSS=1`,
`REFLECTION_NO_JITTER=1`, management/target observation enabled, and the
corresponding `REFLECTION_CLIENT_CONFIG=client-isolated-{pathset}.toml`.
QoS and blackhole remain enabled. DOWN leaves `REFLECTION_MIRROR_IMPAIRMENT`
unset; UP sets it to1. Config credentials are not reproduced or archived.
The recorded binary selection distinguishes current04f from published R;
runner/configuration support is not a new Product threshold or carrier cap.

Across all333 service rows, both cuts have rate=ceil200Mbps except physical
46 in the bulk direction, reduced to10Mbps during the QoS phase. Physical47
stays200Mbps. Bulk direction has70ms delay and return30ms, jitter0, no random
loss. The later INPUT-rule UDP blackhole affects both links/directions; it is
not a TCP outage. All class/netem drop counters are0, which does NOT negate
the separate iptables DROP interval. T therefore sees the same schedule but
its TCP47 carrier is not directly impaired by either change.

| Cell |Service rows|First QoS10 / restored200 row,s|First UDP blocked / restored row,s|Client/server repeated management stamps|
|---|---:|---:|---:|---:|
| A DOWN |40|15.001689 /25.002769|30.003276 /33.327999|1/0|
| A UP |43|15.046805 /25.047800|30.048322 /33.338357|1/1|
| B DOWN |40|15.055191 /25.109579|30.269739 /33.706486|1/0|
| B UP |44|15.001638 /25.002728|30.003252 /33.324149|1/2|
| T DOWN |40|15.001627 /25.002638|30.003211 /33.220686|2/0|
| T UP |44|15.001592 /25.002606|30.003115 /33.197647|1/0|
| R DOWN |40|15.052127 /25.053154|30.053695 /33.489834|0/0|
| R UP |42|15.042843 /25.043936|30.044470 /33.369618|1/2|

These are driver-loop elapsed stamps for rows carrying the new state, not
nanosecond rule-install times: `elapsed` is read before shaping/iptables
commands and the following telemetry queries. Actual class samples confirm
the state. Management generation has its own clock; repeated timestamps count
as one observation. No row alignment proves an exact source/target head.

### Complete user service, including the released DOWN disconnect

DOWN again reads a40s duration-partial object with HTTP200; it is not an8GiB
completion claim. All current DOWN echoes succeed. UP offers40s then includes
terminal settlement: all four are exactly1/1 complete, accepted=confirmed,
failed streams0 and errors[]. UP has no concurrent echo. All eight stderr
files are empty; that does not erase the released DOWN JSON-reported timeout.

| DOWN metric |A|B|T|R|
|---|---:|---:|---:|---:|
| Body bytes |1133067166|1076684352|895710120|635325486|
| Actual duration,s |40.000429|40.000305|40.000132|40.003385|
| Whole Mbps |226.611|215.335|179.141|127.054|
| First body,s |.618151|.577984|.579219|.442630|
| Maximum read gap,s |1.469751|.327493|.390944|2.184532|
| Actual echo success / failure records |75/0|80/0|80/0|32/43|
| Successful echo p50/p95/max,ms |123.800/758.345/1457.636|158.150/326.850/654.008|223.941/279.989/623.360|101.390/347.811/576.050|
| Maximum successful-echo spacing,s |1.457661|.926587|.916640|.898893|

R DOWN's43failure records are **one actual timeout plus42 subsequent
`unavailable_after_disconnect` records**, not43independent network timeouts.
Last successful attempt31 spans15.504615–16.080665s. Attempt32 starts
16.080685s and ends19.083244s with `io_error`, `timed out`, latency null:
3.002559s actually waited. Attempts33–74, spanning19.083302–39.591781s, are
unavailable because that persistent connection is gone. There is no later
successful or reconnected echo evidence. R's success-only p95 excludes both
the timeout and this unavailable tail and must not be presented as recovery.
A's75attempts are all successful; fewer attempts than80 are not failures.

| UP metric |A|B|T|R|
|---|---:|---:|---:|---:|
| Exact accepted=confirmed bytes |1013710848|957284352|988872704|1098186752|
| Elapsed including settlement,s |43.219201|43.514626|43.910887|41.868553|
| Whole Mbps |187.641|175.993|180.160|209.835|
| First write/confirmation,s |.106472/.409934|.105320/.408727|.105723/.409372|.105214/.412766|
| Maximum write/confirmation gap,s |1.928834/1.972027|3.045357/5.699120|.475849/.454901|2.781784/3.329568|
| Raw bins / zero bins |44/1|44/8|44/0|42/4|

Current B is not a severe whole-mean slowdown versus T, but its5.699s
confirmation gap and five consecutive zero bins20–24 are severe service
disruption. DOWN B's15–25s mean13.381Mbps is92.71% below T's183.595Mbps
under the matching schedule, despite its subsecond maximum individual read
gap. A DOWN likewise has a17–22s five-bin mean17.608Mbps. Whole means and
maximum-gap scalars therefore cannot certify these cells as stable.

Root's separate own-clock UP B join identifies a return hold within the
adverse interval: over a five-second bracket, client successfully delivered
reply prefix stays1119B while server reply reads grow1119→1231B and target
successful writes advance80,143,392B. TCP47 native ACKs advance86,071,574B.
This excludes labelling that entire bracket a forward-service freeze, but
does not identify the missing reply's carrier, queue, timer or decoder owner.
The exact5.699120s probe maximum cannot be assigned endpoints from one-second
management rows alone. Root's bounded diagnostic is separate evidence.

### All334 adverse/control raw confirmation/body bins

Mbps, zero-based starting second; no trimming or omitted startup/settlement.
DOWN has40bins each and only R has zeros(two). UP bins are confirmation
delivery, not instantaneous physical throughput or exact QoS phase accounting.

```text
DOWN A 00: 1.049 186.281 252.183 208.963 298.873 532.018 244.178 298.336 439.574 281.306
DOWN A 10: 286.613 435.095 293.675 261.818 384.252 310.652 88.705 31.748 16.733 11.438
DOWN A 20: 21.096 7.024 426.698 282.060 188.788 188.744 193.750 493.944 280.331 250.452
DOWN A 30: 47.268 190.130 375.562 165.093 187.171 215.792 118.643 85.866 324.563 157.978
UP A 00: 10.699 184.549 215.158 210.212 268.026 235.073 130.185 88.329 296.477 151.402
UP A 10: 205.226 72.352 75.593 116.214 272.615 154.358 246.490 183.379 206.553 126.488
UP A 20: 54.498 0 93.844 93.715 99.501 356.608 536.871 200.470 196.800 171.621
UP A 30: 76.603 22.018 264.768 320.803 211.340 117.354 167.577 378.476 257.354 328.660
UP A 40: 90.842 275.121 259.931 85.528
DOWN B 00: 2.597 192.506 214.294 237.214 470.382 334.163 270.807 376.659 373.047 291.595
DOWN B 10: 331.337 356.625 256.535 370.727 280.520 17.672 14.820 11.630 22.116 13.495
DOWN B 20: 13.088 9.961 9.961 10.582 10.486 544.307 304.071 202.318 279.542 499.312
DOWN B 30: 106.193 158.990 219.867 317.647 193.458 185.085 230.500 200.024 225.082 454.162
UP B 00: 4.957 199.575 179.971 313.288 164.626 210.335 266.575 66.586 252.934 116.680
UP B 10: 267.121 57.816 117.869 319.243 120.298 89.919 176.670 0 0 283.134
UP B 20: 0 0 0 0 0 653.356 508.209 179.750 343.268 206.982
UP B 30: 80.371 96.097 0 9.049 562.798 48.352 199.406 164.963 305.786 339.585
UP B 40: 205.308 135.832 251.471 160.093
DOWN T 00: 2.097 151.925 165.675 189.792 190.317 190.841 192.007 190.724 190.317 189.268
DOWN T 10: 186.646 130.548 192.938 189.268 190.841 190.841 193.156 190.125 190.814 189.268
DOWN T 20: 191.432 147.193 161.766 191.687 189.671 190.978 190.875 189.972 192.061 189.375
DOWN T 30: 192.445 189.924 116.819 191.006 190.087 191.422 189.984 191.900 189.750 189.403
UP T 00: 17.704 193.988 195.035 160.431 188.745 193.462 199.229 200.038 160.673 191.604
UP T 10: 159.143 173.016 195.560 193.987 159.382 197.133 198.642 195.098 194.271 194.227
UP T 20: 194.578 108.985 198.380 195.360 156.762 196.609 194.510 192.938 196.608 195.930
UP T 30: 193.856 192.697 87.556 195.362 194.184 195.145 193.636 195.175 194.958 193.924
UP T 40: 193.178 193.987 146.559 148.735
DOWN R 00: 2.621 144.773 52.953 110.625 127.926 367.526 20.972 314.573 147.442 34.486
DOWN R 10: 243.794 57.147 114.295 340.395 8.796 62.390 52.953 412.090 0 0
DOWN R 20: 1.049 1.168 1.168 601.644 146.090 121.536 250.939 88.197 224.252 129.499
DOWN R 30: 89.653 44.617 123.732 33.554 126.878 108.003 110.100 179.831 18.350 66.060
UP R 00: 11.008 248.276 204.140 510.465 334.210 376.725 359.617 317.522 341.927 347.749
UP R 10: 360.082 235.127 364.053 350.455 355.659 56.527 0 0 0 158.239
UP R 20: 579.768 0 25.785 43.516 45.089 508.988 196.084 99.615 232.879 491.879
UP R 30: 109.243 69.442 56.099 210.764 189.792 132.121 13.107 79.692 67.846 1.189
UP R 40: 52.888 647.928
```

| Untrimmed mean,Mbps |0–5|5–15|15–25|25–30|30–33|33–40|40+|
|---|---:|---:|---:|---:|---:|---:|---:|
| A DOWN |189.470|345.687|138.494|281.444|204.320|179.301|—|
| A UP |177.729|164.347|125.883|292.474|121.130|254.509|177.856|
| B DOWN |223.399|324.202|13.381|365.910|161.683|257.994|—|
| B UP |172.483|179.546|54.972|378.313|58.823|232.848|188.176|
| T DOWN |139.961|184.340|183.595|190.652|166.396|190.507|—|
| T UP |151.181|182.609|183.344|195.319|158.036|194.626|170.615|
| R DOWN |87.780|164.943|127.855|162.885|86.001|91.825|—|
| R UP |261.620|340.892|90.892|305.889|78.261|99.216|350.408|

### Physical/process cost and native-evidence boundaries

First-to-last sampled class byte deltas; DATA/RETURN directions as defined
above. Sample duration differs slightly from probe duration. Ratios include
protocol overhead, retransmission and possible copies; residual is NOT an
exact repair total. Physical backlog is the peak sum across both DATA cuts.

| Cell |DATA46/47 B|RETURN46/47 B|DATA backlog peak,B|Client/server peak RSS,KiB|Client/server peak lifetime CPU,%|Sampled DATA/useful ratio|
|---|---:|---:|---:|---:|---:|---:|
| A DOWN |699500213/644769141|12127608/7838796|12290026|128412/354504|53.8/121|1.186399|
| A UP |604138652/841222687|14196782/11595450|28351438|391104/125520|137/66.4|1.425812|
| B DOWN |667294658/738667538|5592074/14917742|11748316|81864/313896|54.4/131|1.305826|
| B UP |742034382/784604791|9567209/13694376|29156338|376188/97904|139/69.8|1.594760|
| T DOWN |42/929837188|42/10584498|7247522|22064/143236|15.6/30.2|1.038101|
| T UP |42/1040495109|42/11068998|7195904|183784/22448|22.0/14.8|1.052203|
| R DOWN |297003309/891717038|15542283/46510794|14221100|101700/211352|32.2/52.6|1.871041|
| R UP |566425894/943360080|9685957/21276682|14649868|224972/92108|81.5/57.3|1.374799|

Current native telemetry has one stable epoch per actual path in both roles,
no decreasing native sample time or cumulative native ACK count. Published R
has no equivalent `native_delivery` journal fields; missing is not zero or a
reset. It still exposes the older summary/native estimates. Reported peak
sender native flight is13,702,936/32,964,336B for A DOWN/UP,
47,874,162/56,702,084B for B, and7,652,680/7,615,032B for T.
Current sender queue summaries peak521,232/469,376B for A,
556,842/432,120B for B,433,286/463,483B for T. R's reported sender queues
peak91,146,706B DOWN and85,770,666B UP; cross-version representation is not
proven equivalent, so these are not attributed as a newly fixed queue leak.
Current UP Product flight peaks67,043,328B(A),60,111,688B(B),9,437,184B(T).
DOWN summary Product flight0 is not proof of no retained response work.

The long-lived target pid25 keeps the same start identity in each cell. It
already has about704,000KiB swap, unchanged except a4KiB movement in B UP;
only B UP observes one new major fault, other cells0. UP target CPU deltas
are203/174/76/139ticks(A/B/T/R), DOWN2ticks each. This target observer is the
upload sink, not an HTTP8080 or echo10022 ownership trace. Neither historical
swap nor in-load RSS establishes a current stall cause or a post-load leak.
Process CPU is a lifetime percentage, not an instantaneous exclusive owner.

### Bounded conclusion and raw archive

The unaffected TCP47 singleton sustains service under the same schedule,
so shortage on that physical cut alone does not explain B's sustained cut
collapse. The published split also disrupts service: the problem is not
solely introduced by current04f. This does NOT prove global release
nonregression: current B's confirmation maximum5.699s exceeds R's3.330s,
while current DOWN eliminates the observed R echo disconnect and improves
whole goodput. Different cells do not establish the same underlying owner.
Healthy useful DOWN aggregation remains real, but it does not waive these
severe adverse failures. No protocol preference, allocator change, timer
tuning or accepted runtime correction follows from these summaries alone.

Archive `./docs-dev/ISOLATED_LINK_ADVERSE_20260911.raw.tar.gz` contains the
eight exact raw sets:40base files plus four UP `probe.started` files, and
the existing `./.tmp/reflection/run.py` and `shape.sh`. All46regular members
were byte-compared with their source files:15,266,441B uncompressed,
1,903,034B compressed. No binary, secret config, invalid cell, diagnostic
capture or invented driver wrapper is included. The pathset/recipe above
and the archived runner define reproduction; strict driver completion and
teardown must precede starting the next cell.

## Separate diagnostic evidence and remaining raw archives — 2026-09-11

The six VALID healthy cells above are now separately archived as
`./docs-dev/ISOLATED_LINK_HEALTHY_20260911.raw.tar.gz`:33exact raw files plus
run.py/shape.sh,35regular members,10,220,431B raw /1,174,512B compressed,
all bytes verified. Neither invalid overlapping attempt is included.

Three subsequent captures inspect causes; they are NOT ordinary performance
comparisons, favourable replacements, or runtime corrections. Exact directories
under `./.tmp/reflection/results/`, with compact aliases:

| Alias |Directory|Executable provenance|
|---|---|---|
| U1 |`aggregate-combined-up-isolated-tcp47-quic46-return-trace-0911`|existing04f `bin/clipped-prefix-20260911/mptunnel`|
| U2 |`aggregate-combined-up-isolated-tcp47-quic46-gate-trace-0911`|04f + two-file observation-only retained-reply-gate patch|
| D |`aggregate-combined-down-isolated-tcp47-quic46-gate-trace-0911`|same frozen `bin/retained-reply-gate-20260911/mptunnel`|

U2/D add exact prepared response Original identity and actual loop-head
retained-frontier outcomes; they do not alter clocks/admission/queues. The
feature build finished in1m27s; source hooks were reversed after freezing.
U1's older two-file observer provenance is also documented/archived at
`./docs-dev/CLIPPED_RANGE_PREFIX_DIAGNOSTIC_20260911.raw.tar.gz`.

All127service rows retain3TCP47+1QUIC46,4active,0suspect/failed; physical47
stays200Mbps,46is10Mbps only in the bulk-direction QoS phase. Delay is
bulk70ms/return30ms; no random loss/jitter, class/netem drops0. Separate UDP
blackhole remains real. Row-stamp precision limits are the same as above.

| Alias |QoS / restoration,s|UDP block / restoration,s|Useful bytes / duration,s /Mbps|Max confirmation or body gap /write gap,s|
|---|---:|---:|---:|---:|
| U1 |15.226713/25.227805|30.235974/33.460483|961347584/43.214848/177.966|4.145898/1.716059|
| U2 |15.186223/25.187571|30.211168/33.535376|1010368512/43.805337/184.520|1.829351/1.261744|
| D |15.148778/25.149862|30.160365/33.417748|1107899686/40.002581/221.566|.396887/—|

Both uploads complete exactly; D is HTTP200 duration-partial,80/80echoes,
p50/p95/max210.984/327.667/409.982ms. All three probe stderr files are empty.
Combined client/server logs are31,440,280B(U1),71,104,704B(U2),74,015,136B(D).
Client/server peak RSS,KiB, is314928/80312,303048/86212,83836/252776;
peak lifetime CPU,%128/74.6,130/84.1,63.8/114. Summed sampled DATA bytes are
1457942861,1634066664,1300492075; these and heavy logging are measurement
costs, not an exclusive repair total, CPU attribution or ordinary improvement.

U1 exact response range[1078,1092) is committed at Unix1789091895282ms and
released in order on client QUIC at1789091899197ms,3.915s later. All130actual
copy ranges were checked: its only overlapping repair is TCP2/incarnation3,
accepted at1789091899639ms,442ms AFTER release. Queue residence0ms, native
write18us, client TCP authentication30ms later. Thus late delivery AFTER an
accepted TCP copy does not explain this observed head hold. Original QUIC
ownership is inferred from the unique claim, QUIC release and no earlier
overlapping copy; U1 lacks the explicit Original identity added in U2.
ServerF1078 was recorded at1789091896045ms,3.152s before release, so delayed
knowledge of an older head is not the whole cause. This does not make the
entire hold removable, identify a gate, or exactly locate the probe's4.145898s
maximum: its endpoints are not logged.

U2 does NOT reproduce the >3s confirmation hold. Its exact QUIC0/physical1/
incarnation4 head1121 is assigned at1789092253287ms and released at4947ms
within that same1789092250000ms base(+1.660s); head1163 is assigned3903ms and
released5995ms(+2.092s). Respectively2185/841active-head evaluations retain
future deadlines near1789092258267/1789092260200ms, about4.98/6.30s after
assignment, with service0 and capacity_blocked=false. No overlapping copy
is observed for either head. These are future-clock stops BEFORE target/copy
gates, but only for those shorter episodes. The hook covers the actual
loop-head call, not every retained helper caller or the earlier severe U1 hold.

D DOES reproduce sustained low service: raw15–25s mean17.971Mbps,
16–25s14.249Mbps. In the conservative management-clock interior
[1789092794082,1789092801082)ms,687new TCP Originals total12,549,504B and
no new QUIC Original is admitted. Meanwhile765advancing client heads cover
the same byte total, all pre-QoS QUIC Originals assigned at1789092791296–1538ms.
Reorder grows25.955→34.238MB; assigned-minus-server-ACK-frontier repeatedly
equals64MiB. Thus new TCP work exists, but old QUIC-owned ordered debt and
credit constrain useful progress; this is not simply no offering on TCP.

Of395TCP persistent copies/5,041,220B in that interior,390/4,968,220B(98.552%)
are wholly below a strictly earlier client released frontier. The other five
extend at most2600B beyond the latest frontier; all395start bytes had already
been released1–31ms earlier(median19,p9526). No QUIC copy overlaps the entire
head span[573465074,586014578). Example head579050494 has exact QUIC Original
assignment1789092791412ms, QUIC release1789092797104ms(+5.692s), and its
first/only overlapping TCP copy at1789092797122ms,18ms after release.
These are receiver-versus-sender-knowledge comparisons, not ACK authority
violations or proof that every byte/copy is unnecessary.

Crucially, D's4305retained records in that interior are SKIPPED/default
outcomes because ACK-gap evaluation returned a nonempty frame list, even
when it did not queue work:3981with one frame,324with two. Frame count is
not an admission count; these records do not establish queued=0 for all4305.
They are NOT evidence of a future retained clock. Native TCP
ACKs advance343309608→360955512B with fresh sample times; its flight is about
210–216KiB against7.3→7.1MB limit and queue0. QUIC ACKs also advance while
RTT rises about.997→5.762s. This selects old ordered debt/recovery timing for
further model investigation, not a proven corrective algorithm or CC defect.
No DOWN probe wall anchor exists; use the conservative Unix interior above,
not invented exact probe-phase or maximum-gap endpoints.

Diagnostic archive `./docs-dev/ISOLATED_LINK_DIAGNOSTIC_20260911.raw.tar.gz`
contains17raw files for U1/U2/D, exact `retained-reply-gate-0911.patch` and
its build log, `clipped-prefix-observer-0911.patch` and its build log, plus
run.py/shape.sh:23regular members,182,570,325B raw /7,407,634B compressed,
all source bytes verified. No binaries, credentials, ordinary cells or
unrelated captures are included. Diagnostic and ordinary archives remain
separate; no source/runtime/build/lab change was made during this archival task.

## Response uncovered-prefix ordinary trial — ADVERSE / NOT ACCEPTED

Recorded2026-09-11. The isolated response recovery-coverage candidate is
retained at checkpoint `ec8cd2f`; it is NOT an accepted runtime correction.
Root is withdrawing its eight owned runtime/test/RFC files to `dbed0c5`.
No further lab cell is justified by this result alone. This appendix/archive
does not alter source or CURRENT and does not declare the trial's code the
proven cause of every difference between two ordinary realizations.

Candidate: `./.tmp/reflection/results/aggregate-combined-down-isolated-tcp47-quic46-uncovered-prefix-0911/`,
ordinary no-feature executable `bin/response-uncovered-prefix-20260911/mptunnel`,
driver session27582 CLOSED0. Matching baseline is B DOWN above:
`aggregate-combined-down-isolated-tcp47-quic46-qos-outage-0911`.
The real-producer test first failed at the intended second-uncovered-prefix
assertion, after earlier setup controls; it then passed. The final170focused
tests passed. Build finished1m27s. Those mechanism/component results do NOT
override adverse ordinary service. Two old covered-frontier-stop expectations
were migrated to successor/no-overlap/all-covered-wake controls, not silently
waived. The earlier Debug-format compile error is not the intended RED.

| User service |Baseline B|Candidate|
|---|---:|---:|
| HTTP status / partial objects |200 /1|200 /1|
| Body bytes |1076684352|907696120|
| Actual body duration,s |40.000305|40.000731|
| Whole Mbps |215.335|181.536|
| First body,s |.577984|.578617|
| Maximum body gap,s |.327493|3.300801|
| Echo successes/failures |80/0|80/0|
| Echo p50/p95/max,ms |158.150/326.850/654.008|244.641/436.064/541.064|
| Maximum successful-echo spacing,s |.926587|.732532|
| Raw bins / zero bins |40/0|40/5|

Both are40s duration-partial8GiB downloads, not full-object completion.
All echoes succeed, neither disconnects, and both stderr files are empty.
Whole useful rate is15.70% lower, but that number alone is within the user's
speed tolerance; the new3.300801s body hold and healthy-phase40.37% reduction
are the adverse practical results. Improved QoS mean does not erase them.

Candidate maximum gap is exactly20.968530551–24.269331275 probe seconds,
body463318468→463384004B. Baseline maximum is30.417986828–30.745479455s,
body803541246→803555846B. Candidate's worst successful echo is attempt20 at
10.032186206–10.573250526s; it is not the maximum bulk-gap interval.

| Complete raw phase mean,Mbps |Baseline B|Candidate|
|---|---:|---:|
|0–5|223.399|151.587|
|5–15|324.202|193.307|
|15–25|13.381|153.633|
|16–25|12.904|153.222|
|25–30|365.910|232.120|
|30–33|161.683|160.703|
|33–40|257.994|198.762|

All40candidate bins, Mbps, no trimming; baseline's entire series remains above.
Catch-up bursts inside the QoS phase are delivery, not sustained physical rate.

```text
00: 1.572 174.799 199.290 192.607 189.665 175.903 168.061 189.953 225.448 188.315
10: 220.656 187.904 164.582 192.425 219.821 157.331 190.652 165.339 0 0
20: 502.224 0 0 0 520.783 289.421 228.056 226.124 215.944 201.057
30: 151.808 163.671 166.630 189.394 143.577 269.562 195.847 223.420 189.195 180.339
```

Physical checks retain40service rows each, exactly3TCP47+1QUIC46,4active,
no suspect/failed or changed physical identity. Both directions are200Mbps
except QUIC46's DOWN cut10Mbps; DOWN70/UP30ms, no random loss or jitter,
class/netem drops0. Candidate new-state row times areQoS15.002109→25.003121,
UDP-blackhole30.003662→33.495635; baseline15.055191→25.109579 and
30.269739→33.706486. These are pre-command loop stamps, not exact rule-install
instants. TCP47 remains200Mbps. No new physical-profile mismatch is observed.

| Sampled cost |Baseline B|Candidate|
|---|---:|---:|
| DOWN TCP47 bytes |738667538|923852556|
| DOWN QUIC46 bytes |667294658|635677756|
| Summed DOWN wire / useful |1.305826|1.718119|
| UP return bytes |20509816|22047225|
| DOWN backlog peak,B |11748316|12735010|
| Client/server peak RSS,KiB |81864/313896|104048/331540|
| Client/server peak lifetime CPU,% |54.4/131|49.5/106|
| Server native flight peak,B |47874162|28978275|
| Server queue summary peak,B |556842|539884|

Wire is last-minus-first class samples, not exactly the probe lifetime;
sample endpoints are40.093744s baseline and39.739496s candidate. Total sampled
DOWN wire rises10.92% while useful bytes fall15.70%. This is materially more
carrier work per useful byte, NOT a measurement of exact copy bytes. No
ordinary Original/copy event ledger identifies the residual. Process CPU is
a lifetime percentage, not an exclusive instantaneous bottleneck attribution.

In healthy rows5→15, native TCP/QUIC ACK advancement is227022732/238422756B
baseline versus229420514/226867471B candidate: aggregate465.445→456.288MB,
nearly unchanged. Client logical delivery instead advances407856392→242820169B,
and server source reads408571568→242908290B. Own management durations are10s
(candidate server9.999s); both cuts are busy, around190/193Mbps baseline and
192/183Mbps candidate in physical TCP47/QUIC46 samples. Thus the healthy loss
of useful aggregation is not simply unused native throughput or absent source
demand. Ordinary counters still do not distinguish Original overlap, repairs,
native retransmission, framing or ordered buffering as the cause.

The new stalls are confirmed independently of echo traffic. HTTP8080 flow2
client delivery stays463318676B at Unix1789094635501/6501/7501ms; that is
the probe's463318468B body frontier plus208HTTP-header bytes. Server HTTP
source reads stay530427540B at5503/6503/7503ms in the same1789094630000ms
base, exactly64MiB ahead. These are distinct observations2s apart, not repeats.
During the same server bracket TCP native ACKs advance41276933B and QUIC
4055436B, while successful echoes continue. Earlier rows18→20 have another
HTTP-only2s plateau at400540724B; server source467649588B is also flat, while
TCP native ACKs advance47828888B. No useful-source/read stall is inferred
merely from a raw zero or from including/excluding64B echoes.

Source-read minus delivered bytes is NOT the exact assigned-offset/peer-MAX
pair, nor a per-path Product-credit measurement. The plateau's body identity
matches the probe, but independently generated management rows do not provide
the probe's exact wall-clock start/end. Candidate server management repeats
once elsewhere; baseline client repeats once. Neither repeated row was used
as a distinct plateau sample. TCP/native progress cannot identify which
critical DSN is missing or which carrier originally owns it.

All native paths keep one epoch and nondecreasing producer time/ACK counters.
Maximum server QUIC RTT is similar4.243→4.259s. TCP aggregate flight limits
remain roughly4–6MB; candidate sometimes has several MB in flight during a
stall and later drains nearly completely. Native headroom is not exact
Product repair credit. DOWN Product-flight summaries0 do not imply zero
retained response work. The separately observed long-lived upload sink is
idle(2CPU ticks,0major faults each); its148816/157468KiB RSS and preexisting
704004/716464KiB swap are not a traced HTTP/echo cause or a new leak proof.

Archive `./docs-dev/RESPONSE_UNCOVERED_PREFIX_ORDINARY_20260911.raw.tar.gz`:
9regular files,1,877,251B raw /235,057B compressed, every member byte-verified.
It contains the candidate's five raw files plus build log and three test logs:
`response-uncovered-prefix-red-exec-0911.log`, `-green-0911.log`, and
`-controls-accepted-0911.log` with the same prefix. The last filename denotes
170component tests passing, NOT ordinary acceptance. Baseline and runner are
already archived in ISOLATED_LINK_ADVERSE; exact trial source is checkpoint
ec8cd2f. No binary, credentials, invalid run or unrelated test log is included.
