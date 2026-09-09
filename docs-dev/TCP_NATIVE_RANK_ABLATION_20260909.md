# TCP native-rank ablation — 2026-09-09

Status: **promotion rejected; diagnostic rate-source substitution, not an
accepted performance correction**. Useful goodput rises396.378→419.059Mbps,
but echo median/p95 worsens281.651/469.903→318.842/577.845ms. Maximum echo
latency and body-read gap improve. All157 attempts succeed. The mixed outcome
does not justify native-rate restoration, a favorable repeat or another tuning
parameter. Ordinary source and executable remain restored.

## Predeclared question and exact intervention

The preceding [TCP Product observation](TCP_PRODUCT_EVIDENCE_20260909.md) shows
numeric fallback despite fresh native advice, but not contemporaneous ambiguity
causing that fallback. Declining Original service precedes the two observed
expiries. This pair asks the narrower counterfactual: does ranking carrier work
with per-flow Product service materially worsen mixed placement, or does shared/
native service remain costly when the available qualified native advisory is
used only for ranking?

CURRENT_CLOSURE_PLAN predeclares one same-feature-build control→advised pair,
with the existing healthy500/500Mbps,30/70ms profile. The advisory differs
severalfold in prior observations, so allocation changes could be material;
the earlier mixed/QUIC-only latency difference is a comparison envelope, not
a promised removable delay. Better sustained timing with comparable useful load
and changed allocation would support further model work. Adverse or ambiguous
timing stops promotion. Native advice can overpredict unique service and increase
queuing; zero gain or regression was explicitly possible.

The startup-fixed server flag changes **only two Throughput Original ETA
evaluations**: a temporary scoring snapshot uses the already available finite,
qualified local TCP carrier rate and PathCapacity scope. The original snapshot
remains in each target/admission tuple. Latency lanes, QUIC, Product qualification,
typed authority, feedback, recovery, resource rules and ordinary shared snapshot
helpers are unchanged. Incumbent hysteresis consumes the supplied ETAs, queue
and jitter; no separate rate recomputation bypasses the intended discriminator.
Placement can change FirstPath identity and therefore where unchanged admission
rules apply. This is not a bit-identical allocation control.

Each server emits one mode marker: control `active=false`, advised
`active=true`. Only advised emits actual first-use evidence: observer time.734s,
TCP path2 /physical3 /output incarnation2, ordinary350750.750751bps replaced
for scoring by3506344bps, both PathCapacity. This verifies a real finite qualified
source use, not merely a requested flag. It does not enumerate every later
substituted score or prove that the first-use path won that decision.

The existing `tcp_prepared_rate_evidence` events still show the **ordinary**
prepared projection in both cells. In advised they are not the substituted
ranking scalar. They cannot be treated as direct records of actual ranking,
selected winners or admitted bytes.

## Build, capture and scope

Ordinaryb2aa215 plus one frozen eleven-source-file feature overlay supplies both
cells. The clean optimized build takes3m36s. Parent freezes and fully removes
the overlay before traffic, and restores/byte-compares the ordinary executable
afterward. No runtime or RFC change remains accepted.

Inputs under `./.tmp/reflection/` are
`tcp-native-rank-observer-0909.patch`,
`tcp-native-rank-build-0909.log`,
`tcp-native-rank-{control,advised}-run-0909.log`, and
`results/mixed-combined-down-tcp-native-rank-{control,advised}-0909/`.
Each result retains the complete probe JSON, empty probe stderr, service samples,
and server/client logs. Both runners exit0, elapsed41.007802/41.005406s.
Existing HTB quantum warnings remain; no profile adjustment silences them.

[Raw archive](TCP_NATIVE_RANK_ABLATION_20260909.raw.tar.gz) retains17 regular
files: both complete five-file results, the overlay, build log, two run logs,
two wrappers and derived `tcp-native-rank-costs-0909.json`. Size572047B;
gzip integrity, exact member listing and decompressed byte comparisons against
all original inputs pass. Ordinary source/RFC/Cargo matchb2aa215 and the restored
executable matches the retained ordinary binary. No build or lab remains active.

All82 effective service rows independently confirm500Mbps each direction,
30ms DOWN/70ms UP, zero configured jitter/loss and no blackhole. HTB rate/ceil
62500000B/s, burst/cburst65536 and netem limit8192 remain constant. Every
class/qdisc drop delta is zero. No15/25s QoS transition occurs; chronology slices
below do not represent different link conditions.

Both workloads use one duration-stopped8GiB HTTP response and persistent64B
TCP echoes every500ms with3s timeout. No browser/Cloudflare, upload, outage or
required recovery acceptance is exercised. Control and advised share binary,
profile and workload but have independent native/task histories and actual
allocations; the pair does not establish statistical causal magnitude.

## Complete useful service and timing

| Outcome | Control | Native advised ranking |
|---|---:|---:|
| Received body B | 1982037662 | 2095299417 |
| Body duration s | 40.002960 | 40.000069 |
| Whole useful goodput Mbps | 396.378198 | 419.059161 |
| First body s | .583854 | .579087 |
| Maximum body-read gap s | .325905 | .271684 |
| Maximum-gap start → end s | 21.952216→22.278120 | 11.782859→12.054543 |
| Body bytes before /after gap | 1077663686 /1077675686 | 567782565 /567794565 |
| HTTP code /requests /complete /duration-partial | 200 /1 /0 /1 | 200 /1 /0 /1 |
| Echo success /attempts /failures | 79 /79 /0 | 78 /78 /0 |
| Echo request /response B | 5056 /5056 | 4992 /4992 |
| Echo p50 /p95 /maximum ms | 281.651 /469.903 /1244.017 | 318.842 /577.845 /761.642 |
| Maximum echo-completion spacing s | 1.478111 | 1.129329 |

Both HTTP bodies are intentionally duration-partial, not completed8GiB transfers.
All157 echo records are preserved and validated: unique sequential indices,
finite consistent timing and successful outcomes. Fewer than80 attempts reflect
serial scheduling while earlier exchanges take longer, not hidden failures.
Control's last echo finishes40.427962s and remains included; advised ends
39.876307s. Neither probe records disconnect/error, and both stderr files are empty.
Successful echo timing excludes initial SOCKS/connect setup; first-body timing
includes the existing wait for first echo readiness.

| Healthy chronology slice | Control body Mbps | Advised body Mbps | Control echo p50/p95/max ms (n) | Advised echo p50/p95/max ms (n) |
|---|---:|---:|---|---|
| 0–5s | 299.347 | 321.560 | 222.803 /377.332 /377.332 (10) | 353.863 /761.642 /761.642 (9) |
| 5–15s | 427.845 | 443.091 | 266.007 /469.903 /1244.017 (19) | 335.335 /685.294 /690.036 (19) |
| 15–25s | 405.506 | 433.698 | 296.664 /445.100 /552.447 (20) | 330.147 /489.290 /527.715 (20) |
| 25–40s | 401.730 | 425.774 | 290.507 /483.330 /639.894 (30) | 306.431 /464.866 /482.849 (30) |

Every body bin is included, not the trimmed418.058/435.002Mbps fields. Echo
slices use attempt start and the probe's rounded `(n−1)p` percentile index,
including ties-to-even. Recalculation matches whole probe quantiles. The early
and5–25s p95 costs are retained; better last-phase tails do not cancel them.
Median echo latency worsens in all four slices despite greater useful service.

| Cell /slow echo index | Start → end s | Latency ms |
|---|---|---:|
| Control26 | 13.004754→14.248771 | 1244.017 |
| Control74 | 37.818584→38.458478 | 639.894 |
| Control29 | 15.248894→15.801341 | 552.447 |
| Advised1 | .500622→1.262263 | 761.642 |
| Advised3 | 1.762390→2.502017 | 739.627 |
| Advised21 | 11.159261→11.849297 | 690.036 |
| Advised22 | 11.849321→12.534615 | 685.294 |

Maximum completion-to-completion spacing is12.770660→14.248771s in control
(attempts25→26) and1.372688→2.502017s in advised (2→3). That measure includes
scheduled spacing plus exchange service, not another body-read gap. The better
maximum does not make the adverse median/p95 disappear.

Client Broken-pipe at each bulk stop is09:55:13.801/09:56:44.254UTC; corresponding
server RemoteClosed follows13.874/44.328 within those minutes. H3_NO_ERROR
shutdown follows09:55:14.844/09:56:45.292. These retained teardown warnings are
not failed probe attempts or a newly inferred lifecycle defect.

## Actual allocation, copies and receipt

All perf interval count/byte/time sums reconcile against cumulative values:
control/advised server788/810 rows and client834/842 rows, one PID per role
and no invalid-receipt component. Zero/absent categories mean no observed event
within the flushed capture, not invented numeric evidence.

| Source payload B | Control | Advised |
|---|---:|---:|
| TCP Originals, all lanes | 666847370 | 1032534178 |
| QUIC Originals, all lanes | 1333842188 | 1084885247 |
| Total Originals | 2000689558 | 2117419425 |
| TCP Original share | 33.3309% | 48.7638% |
| TCP persistent-gap /tail copies | 220380028 /50422870 | 94327808 /65712646 |
| QUIC persistent-gap /tail copies | 4018628 /764576 | 22307286 /3756456 |
| All accepted copies | 275586102 | 186104196 |

The intervention changes realized allocation materially. All observed Latency
Originals remain TCP in both cells; the echo policy itself was not substituted.
Yet echo timing worsens, consistent with a composed-service cost that a bulk-only
ranking change need not avoid. These aggregates do not identify its exact queue
or byte-level causal mechanism.

Total copies fall, but QUIC copies rise4783204→26063742B and TCP tail copies
rise50422870→65712646B. No other accepted repair-cause or requalification
component appears. This is not uniform recovery-cost improvement, nor evidence
that the remaining required copies can safely be removed.

| Receiver payload B | Control | Advised |
|---|---:|---:|
| TCP new /duplicate | 662691458 /268346622 | 1025785682 /158199878 |
| QUIC new /duplicate | 1320464700 /4683268 | 1072325463 /26049142 |
| All new /duplicate | 1983156158 /273029890 | 2098111145 /184249020 |
| All input = new +duplicate | 2256186048 | 2282360165 |
| Ordered-triggered | 1982066926 | 2095351689 |
| New not yet ordered at final observation | 1089232 | 2759456 |

Mux input equals those receipt partitions. Ordered-triggered service can unlock
another ingress's suffix and is not carrier ownership or completed local delivery.
Admission, receipt, body completion and final flush have distinct cancellation/
transit tails. Buffered residual is not proof of post-teardown leakage.

Server TCP encoded plaintext rises938736289→1193576343B; QUIC encode and
successful write-wait agree within each cell at1342806868→1114446827B. Total
encoded bytes rise2281543157→2308023170B despite fewer copies, because Original
service rises. Client return encoded TCP falls19390155→14719884B and QUIC
7419935→5617456B. Encoded data are not physical-wire or native retransmission
counts; the actual allocation shift prevents treating copy reduction as an
isolated wire-efficiency experiment.

## Physical and resource costs

Independent profile/class/resource extraction agrees with the parent's derived
cost JSON. Class windows span40.007568/40.005201s, not exact body or final perf
flush intervals. Stable QUIC physicalinstance1 and one role-specific native
epoch/session persist throughout all41 samples per cell.

| Sampled cost | Control | Advised |
|---|---:|---:|
| DOWN /UP class byte deltas | 2405627596 /98409788 | 2433035775 /80833598 |
| DOWN /UP packet-counter deltas | 1763831 /851307 | 1766093 /728888 |
| Peak DOWN /UP backlog B | 22786646 /262480 | 19378986 /265035 |
| Client peak /final RSS KiB | 91320 /91320 | 75108 /75108 |
| Server peak /final RSS KiB | 334448 /326064 | 317540 /313056 |
| Client peak /final ps CPU % | 136 /136 | 119 /119 |
| Server peak /final ps CPU % | 201 /201 | 200 /200 |
| Client QUIC RTT p50 /p95 /max ms | 255.037 /405.762 /455.246 | 261.590 /355.289 /372.084 |
| Server QUIC RTT p50 /p95 /max ms | 254.033 /403.461 /461.585 | 250.759 /347.466 /376.346 |
| Server QUIC flight p50 /p95 /max B | 10233696 /17397864 /19302975 | 7713177 /13654676 /14392224 |

All class/qdisc drop deltas are zero. Lower aggregate DOWN backlog, native tails,
CPU/RSS and UP traffic coexist with worse application echo median/p95; they do
not establish a latency-neutral optimization. UP backlog and client median
QUIC RTT increase slightly. Server median QUIC RTT stays near251ms on a100ms
propagation path. Packet counters are offload-sensitive, queues are sampled not
continuous maxima, ps CPU is lifetime multicore use, and RSS is not leak proof.
No aggregate cost identifies the exact critical position of an individual echo.

## Forecast versus outcome and disposition

Actual qualified source use and changed allocation validate the discriminator.
Higher useful service, fewer copies and lower aggregate cost support a material
ranking effect, but the predeclared composed-timing expectation fails: early
tails and median/p95 echo service are worse. The favorable maximum/gap and
late-phase tails are retained rather than describing the run as universally
slower. One ordered pair does not establish statistical causal certainty about
a particular internal mechanism.

**Reject promotion.** Do not restore native rates into the ordinary Product
snapshot or assume that another multiplier, threshold or blend would resolve
the tradeoff. The11-file diagnostic overlay is removed; no runtime/RFC fix,
new impairment test, favorable repeat, baseline victory or release follows.
The parent owns the next bounded model decision. README/PERFORMANCE remain
unchanged. This report preserves an unsuccessful practical forecast, not a new
mandatory feature or an invitation to reopen unrelated issues.

## Complete body and echo timing series

All80 raw one-second body Mbps bins, including startup and adverse intervals.
Application bursts above500Mbps are not sustained physical-link capacity.

```text
control = [2.621,109.549,292.841,626.364,465.361,425.452,243.354,611.771,404.328,385.803,446.548,327.645,549.507,432.431,451.612,409.097,362.273,266.323,503.637,449.264,401.497,454.029,426.044,400.387,382.512,413.132,340.79,415.144,380.052,414.335,434.443,392.127,408.409,269.491,504.137,389.502,427.167,370.451,313.05,553.722]
advised = [2.621,309.967,437.212,433.116,424.884,456.182,371.096,513.382,437.458,446.428,460.83,249.085,593.079,465.805,437.566,433.68,410.286,452.307,418.808,440.098,366.024,471.823,416.441,462.754,464.758,453.786,311.395,517.195,458.956,422.998,458.884,425.999,433.93,368.807,407.773,427.649,476.807,277.786,537.631,407.014]
```

All157 actual echo attempts follow. Display offsets are rounded to6 decimals
and latency to3; archived JSON retains original precision. Every attempt is
successful; there is no omitted failed subset or inferred80-attempt denominator.

```csv
cell,index,start_s,end_s,latency_ms,outcome
control,0,0.102914,0.203442,100.527,success
control,1,0.500341,0.683733,183.392,success
control,2,1.000427,1.286444,286.017,success
control,3,1.500500,1.877832,377.332,success
control,4,2.000577,2.304069,303.492,success
control,5,2.500651,2.723454,222.803,success
control,6,3.000822,3.120765,119.943,success
control,7,3.500918,3.682107,181.189,success
control,8,4.002458,4.263086,260.628,success
control,9,4.501630,4.761256,259.626,success
control,10,5.001723,5.163224,161.502,success
control,11,5.502360,5.781061,278.701,success
control,12,6.002433,6.217293,214.860,success
control,13,6.503055,6.693819,190.764,success
control,14,7.003130,7.204908,201.777,success
control,15,7.503205,7.742503,239.299,success
control,16,8.003284,8.287878,284.594,success
control,17,8.503435,8.795418,291.984,success
control,18,9.003492,9.344326,340.834,success
control,19,9.503540,9.807824,304.285,success
control,20,10.003628,10.250589,246.961,success
control,21,10.503696,10.757334,253.639,success
control,22,11.003751,11.189205,185.454,success
control,23,11.503837,11.811922,308.085,success
control,24,12.003920,12.184462,180.542,success
control,25,12.504653,12.770660,266.007,success
control,26,13.004754,14.248771,1244.017,success
control,27,14.248790,14.699750,450.960,success
control,28,14.748823,15.218727,469.903,success
control,29,15.248894,15.801341,552.447,success
control,30,15.801389,16.203730,402.341,success
control,31,16.304079,16.749179,445.100,success
control,32,16.804227,17.064121,259.893,success
control,33,17.305052,17.583537,278.485,success
control,34,17.805397,18.070828,265.431,success
control,35,18.305467,18.673586,368.119,success
control,36,18.805755,19.109345,303.590,success
control,37,19.305836,19.585282,279.445,success
control,38,19.805947,20.087598,281.651,success
control,39,20.306125,20.608762,302.637,success
control,40,20.806207,21.102871,296.664,success
control,41,21.306992,21.590814,283.822,success
control,42,21.807042,22.060630,253.588,success
control,43,22.307139,22.565520,258.380,success
control,44,22.807263,23.046859,239.596,success
control,45,23.307349,23.577987,270.638,success
control,46,23.807417,24.145323,337.906,success
control,47,24.307537,24.643684,336.147,success
control,48,24.807609,25.149875,342.266,success
control,49,25.308684,25.545641,236.957,success
control,50,25.808899,26.146464,337.565,success
control,51,26.308996,26.609602,300.606,success
control,52,26.809067,27.099574,290.507,success
control,53,27.309304,27.776138,466.834,success
control,54,27.809427,27.920501,111.074,success
control,55,28.309512,28.498137,188.625,success
control,56,28.809837,29.055454,245.617,success
control,57,29.310255,29.581155,270.900,success
control,58,29.810021,30.092387,282.366,success
control,59,30.310102,30.671277,361.175,success
control,60,30.811791,31.082907,271.116,success
control,61,31.311875,31.625538,313.663,success
control,62,31.811963,32.082500,270.538,success
control,63,32.312035,32.558428,246.393,success
control,64,32.812120,33.199535,387.415,success
control,65,33.312624,33.560983,248.359,success
control,66,33.812977,34.114000,301.022,success
control,67,34.313449,34.631838,318.389,success
control,68,34.816844,35.085537,268.693,success
control,69,35.318136,35.631250,313.114,success
control,70,35.818221,36.199670,381.449,success
control,71,36.318292,36.775505,457.214,success
control,72,36.818370,37.301700,483.330,success
control,73,37.318513,37.664383,345.870,success
control,74,37.818584,38.458478,639.894,success
control,75,38.458497,38.645016,186.520,success
control,76,38.958568,39.161139,202.571,success
control,77,39.458655,39.698621,239.966,success
control,78,39.958745,40.427962,469.217,success
advised,0,0.103079,0.203915,100.836,success
advised,1,0.500622,1.262263,761.642,success
advised,2,1.262292,1.372688,110.396,success
advised,3,1.762390,2.502017,739.627,success
advised,4,2.502032,2.730668,228.637,success
advised,5,3.005538,3.359400,353.863,success
advised,6,3.505639,4.079766,574.127,success
advised,7,4.079791,4.397130,317.340,success
advised,8,4.579882,5.157726,577.845,success
advised,9,5.157749,5.404183,246.434,success
advised,10,5.657835,5.941928,284.093,success
advised,11,6.157916,6.380942,223.025,success
advised,12,6.657979,7.005977,347.998,success
advised,13,7.158130,7.513244,355.114,success
advised,14,7.658164,7.950135,291.971,success
advised,15,8.158361,8.496045,337.683,success
advised,16,8.658443,8.892972,234.529,success
advised,17,9.158531,9.384735,226.204,success
advised,18,9.658632,9.815082,156.450,success
advised,19,10.158730,10.372715,213.985,success
advised,20,10.658803,11.100842,442.038,success
advised,21,11.159261,11.849297,690.036,success
advised,22,11.849321,12.534615,685.294,success
advised,23,12.534667,12.759072,224.405,success
advised,24,13.034797,13.370132,335.335,success
advised,25,13.534862,13.913320,378.459,success
advised,26,14.035402,14.438221,402.819,success
advised,27,14.535779,14.901107,365.328,success
advised,28,15.035847,15.198978,163.131,success
advised,29,15.535926,15.918706,382.780,success
advised,30,16.036002,16.465505,429.504,success
advised,31,16.536135,16.921051,384.917,success
advised,32,17.036232,17.363152,326.919,success
advised,33,17.536295,17.778543,242.248,success
advised,34,18.036688,18.326504,289.816,success
advised,35,18.536903,18.814680,277.777,success
advised,36,19.036983,19.425715,388.732,success
advised,37,19.537078,19.760635,223.558,success
advised,38,20.037174,20.526464,489.290,success
advised,39,20.537213,20.749941,212.728,success
advised,40,21.037291,21.481336,444.046,success
advised,41,21.537365,22.065080,527.715,success
advised,42,22.065101,22.411111,346.009,success
advised,43,22.565195,22.993396,428.201,success
advised,44,23.065270,23.395417,330.147,success
advised,45,23.565342,23.791855,226.513,success
advised,46,24.065613,24.333293,267.680,success
advised,47,24.565686,24.831763,266.076,success
advised,48,25.065779,25.384621,318.842,success
advised,49,25.565862,25.828954,263.091,success
advised,50,26.066078,26.388285,322.207,success
advised,51,26.566054,26.820350,254.296,success
advised,52,27.066133,27.381398,315.265,success
advised,53,27.566221,27.967336,401.115,success
advised,54,28.066304,28.509250,442.945,success
advised,55,28.566458,28.908320,341.862,success
advised,56,29.066527,29.295080,228.553,success
advised,57,29.566635,29.893537,326.902,success
advised,58,30.067777,30.360503,292.726,success
advised,59,30.567856,30.829628,261.771,success
advised,60,31.068020,31.532887,464.866,success
advised,61,31.568111,31.714969,146.858,success
advised,62,32.068200,32.417180,348.980,success
advised,63,32.568266,33.028608,460.342,success
advised,64,33.069233,33.552083,482.849,success
advised,65,33.569203,33.932429,363.227,success
advised,66,34.069277,34.424741,355.464,success
advised,67,34.569361,34.917264,347.903,success
advised,68,35.070064,35.362741,292.677,success
advised,69,35.570143,35.805977,235.834,success
advised,70,36.070216,36.344430,274.214,success
advised,71,36.570289,36.852531,282.242,success
advised,72,37.070364,37.227551,157.187,success
advised,73,37.570445,37.689817,119.372,success
advised,74,38.070516,38.354564,284.048,success
advised,75,38.570666,38.900534,329.868,success
advised,76,39.070706,39.377137,306.431,success
advised,77,39.570813,39.876307,305.494,success
```
