# Native refill: combined directional stress classification

Recorded: 2026-09-10. Ordinary working MPP `b0baca2`, unchanged refill
reserve and protocol, no feature observer or concurrent build. **This is a
diagnostic stress classification, not healthy-link competitiveness or release
acceptance.** Mixed DOWN transfers312.384Mbps with all66echoes succeeding;
mixed UP settles every accepted byte at62.511Mbps but has a3.059s confirmation
gap. QUIC-only UP settles at96.986Mbps with a5.160s gap. Raw TCP settles at
7.293Mbps; Hysteria2 remains incomplete at the observation limit. Neither the
lower mixed mean nor the worse baseline outcomes alone select a runtime defect.
All twelve directional captures have now closed. Execution is complete, not
performance acceptance: only mixed MPP, QUIC MPP and raw TCP have exact upload
settlement; sole-UDP download echoes time out during the outage.

| System | DOWN Mbps | DOWN echo successes / records; p95 ms | UP exact Mbps or incomplete outcome |
|---|---:|---:|---|
| MPP TCP | 404.684 | 56/56;1190.884 | 126.339/152.437MB confirmed/accepted, guard-censored |
| MPP QUIC | 311.418 | 55/70;926.612, successful-only | 96.986, exact |
| MPP mixed | 312.384 | 66/66;947.868 | 62.511, exact |
| Raw TCP | 363.575 | 80/80;447.028 | 7.293, exact |
| Xray VMess/TCP | 299.315 | 79/79;450.295 | 33.862/33.948MB, closes before terminal ACK |
| Hysteria2 | 144.912 | 61/75;331.163, successful-only | 39.120/60.097MB, guard-censored |

The [complete raw archive](NATIVE_REFILL_COMBINED_20260910.raw.tar.gz) retains
61files: all twelve cells, driver logs and the exact runner/shaper before later
topology changes. All397available raw bins,426download outcome records and
651service samples are represented below; censored uploads have no valid bins.

## Question, inputs and actual directional conditions

The preceding [recovery classification](NATIVE_REFILL_RECOVERY_20260910.md)
identified mostly post-admission residence for one exact missing prefix, not
a late actor wake or repair enqueue. This next bounded question is how current
mixed service behaves when changing loss/jitter, restriction and UDP outage
are combined, and whether poor upload service is specific to MPP. No reserve,
timer, native-controller, source or profile adjustment follows a low mean.
The result must retain startup, restriction, restoration, settlement, gaps and
costs; this difficult profile does not replace the high-capacity healthy gates.

Closed inputs are under `./.tmp/reflection/results/`:

- `mixed-combined-{down,up}-native-refill-combined-0910/`;
- `{quic,h2,raw}-combined-up-native-refill-combined-controls-0910/`.

Driver logs are `./.tmp/reflection/native-refill-combined-0910.log` and
`./.tmp/reflection/native-refill-combined-controls-0910.log`. Mixed DOWN then UP
complete first; controls run QUIC then H2. H2 reaches the runner's85s guard, so
that sequence exits1 before raw starts. Root runs the remaining raw cell once
with the same configuration and appends its result to the same log. There is
no H2 retry or favorable replacement. MPP and raw runners return0.

All285 initial-cell sampled rows independently match the configured epoch schedule. This
is one shared routed500Mbps cut, not aggregation of independent links. Physical
DOWN is30ms with5ms normal-distribution jitter; physical UP is70ms with20ms
jitter. The mirrored loss schedule, in successive5s epochs, is:

```text
Epoch start, s:    0   5  10  15  20  25  30  35
DOWN loss, %:     1   2  .5   3   2  .5   1   2
UP loss, %:       3   8   5   6  10   3   5   8
UP capacity,Mbps:500 500 500  10  10 500 500 500
```

DOWN stays500Mbps; UP's configured loss averages6% over the40s offered window,
DOWN1.5%. The final epoch persists during settlement. Rate equals ceil,
burst/cburst65536B and netem limit8192 throughout. UDP is deliberately dropped
both ways near30–33s. Thus the10Mbps cut constrains **upload data**, but only
the return direction for download. Equal configurations do not give identical
random packet histories or effective physical packet erasure, especially with
offloads. HTB/netem drop counters exclude the separate endpoint UDP filters.

| Cell | Service rows / final elapsed, s | First UP10 / restored500, s | UDP set / clear, s |
|---|---:|---:|---:|
| Mixed DOWN | 41 /40.209640 | 15.001681 /25.002760 | 30.003301 /33.105189 |
| Mixed UP | 46 /45.094489 | 15.001660 /25.002665 | 30.003182 /33.041707 |
| QUIC UP | 47 /46.125390 | 15.006890 /25.008009 | 30.010837 /33.045335 |
| H2 UP | 86 /85.010974 | 15.001791 /25.002834 | 30.003376 /33.003704 |
| Raw UP | 65 /64.006922 | 15.001697 /25.002752 | 30.003291 /33.003614 |

These are runner sampling boundaries around sequential rule/telemetry commands,
not synchronized packet timestamps. Raw TCP is unaffected by UDP filters but
does experience the same loss/jitter/rate schedule. Mixed retains TCP when its
QUIC is blocked; QUIC-only/H2 lose their sole carrier. H2 retains explicit
500Mbps up/down priors; MPP uses unconfigured dynamic discovery. Xray uses
VMess over TCP. No MPTCP, Cloudflare browser or independent-link aggregation
result is added here.
Root's read-only check confirms default kernel TCP congestion control is BBR
in both endpoint containers, with no configured per-product TCP override found.
That describes the TCP baseline context, not QUIC's controller or identical
connection histories.

## Mixed download: completion and timing

HTTP200, probe `ok`,1,566,128,576body bytes in40.107836s:312.383561Mbps.
One8GiB response is intentionally duration-stopped, not fully completed. First
body is0.852808s; the maximum read gap is0.802930s at30.398906–31.201835s,
body counters1,194,375,120→1,194,389,720. All66actual64-byte echoes succeed,
with p50/p95/max369.564/947.868/2120.339ms and maximum successful-reply spacing
2.282532s. The worst echo29.697989–31.818328 straddles the outage start;
the second-worst22.345484–24.009869 lasts1664.384ms during return restriction.
There are no failed attempts or empty-success substitutions; serial delays
reduce the number of attempted echoes. Stderr is empty.

| Request-start phase | Body Mbps | Echo count / p50 / p95 / max, ms |
|---|---:|---:|
| 0–5s | 197.925 | 9 /395.715 /724.397 /724.397 |
| 5–15s | 444.298 | 15 /630.146 /947.868 /1055.816 |
| 15–25s | 243.095 | 16 /445.096 /811.923 /1664.384 |
| 25–30s | 336.821 | 10 /213.724 /2120.339 /2120.339 |
| 30–33s | 2.914 | 3 /110.434 /152.128 /152.128 |
| 33–35s | 442.387 | 3 /154.934 /701.032 /701.032 |
| 35–40s | 417.551 | 10 /340.420 /523.816 /523.816 |

Quantiles use sorted index`round((n−1)*rank)`. Phase membership follows request
start; the apparently short echoes starting30–33s do not erase the preceding
2.120s request that crosses into that phase. Restored bulk throughput is
substantial, but whole-run success is not uniformly fluent short-request service.

## Upload: exact settlement versus censoring

Each upload offers work for40s, then waits for target-sink confirmation. The
whole rate includes settlement, not merely local socket acceptance. No
concurrent interactive echo workload runs in these upload cells.

| Metric | Mixed | QUIC-only | H2, censored | Raw TCP |
|---|---:|---:|---:|---:|
| Target-confirmed bytes | 352,845,824 | 564,789,248 | 39,120,373 | 58,327,040 |
| Locally accepted bytes | 352,845,824 | 564,789,248 | 60,096,512 | 58,327,040 |
| Elapsed, s | 45.156199 | 46.587072 | 85.333724 | 63.979294 |
| Exact completed streams | 1/1 | 1/1 | 0/1 | 1/1 |
| Exact whole Mbps | 62.511 | 96.986 | unavailable | 7.293 |
| First local write / confirmation, s | .124320 /.497727 | .145611 /.252141 | .083466 /.174812 | .103965 /.257524 |
| Recorded maximum confirmation gap, s | 3.059472 | 5.160364 | 1.167877, censored | .767671 |
| Recorded maximum local write gap, s | 2.350313 | 5.092823 | .476587, censored | .400935 |

H2's3.668Mbps JSON ratio is confirmed-so-far bytes divided by a guard-limited
interval, not an exact completed-transfer comparison. At85.011s the outer
runner raises `probe failed to settle` and begins normal product teardown;
the resulting client shutdown causes the recorded connection reset. That reset
is not evidence of an independent spontaneous H2 failure. Confirmation remains
incomplete before teardown, however, so the result is not a pass. Its reported
maximum inter-confirmation gap does not bound the unresolved trailing wait.
`upload_ack_accounting_valid=false` and both interval arrays are empty: there
is **no valid H2 confirmation time series** to reconstruct from class traffic.

MPP mixed, QUIC and raw have valid exact accounting and no probe errors. Their
maximum confirmation/write-gap endpoints are not saved by this probe version;
raw zero bins reveal periods without newly confirmed bytes but cannot assign
an exact maximum-gap endpoint. The following are means of **untrimmed wall-clock
bins**, not the helper's arrays after discarding three bins from either end.

| Offered-window phase | Mixed confirmed Mbps | QUIC confirmed Mbps | Raw confirmed Mbps |
|---|---:|---:|---:|
| 0–5s | 44.140 | 168.026 | 41.027 |
| 5–15s | 40.461 | 171.935 | 8.237 |
| Data restriction15–25s | 4.928 | 8.441 | 4.226 |
| Restored25–30s | 110.415 | 84.803 | 2.933 |
| UDP outage30–33s | 20.905 | 35.397 | 3.713 |
| Early restored33–35s | 69.043 | 32.830 | 4.124 |
| Late restored35–40s | 142.408 | 100.547 | 3.781 |

Mixed is substantially slower than QUIC before restriction but faster in the
last recovery window; a single mean conceals that reversal. Mixed has zero
confirmation bins16,17,20,22,24 during the10Mbps data restriction. QUIC has
zero bins31–33 during/after UDP blockage and38,43 after restoration, including
settlement. These adverse gaps remain despite exact final delivery. Raw's
small gap statistic coexists with very low sustained service and24s of
settlement; it is not an equal-speed low-latency control. The results do not
establish that each stall is physically unavoidable or caused by MPP's model.

## Native class traffic and sampled resource context

Use native class counters once per direction, not class plus parent/qdisc.
They include protocol/recovery/native traffic and are not unique payload.
Whole cost windows differ because uploads settle at different times; row0→40
offers a common coarse boundary, not exact per-probe alignment.

| Cell | Whole DOWN / UP class bytes | Row0→40 DOWN / UP bytes | DOWN / UP class drops |
|---|---:|---:|---:|
| Mixed DOWN | 1,909,420,550 /57,411,602 | same | 4,331 /14,496 |
| Mixed UP | 25,948,995 /459,939,921 | 19,783,245 /364,600,961 | 2,469 /9,971 |
| QUIC UP | 20,836,957 /694,905,762 | 18,446,980 /612,128,667 | 1,278 /7,449 |
| H2 UP | 165,224,887 /2,162,430,443 | 56,210,287 /978,374,763 | 14,036 /281,123 |
| Raw UP | 1,216,524 /67,448,058 | 890,334 /55,745,986 | 215 /1,010 |

In the strict restriction interior, rows16→24 span approximately8.001s. UP
class rates are8.878Mbps for mixed DOWN's return and10.042/10.029/10.004Mbps
for mixed/QUIC/H2 uploads. The broader15→25delta can include buffered
dispatch immediately after the row25restoration; it is not a pure10Mbps
window. Likewise confirmation bursts reflect buffered delivery and ACK timing,
not instantaneous physical capacity. No achieved loss percentage is inferred
from configuration or an offload-sensitive packet denominator.

H2 sends an additional1,184,055,680UP class bytes after sampled40s through85s,
while total local accepted payload is only60,096,512B and exact confirmation
never finishes. This proves continuing native traffic, not unique progress,
and does not identify its packet-level repetition mechanism. Its large wire
cost and censorship cannot be replaced with fabricated confirmation bins.

| Cell | Peak DOWN / UP backlog, B | Peak client / server RSS, KiB | Final client / server lifetime CPU, % |
|---|---:|---:|---:|
| Mixed DOWN | 23,822,356 /522,156 | 137,396 /273,616 | 58.1 /92.9 |
| Mixed UP | 55,814 /4,980,724 | 340,920 /70,712 | 34.9 /20.2 |
| QUIC UP | 52,508 /9,744,658 | 291,036 /120,420 | 24.9 /16.5 |
| H2 UP | 100,387 /38,226,744 | 223,696 /49,248 | 130.0 /96.2 |
| Raw UP | 2,914 /896,288 | no tunnel process | no tunnel process |

RSS is sampled process memory, not a post-teardown leak measurement. `ps`
CPU is lifetime average, not interval CPU or proof of a critical CPU bottleneck.
MPP management/direct-socket queries add observations absent for H2/raw;
sequential sampling and random histories prevent a packet-identical comparison.
Native QUIC identities/epochs stay stable within all three MPP cells. Client
QUIC RTT p95/max is2691.717/2691.717ms for mixed UP and2877.392/3888.579ms
for QUIC UP; these are sender-side congestion/service observations, not exact
application-gap or proof-clock attribution. No exact DSN/copy/winning-path
observer runs here, so previous diagnostic causes cannot be imported.

## Full untrimmed series and disposition

Each value is newly received/confirmed Mbit in its one-second bin, expressed
as Mbps; the final partial second is not renormalized. Upload series include
settlement beyond40s. Whole goodput uses exact bytes divided by actual elapsed
time. DOWN bins cover the40s offered window; its last read after40s can add
bytes to the whole counter without appearing in those40bins. All197available
raw bins and66download echo attempts from these initial five cells are preserved;
H2 has no valid confirmation bins, not an implicit all-zero series.

```text
Mixed DOWN: 0.466,68.622,245.987,507.562,166.987,550.847,183.411,655.407,398.470,319.051,517.013,534.869,374.002,444.846,465.063,304.349,52.044,295.073,245.809,33.758,438.384,497.093,74.960,200.842,288.642,424.855,395.113,3.712,541.715,318.710,7.340,0.584,0.818,539.318,345.457,505.182,265.011,404.960,539.448,373.154
Mixed UP: 2.193,29.145,61.822,73.304,54.238,31.886,62.486,40.990,24.405,38.938,22.308,27.167,16.349,24.213,115.868,5.051,0.000,0.000,12.155,9.533,0.000,8.485,0.000,14.060,0.000,78.119,141.462,147.870,156.505,28.120,15.705,45.863,1.147,11.069,127.018,86.233,157.945,150.330,153.623,163.910,69.104,142.562,156.096,128.031,132.924,54.536
QUIC UP: 0.096,112.913,369.859,147.272,209.988,134.382,201.221,129.676,192.962,150.052,186.281,237.682,144.882,181.477,160.736,0.000,21.933,0.000,3.439,31.073,0.000,0.000,0.000,0.000,27.962,12.199,77.355,10.486,176.589,147.388,106.192,0.000,0.000,0.000,65.659,231.155,119.448,116.409,0.000,35.724,148.401,37.646,19.941,0.000,271.268,174.028,124.540
Raw UP: 2.386,28.095,80.208,53.252,41.193,15.395,16.947,8.584,7.460,7.784,2.780,5.954,2.433,8.225,6.811,6.904,5.838,6.413,5.236,4.101,2.270,3.948,2.616,2.734,2.201,2.734,2.850,2.131,2.850,4.101,3.197,3.776,4.167,4.495,3.753,3.475,2.502,5.722,4.240,2.966,3.336,4.240,2.942,2.780,2.317,5.466,2.711,5.221,3.591,3.498,2.178,4.446,2.618,2.178,4.888,3.035,4.610,2.409,4.471,3.730,2.942,3.591,5.491,1.198
```

This set supports exact eventual mixed/QUIC upload completion and mixed
download/echo survival in the declared combined stress, with substantial
adverse timing. It does not prove fluent service, optimal mixed allocation,
normal-Internet competitiveness or a new fix. The low raw/H2 outcomes confirm
that this is also a difficult baseline environment; they do not waive MPP's
multi-second gaps or the large mixed-versus-QUIC pre-restriction difference.
Keep the existing high-capacity healthy/recovery/aggregation and browser gates.
Select any next action from a material phase/ownership question, not a target
Mbps, a faster trimmed mean or the desire to make every stress sample look good.

## Matrix continuation: QUIC/TCP DOWN and incomplete TCP UP

The next unchanged-binary sequence closes three additional captures under
`./.tmp/reflection/results/{quic,tcp}-combined-down-native-refill-combined-matrix-0910/`
and `tcp-combined-up-native-refill-combined-matrix-0910/`. The driver log is
`./.tmp/reflection/native-refill-combined-matrix-0910.log`. TCP UP reaches the
85s runner guard; the sequence stops before raw/Xray/H2 DOWN and Xray UP.
At that checkpoint those four cells were unrun. Root paused execution to
classify the incomplete MPP outcome, not replace it with a favorable run.
The final section records their subsequent once-only completion.

All168 additional rows match the same directional epoch/rate/loss/jitter and
burst/limit checks, bringing the report to453 validated rows. Neither UDP
rule toggling nor its lack of effect on TCP constitutes an equal failure
exposure between these modes.

| Cell | Rows / last elapsed, s | UP10 / restored500, s | UDP set / clear, s |
|---|---:|---:|---:|
| QUIC DOWN | 41 /40.056437 | 15.002664 /25.005704 | 30.007122 /33.055695 |
| TCP DOWN | 41 /40.136479 | 15.001656 /25.002736 | 30.003258 /33.069549 |
| TCP UP | 86 /85.151630 | 15.001886 /25.002919 | 30.003440 /33.146115 |

### Download outcomes, phases and censoring

Both bulk requests return HTTP200 and remain duration-partial, not completed
8GiB objects. TCP probe status is`ok`; QUIC probe status is`loss` despite its
bulk status`ok` and runner exit0.

| Metric | QUIC DOWN | TCP DOWN |
|---|---:|---:|
| Body bytes / elapsed, s | 1,557,133,647 /40.001072 | 2,024,776,447 /40.026772 |
| Whole body Mbps | 311.418381 | 404.684433 |
| First body, s | .405634 | .789568 |
| Maximum body gap, s | 4.540930 | .820107 |
| Gap interval, s | 30.169951–34.710881 | 37.069770–37.889877 |
| Gap body counters | 1,421,783,343→1,421,848,879 | 1,877,622,711→1,877,688,247 |
| Echo successes / I/O timeouts / unavailable | 55 /1 /14 | 56 /0 /0 |
| Successful-only echo p50 / p95 / max, ms | 297.364 /926.612 /1227.628 | 606.603 /1190.884 /1445.539 |
| Maximum successful-reply spacing, s | 1.278946, censored | 1.445561 |

QUIC echo55times out30.099035–33.099426s; the worker closes the socket and
records14later `unavailable_after_disconnect` outcomes through39.601s. There
is no successful restored echo measurement. Its worst successful echo,
21.094425–22.322053s, lasts1227.628ms during restriction. TCP's worst echo
23.132365–24.577904 lasts1445.539ms; all56succeed, but high bulk throughput
does not erase its elevated loaded latency. TCP's late gap occurs without
being exposed to the UDP outage. All126attempt records remain intact.

| Request-start phase | QUIC body Mbps; successes / failures; p95 ms | TCP body Mbps; successes; p95 ms |
|---|---:|---:|
| 0–5s | 325.090;10 /0;593.840 | 272.501;10;582.512 |
| 5–15s | 449.761;20 /0;401.786 | 435.189;16;907.781 |
| 15–25s | 300.148;15 /0;961.201 | 413.268;10;1445.539 |
| 25–30s | 435.841;10 /0;323.437 | 434.504;8;1147.325 |
| 30–33s | 23.507;0 /1;unavailable | 369.462;4;935.131 |
| 33–35s | 64.608;0 /4;unavailable | 347.383;2;872.556 |
| 35–40s | 190.613;0 /10;unavailable | 474.989;6;1349.493 |

QUIC's final phase is weaker than its earlier no-loss outage classification;
these are different conditions and histories, not a proved new controller
regression. TCP and mixed keep their echo connections, with their own timing
costs. No transport wins every service dimension in these observations.

```text
QUIC DOWN continuation: 5.601,314.801,410.185,439.449,455.415,441.030,347.500,454.505,383.961,423.511,603.947,409.645,458.302,421.795,553.411,297.806,320.875,272.663,330.320,267.840,403.864,204.170,143.597,425.421,334.925,493.263,399.603,340.504,475.402,470.434,70.522,0.000,0.000,0.000,129.215,41.622,46.170,61.742,215.514,588.015
TCP DOWN continuation: 0.466,53.418,352.322,395.313,560.988,385.876,485.491,392.692,490.209,430.440,601.358,331.186,595.067,205.982,433.585,374.344,460.951,487.064,475.005,537.919,213.492,409.927,402.653,432.441,338.884,684.720,195.948,436.208,381.954,473.688,565.235,50.251,492.900,646.338,48.429,556.779,611.045,200.285,425.401,581.435
```

### TCP upload: incomplete, with continuing drain at the guard

TCP UP confirms126,339,020of152,436,736locally accepted bytes over85.716881s;
26,097,716B remain unconfirmed. `complete=false`, one failed stream and
`upload_ack_accounting_valid=false` mean there is no exact completion rate or
valid confirmation-bin series. The JSON11.791Mbps is a censored confirmed-so-far
ratio, not accepted sustained performance. First write/confirmation are
.214000/.597734s; recorded inter-confirmation/local-write gaps are
.781732/1.623144s. These finite between-event maxima do not bound the unfinished
tail or prove that all accepted work received fluent service.

The runner guard initiates product cleanup; the resulting probe error is
`upload sink closed before terminal acknowledgement`. No independent spontaneous
native disconnect is established. The incomplete outcome itself remains real
and prevents acceptance; guard-triggered closure does not make it complete.

Independent ordinary-telemetry review narrows this case without a new observer:

- One session and all three active TCP carriers retain their identities/native
  epochs across86snapshots; no sampled suspect/failed transition appears.
- During the final5s, directly sampled native TCP ACK bytes increase9,997,740B;
  every carrier advances in each one-second interval. Sampled retransmitted
  bytes increase1,646,816B. Final aggregate NOTSENT is267,670B and RTT is about
  102–107ms. This is continuing lossy native service, not a frozen dashboard.
- Server successful target-socket `poll_write` bytes grow116,312,118→125,487,158
  over the last five management seconds:9,175,040B, approximately14.680Mbps.
  Every intervening one-second delta is positive. The target-write counter
  grows61,669,376B from sampled40→85s while client local reads reach the full
  152,436,736B by60s and then stop. Target write acceptance is not sink ACK;
  the later126,339,020B confirmation remains the authoritative partial result.

These observations support **ongoing lossy drain at the observation guard**,
not an established MPP deadlock, lost actor wake or permanently missing target
credit. They do not bound unsampled stalls or guarantee eventual completion.
Raw's completed58,327,040B is only38% of TCP MPP's accepted work; its shorter
elapsed time is not an equal-work proof that MPP should have settled earlier.
Conversely more accepted bytes and continuing native traffic do not waive
MPP's incomplete result or select a larger observation guard as the fix.

### Continuation costs and current scope

| Cell | Whole DOWN / UP class bytes | Class drops DOWN / UP | Peak backlog DOWN / UP, B |
|---|---:|---:|---:|
| QUIC DOWN | 1,767,053,931 /41,434,848 | 5,980 /13,397 | 14,787,234 /384,080 |
| TCP DOWN | 2,259,822,359 /10,598,009 | 1,106 /3,977 | 27,523,726 /107,521 |
| TCP UP | 5,697,044 /165,454,183 | 975 /4,477 | 6,094 /568,796 |

TCP UP's common row0→40class deltas are2,768,732DOWN/84,778,851UP bytes;
its whole85s cost is not a40s equal-load comparator. Peak client/server RSS is
95,932/329,156KiB for QUIC DOWN,62,332/159,372for TCP DOWN,99,608/20,172for
TCP UP. Final lifetime CPU is53.1/80.8%,24.3/43.7%,3.2/2.2% respectively.
Neither low sampled CPU nor absence of log errors proves correctness of every
owner. Existing telemetry provides coarse progress and native-cost evidence,
not exact Original/copy attribution or a physical packet-loss percentage.

This intermediate checkpoint retained277available raw bins and192download
attempt outcomes across eight cells. H2 UP and TCP UP remain censored with no
valid bins; the final section adds the four then-unrun baselines. The continued
characterization does not authorize a throughput threshold, controller tuning,
a guard extension or release.

## Final four baseline cells and full-cohort disposition

The once-only continuation closes
`./.tmp/reflection/results/{raw,xray,h2}-combined-down-native-refill-combined-matrix-0910/`
and `xray-combined-up-native-refill-combined-matrix-0910/`, using the same
ordinary profile, configurations and matrix log. All four runners return0.
All198additional service rows match the full epoch/rate/loss/jitter/limit
schedule; none is a favorable replacement for an earlier result. The twelve
cells are complete as observations, including three incomplete uploads.

| Cell | Rows / last sampled elapsed, s | UP10 / restored500, s | UDP set / clear, s |
|---|---:|---:|---:|
| Raw DOWN | 41 /40.004301 | 15.001670 /25.002718 | 30.003259 /33.003581 |
| Xray DOWN | 41 /40.004165 | 15.001605 /25.002629 | 30.003155 /33.003470 |
| H2 DOWN | 41 /40.005475 | 15.001829 /25.003678 | 30.004203 /33.004555 |
| Xray UP | 75 /74.008069 | 15.001663 /25.002728 | 30.003271 /33.003613 |

### Final download controls

All three bulk requests return HTTP200 and are duration-partial. Raw and
Xray probes are`ok`; H2 is`loss` because its echo connection times out.

| Metric | Raw TCP | Xray VMess/TCP | Hysteria2 |
|---|---:|---:|---:|
| Body bytes / elapsed, s | 1,818,077,088 /40.004428 | 1,497,488,670 /40.024457 | 725,559,151 /40.055131 |
| Whole Mbps | 363.575171 | 299.314727 | 144.912100 |
| First body, s | .355692 | .398653 | .434358 |
| Maximum body gap, s | .407663 | .366555 | 3.519419 |
| Gap interval, s | 24.135115–24.542778 | 20.019250–20.385805 | 30.256419–33.775838 |
| Gap body counters | 1,103,571,600→1,103,577,392 | 784,192,878→784,200,988 | 589,959,671→589,992,439 |
| Echo successes / I/O timeouts / unavailable | 80 /0 /0 | 79 /0 /0 | 61 /1 /13 |
| Successful echo p50 / p95 / max, ms | 123.687 /447.028 /499.744 | 101.152 /450.295 /791.494 | 205.735 /331.163 /480.340 |
| Maximum successful-reply spacing, s | .889327 | 1.196774 | .735520, censored |

H2's actual timeout is30.514007–33.516069s, followed by13unavailable records;
its successful-only latency does not measure restored echo service. Raw/Xray
remain unaffected by UDP filtering but retain the changing loss/jitter and
return restriction. Their worst echoes occur30.007111–30.506855s and
34.613972–35.405466s respectively. All234attempt outcomes and120body bins
are retained, including H2's availability failure.

| Request-start phase | Raw body Mbps; echoes / p95 ms | Xray body Mbps; echoes / p95 ms | H2 body Mbps; successful echoes / p95 ms |
|---|---:|---:|---:|
| 0–5s | 287.167;10 /190.648 | 263.042;10 /199.801 | 117.417;10 /480.340 |
| 5–15s | 390.793;20 /484.639 | 367.991;20 /450.295 | 141.564;20 /329.378 |
| 15–25s | 362.386;20 /152.938 | 240.815;20 /142.509 | 172.799;20 /221.524 |
| 25–30s | 420.885;10 /169.494 | 211.018;10 /129.631 | 190.243;10 /397.326 |
| 30–33s | 389.920;6 /499.744 | 372.057;6 /119.043 | 12.583;1 /240.268, plus timeout |
| 33–35s | 374.957;4 /141.324 | 278.817;4 /791.494 | 91.963;0 /unavailable |
| 35–40s | 310.474;10 /418.480 | 369.472;9 /771.580 | 180.069;0 /unavailable |

```text
Raw DOWN final: 4.541,247.168,393.123,405.463,385.539,392.920,400.126,386.662,406.101,309.763,462.766,492.723,132.961,491.034,432.877,407.060,414.637,410.971,324.948,279.676,465.887,402.490,349.532,317.842,250.814,375.938,469.575,395.693,448.846,414.371,361.614,458.556,349.591,406.622,343.292,295.485,303.037,281.561,306.084,366.205
Xray DOWN final: 4.259,139.238,375.239,388.438,408.036,397.605,377.747,349.916,299.866,293.472,316.140,326.673,422.534,431.834,464.125,251.160,342.334,208.368,258.469,213.175,114.430,304.126,276.175,242.847,197.062,230.082,201.311,196.138,221.541,206.019,355.299,323.545,437.326,174.108,383.525,378.261,366.158,329.977,368.105,404.857
H2 DOWN final: 125.115,95.714,139.406,108.520,118.330,89.239,87.862,74.700,117.822,136.577,177.942,208.196,188.219,174.742,160.344,151.715,175.790,185.085,154.381,176.280,205.140,191.572,148.231,147.216,192.577,176.053,207.257,188.471,164.582,214.850,37.749,0.000,0.000,44.673,139.253,187.902,180.084,148.143,212.053,172.164
```

### Xray upload closure is not the runner guard

Xray UP confirms33,862,029of33,947,648accepted bytes over74.127596s. It is
missing85,619B of confirmation and terminal settlement. The probe reports
`upload sink closed before terminal acknowledgement`, `complete=false`, one
failed stream and invalid ACK accounting; no valid confirmation bins exist.
The displayed3.654Mbps is a partial-confirmation ratio, not an exact completed
rate. First write/confirmation are.004946/.251107s; recorded maximum local
write/confirmation gaps are4.018327/.902262s, excluding unresolved terminal wait.

Unlike H2 and MPP TCP's85s guard stops, this probe finishes itself and the
runner returns0 at75.008213s, before its guard. Subsequent runner cleanup
therefore cannot explain away this earlier terminal closure. The available
records do not identify which proxy/target/half-close component ended it;
do not label it a spontaneous native-controller failure. Nearly equal byte
totals are still not exact completed delivery, and no missing bins are invented.

### Final-cell costs and explicit acceptance boundary

| Cell | Whole DOWN / UP class bytes | DOWN / UP drops | Peak DOWN / UP backlog, B |
|---|---:|---:|---:|
| Raw DOWN | 2,065,572,860 /2,012,045 | 501 /1,352 | 5,874,320 /8,134 |
| Xray DOWN | 1,722,219,312 /2,064,063 | 545 /1,443 | 4,823,604 /6,666 |
| H2 DOWN | 2,253,293,795 /33,514,392 | 5,620 /12,565 | 17,481,790 /212,139 |
| Xray UP | 1,078,349 /39,509,037 | 194 /919 | 1,410 /81,756 |

Xray UP's row0→40class deltas are710,722DOWN/26,045,892UP bytes; its whole
74s window is not equal to a40s download. Peak client/server RSS is
37,228/39,688KiB for Xray DOWN,46,856/77,032for H2 DOWN and32,932/35,272for
Xray UP. Final lifetime CPU is3.3/4.1%,77.4/66.5% and.1/.3% respectively;
raw has no tunnel process. H2 DOWN carries2.253GB in the DOWN class for only
725.559MB of useful body: material traffic cost, not an exact packet-level
retransmission or unique-copy attribution. All four stderr files are empty.

The entire current stress matrix is now executed and archived. It shows real
MPP strengths—TCP download throughput, exact mixed/QUIC upload settlement,
and mixed echo survival—but also material costs: loaded echo tails, seconds
without confirmation, sole-QUIC echo loss and incomplete TCP upload at the
guard. Raw/Xray provide lower download latency while doing different useful
work and lacking UDP-outage exposure. H2's outcomes do not justify relaxing
MPP's criteria. No one-system or all-conditions winner has been demonstrated.

Three of six uploads obtain exact terminal settlement; three do not, for
different observed closure/censoring reasons. These are not interchangeable
successes or failures, and finite native progress is not a completion proof.
No new timer, guard, queue threshold or protocol preference is selected by
this matrix. The existing independent-link aggregation, healthy/restored
experience, cold/warm requests and real Cloudflare browser gates remain;
public README promotion and release are still withheld.
