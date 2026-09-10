# Current healthy-link panel: ordinary TCP, QUIC and mixed service

Recorded: 2026-09-10. Candidate `364d417`; information-only classification.
**MPP is not performance-accepted.** All MPP uploads settle exactly, but mixed
upload has a 5.140-second confirmation gap and a 3.523-second local write gap
on this healthy link. TCP download sustains 418 Mbps while small-request median
latency reaches 912 ms. Mixed download is slower and has higher loaded latency
than QUIC alone. Those practical failures take precedence over healthy bulk
averages; this panel does not erase the earlier adverse outage comparisons.

## Question, provenance and measurement boundaries

The predeclared question was whether the current ordinary model's substantial
shortfall remains in a serviceable, high-capacity environment across carrier
modes and existing baselines. This is a classification gate before another
runtime attempt, not a predicted gain or a favorable repeat. Root ran the fixed
six-mode DOWN and UP cohort without source/build changes, using the frozen
ordinary `return-round-20260910/mptunnel` executable for MPP. DOWN order was
TCP, QUIC, mixed, raw, Xray, H2; UP order was raw, Xray, H2, TCP, QUIC, mixed.
No result is selected from older controls.

Inputs are `./.tmp/reflection/results/` directories named
`{raw,xray,h2,tcp,quic,mixed}-combined-{down,up}-return-round-healthy-panel-0910`.
Root created and listed the [56-file raw archive](RETURN_ROUND_HEALTHY_PANEL_20260910.raw.tar.gz):
all twelve cells, including every probe result, stderr, service sample and
available proxy log; no configurations/keys. It is authoritative for full
precision, all echo attempts, byte counters and native samples. No public
README/PERFORMANCE, release, or universal-optimality claim follows this report.

All 495 service rows verify the same shared cut: 500 Mbps in both directions,
DOWN 30 ms and UP 70 ms propagation, zero configured jitter/loss/QoS/blackhole,
HTB rate equal to ceil, 65,536-byte burst/cburst and 8,192-packet netem limit.
Actual class and qdisc drop deltas are zero in every cell. These are matched
profiles, not identical packet histories or controller/allocation outcomes.
MPP TCP has three carriers, QUIC one, and mixed three TCP plus one QUIC; this
is not independent-link aggregation. H2 retains explicit 500/500 Mbps bandwidth
priors; MPP uses unconfigured discovery. Xray is the existing VMess/TCP baseline.
MPP alone receives management/native socket sampling, so observation costs are
not perfectly equal. No browser or concurrent-object acceptance is implied.

DOWN is one 8 GiB HTTP response stopped after nominal 40 seconds, with a separate
serial 64-byte TCP echo at nominal 500 ms cadence and 3-second timeout. Slow
replies reduce the number of attempts; unissued slots are not packet failures.
UP is one 40-second offered upload followed by target-acknowledged settlement;
its elapsed time includes drain. It has no concurrent echo worker. First UP
delivery means receipt of a target confirmation, not the target's first-byte
timestamp. Phase means use untrimmed one-second bins; echo phases use attempt
start. Quantiles use sorted index `round((n-1)*rank)`, matching the probe.
Service commands are serially sampled on a different clock from probe events.

## DOWN: complete outcomes and sustained timing

All six probes report `ok`, HTTP 200, one intentional duration-partial response,
zero complete 8 GiB objects and empty stderr. All 443 attempted echoes succeed.
TCP has only 43 attempts because replies commonly exceed the requested cadence;
the other modes each have 80. The final TCP echo ends at 40.350208 seconds.

| Mode | Body bytes | Elapsed, s | Mbps | First body, s | Max read gap, s | Echo p50 / p95 / max, ms |
|---|---:|---:|---:|---:|---:|---:|
| Raw | 2,232,465,584 | 40.000492 | 446.488 | 0.403892 | 0.100188 | 103.161 / 124.020 / 296.899 |
| Xray | 2,245,034,683 | 40.000599 | 449.000 | 0.407909 | 0.200742 | 103.439 / 128.438 / 303.224 |
| H2 | 2,326,492,294 | 40.000236 | 465.296 | 0.406709 | 0.113070 | 111.462 / 114.342 / 117.893 |
| MPP TCP | 2,091,832,642 | 40.002656 | 418.339 | 0.579507 | 0.403428 | 912.220 / 1275.696 / 1566.306 |
| MPP QUIC | 2,131,158,906 | 40.000038 | 426.231 | 0.446726 | 0.100669 | 104.960 / 159.770 / 324.426 |
| MPP mixed | 1,946,224,852 | 40.000972 | 389.236 | 0.579636 | 0.277228 | 266.651 / 428.942 / 570.491 |

Mixed is 8.68% below QUIC goodput with 2.68 times its echo p95. QUIC is 8.40%
below H2 goodput; TCP is 6.30% below raw but much worse for small requests.
These are empirical gaps within this cohort, not code-cause attribution.

| Mode | Max body-gap interval, s | Worst echo interval, s | Max successful-echo spacing, s |
|---|---|---|---:|
| Raw | 39.779279–39.879468 | 2.000548–2.297447 | 0.685997 |
| Xray | 31.974712–32.175454 | 2.000769–2.303993 | 0.677773 |
| H2 | 37.085072–37.198142 | 19.007404–19.125297 | 0.506782 |
| MPP TCP | 32.531290–32.934718 | 32.214065–33.780372 | 1.566328 |
| MPP QUIC | 0.547169–0.647838 | 2.000794–2.325220 | 0.605642 |
| MPP mixed | 0.579636–0.856864 | 20.510024–21.080515 | 0.741960 |

The following fixed windows contain no profile transitions. Each entry is
`body Mbps; echo count / p50 / p95 ms`; startup and unfavorable late windows
remain included rather than using only trimmed averages.

| Mode | 0–5 s | 5–15 s | 15–25 s | 25–40 s |
|---|---|---|---|---|
| Raw | 360.464; 10 / 101.958 / 296.899 | 474.361; 20 / 103.294 / 122.991 | 455.120; 20 / 102.558 / 124.020 | 450.808; 30 / 103.321 / 120.332 |
| Xray | 355.481; 10 / 103.127 / 303.224 | 454.400; 20 / 103.818 / 119.814 | 472.484; 20 / 103.177 / 124.058 | 460.905; 30 / 103.430 / 124.621 |
| H2 | 433.677; 10 / 110.484 / 114.232 | 471.513; 20 / 111.294 / 114.342 | 470.402; 20 / 111.761 / 112.661 | 468.289; 30 / 111.594 / 114.889 |
| MPP TCP | 307.015; 8 / 616.858 / 1114.180 | 438.553; 11 / 936.813 / 1300.743 | 432.886; 10 / 967.179 / 1275.696 | 432.311; 14 / 912.220 / 1206.996 |
| MPP QUIC | 342.404; 10 / 104.098 / 324.426 | 431.714; 20 / 107.667 / 140.234 | 439.698; 20 / 106.847 / 139.395 | 441.536; 30 / 104.447 / 154.820 |
| MPP mixed | 296.313; 10 / 180.975 / 347.178 | 424.839; 20 / 287.622 / 428.942 | 408.944; 20 / 324.178 / 460.064 | 383.341; 30 / 234.369 / 423.007 |

## UP: exact settlement is necessary, but does not hide the mixed stall

Raw and all three MPP modes confirm every locally accepted byte and report
`complete=true`, valid terminal accounting, zero failed streams and `ok`.
Xray and H2 instead report `loss`, one failed stream and the explicit error
`upload sink closed before terminal acknowledgement`. Their terminal settlement
is incomplete despite runner exit zero. Their confirmed totals/rates are lower
bounds, not exact completed throughput, nor proof those unconfirmed bytes were
lost in the network. Their missing confirmation-bin arrays cannot be reconstructed
from unrelated counters. Every stderr is empty.

| Mode | Target-confirmed / locally accepted bytes | Elapsed, s | Confirmed Mbps | Exact settlement | First write / confirmation, s | Max confirmation / write gap, s |
|---|---:|---:|---:|---|---:|---:|
| Raw | 2,273,771,520 / 2,273,771,520 | 40.370967 | 450.576 | Yes | 0.104135 / 0.204489 | 0.302120 / 0.400616 |
| Xray | 2,286,658,677 / 2,290,548,736 | 41.649976 | ≥439.214 | No | 0.004758 / 0.206677 | 0.244464 / 0.584736 |
| H2 | 2,340,364,267 / 2,351,628,288 | 40.152908 | ≥466.290 | No | 0.105453 / 0.206539 | 0.211901 / 0.115870 |
| MPP TCP | 2,196,832,256 / 2,196,832,256 | 41.800679 | 420.440 | Yes | 0.105799 / 0.409187 | 0.477220 / 0.522963 |
| MPP QUIC | 2,279,211,008 / 2,279,211,008 | 41.345195 | 441.011 | Yes | 0.105787 / 1.101029 | 0.246492 / 0.637107 |
| MPP mixed | 1,926,627,328 / 1,926,627,328 | 41.553761 | 370.918 | Yes | 0.105263 / 0.409023 | 5.140055 / 3.523470 |

Mixed has four consecutive zero-confirmation bins (seconds 2–5), followed by
770.381 Mbps in second 6. That catch-up burst and eventual exact settlement do
not make the 5.140-second service interruption acceptable. QUIC's first
confirmation is also delayed to 1.101 seconds; the reported maximum confirmation
gap excludes the initial wait, so quoting its 0.246-second gap alone is misleading.
Raw elapsed exceeds the offered period by 0.371 seconds; MPP TCP/QUIC/mixed by
1.801/1.345/1.554 seconds, respectively. Those differences are elapsed beyond
the nominal load period, not isolated network drain measurements.

| Mode | Confirmation Mbps, 0–5 s | 5–15 s | 15–25 s | 25–40 s |
|---|---:|---:|---:|---:|
| Raw | 361.872 | 461.648 | 478.036 | 451.385 |
| MPP TCP | 358.613 | 413.873 | 432.957 | 430.825 |
| MPP QUIC | 356.755 | 451.170 | 452.644 | 449.604 |
| MPP mixed | 17.864 | 413.459 | 411.541 | 423.315 |

## Cost and native context, not per-byte causal attribution

Class totals below count one shaped class per direction, not duplicate
parent/child/netem accounting. They include all traffic sharing that class,
not just application payload or MPP ACKs. Each capture uses its actual first
and last sample; QUIC DOWN ends at 39.209 seconds rather than approximately 40,
and UP captures can end before final settlement. Do not treat class/payload
ratios as exact wire amplification or compare their differences without those
window boundaries. Sampled peak backlog is not an exact maximum or a blocking
byte's queue position.

| Cell | Rows / sampled end, s | DOWN / UP class bytes | Peak DOWN / UP backlog, B |
|---|---:|---:|---:|
| Raw DOWN | 41 / 40.004983 | 2,340,348,988 / 2,618,673 | 14,221,132 / 6,402 |
| Xray DOWN | 41 / 40.004178 | 2,373,787,340 / 2,654,273 | 13,916,851 / 5,874 |
| H2 DOWN | 41 / 40.004580 | 2,459,504,120 / 16,369,249 | 2,735,576 / 29,527 |
| MPP TCP DOWN | 41 / 40.005186 | 2,397,037,161 / 21,897,422 | 15,928,076 / 45,452 |
| MPP QUIC DOWN | 40 / 39.208822 | 2,238,712,770 / 34,749,864 | 5,981,976 / 83,568 |
| MPP mixed DOWN | 41 / 40.007171 | 2,390,329,445 / 45,176,942 | 20,981,782 / 242,879 |
| Raw UP | 41 / 40.004395 | 2,627,993 / 2,366,331,118 | 2,706 / 12,501,098 |
| Xray UP | 42 / 41.004505 | 2,713,722 / 2,411,205,405 | 2,772 / 11,987,852 |
| H2 UP | 41 / 40.004789 | 16,655,450 / 2,469,425,258 | 14,723 / 5,157,217 |
| MPP TCP UP | 42 / 41.004631 | 22,600,173 / 2,469,741,906 | 20,133 / 16,910,604 |
| MPP QUIC UP | 42 / 41.005468 | 35,215,310 / 2,411,065,960 | 36,739 / 15,140,196 |
| MPP mixed UP | 42 / 41.026025 | 38,863,439 / 2,238,212,102 | 65,961 / 31,649,090 |

RSS is process KiB; `ps` CPU is lifetime average percentage (100% is one core),
not interval CPU utilization. Final means last sampled process, not post-teardown
retention. Raw has no tunnel process; its CPU/RSS are not measured zero.

| Cell | Client RSS peak / final | Server RSS peak / final | Final client / server CPU, % |
|---|---:|---:|---:|
| Xray DOWN | 37,404 / 36,712 | 37,364 / 36,428 | 10.9 / 6.0 |
| H2 DOWN | 42,748 / 27,908 | 45,492 / 45,492 | 91.2 / 82.3 |
| MPP TCP DOWN | 45,344 / 37,588 | 169,220 / 161,040 | 59.3 / 128.0 |
| MPP QUIC DOWN | 36,916 / 36,916 | 290,612 / 284,968 | 98.2 / 153.0 |
| MPP mixed DOWN | 91,796 / 91,796 | 358,300 / 354,552 | 89.3 / 193.0 |
| Xray UP | 36,856 / 36,644 | 35,364 / 34,872 | 5.1 / 13.7 |
| H2 UP | 44,760 / 44,760 | 43,028 / 43,028 | 83.1 / 93.0 |
| MPP TCP UP | 154,332 / 133,168 | 56,112 / 35,880 | 90.0 / 43.0 |
| MPP QUIC UP | 514,272 / 502,060 | 34,904 / 34,904 | 124.0 / 102.0 |
| MPP mixed UP | 329,668 / 311,364 | 125,076 / 124,008 | 158.0 / 88.8 |

TCP DOWN's live server sockets identify kernel `bbr`; pooled sampled TCP RTT
median is 212.946 ms, while BBR minimum-RTT median is 100.042 ms. These are
different from its 912.220 ms end-to-end echo median. After 5 seconds, management
TCP RTT medians are 215.437 ms server / 214.550 ms client. Mixed's corresponding
TCP values are 249.803/264.443 ms and QUIC 261.168/265.146 ms; QUIC-only is
101.165/101.073 ms. Thus mixed has substantial native/common-path inflation too.
Unmatched pooled medians cannot be subtracted to assign exact actor/writer delay,
and native RTT is not application completion. The panel identifies a material
TCP service gap and mixed upload stall without choosing their precise cause.

Proxy logs retain expected duration-close/reset/broken-pipe messages and H2's
duration-cancel warning in DOWN; QUIC close messages also occur in UP. These
are not additional failed probe requests. Baseline UP terminal-ack failure is
explicitly retained above despite absence of WARN/ERROR log messages.

## Full timing series

DOWN entries are the untrimmed `[second, second+1)` application-body Mbps bins.
Values above 500 Mbps describe buffered application read timing, not a claimed
physical link rate. All 443 individual echo intervals remain in the raw archive;
the phase and worst-interval tables above are summaries, not replacements.

| Second | Raw | Xray | H2 | MPP TCP | MPP QUIC | MPP mixed |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 5.653 | 5.281 | 281.991 | 2.580 | 9.605 | 2.597 |
| 1 | 368.603 | 360.245 | 470.976 | 308.808 | 366.288 | 109.432 |
| 2 | 475.905 | 466.281 | 472.747 | 426.770 | 451.368 | 199.325 |
| 3 | 476.450 | 471.659 | 468.713 | 350.558 | 446.789 | 725.305 |
| 4 | 475.709 | 473.941 | 473.956 | 446.360 | 437.972 | 444.906 |
| 5 | 470.913 | 474.880 | 467.118 | 532.677 | 419.999 | 436.503 |
| 6 | 476.044 | 474.821 | 471.405 | 413.163 | 383.541 | 435.255 |
| 7 | 476.357 | 472.518 | 473.511 | 486.614 | 430.410 | 380.057 |
| 8 | 475.523 | 471.269 | 469.362 | 435.203 | 443.434 | 546.352 |
| 9 | 474.365 | 474.460 | 468.589 | 457.964 | 434.529 | 420.709 |
| 10 | 470.252 | 472.350 | 471.254 | 433.228 | 447.118 | 371.505 |
| 11 | 474.203 | 474.135 | 472.578 | 301.028 | 382.976 | 353.914 |
| 12 | 476.044 | 432.094 | 472.795 | 391.284 | 456.790 | 496.507 |
| 13 | 475.871 | 323.670 | 474.298 | 472.220 | 452.614 | 396.434 |
| 14 | 474.040 | 473.806 | 474.218 | 462.148 | 465.730 | 411.154 |
| 15 | 464.171 | 474.247 | 472.874 | 459.103 | 433.281 | 399.332 |
| 16 | 284.990 | 471.758 | 471.620 | 406.455 | 462.907 | 409.607 |
| 17 | 475.349 | 471.701 | 471.608 | 450.881 | 394.262 | 452.386 |
| 18 | 472.569 | 471.994 | 465.818 | 465.567 | 437.916 | 418.642 |
| 19 | 477.840 | 472.233 | 470.002 | 435.230 | 449.332 | 388.922 |
| 20 | 473.148 | 473.616 | 469.376 | 452.289 | 462.189 | 436.644 |
| 21 | 474.423 | 474.491 | 472.530 | 413.663 | 422.908 | 370.391 |
| 22 | 476.160 | 474.730 | 468.305 | 337.498 | 436.801 | 428.125 |
| 23 | 476.740 | 468.929 | 472.017 | 446.384 | 454.873 | 403.377 |
| 24 | 475.813 | 471.140 | 469.866 | 461.793 | 442.513 | 382.016 |
| 25 | 477.782 | 473.551 | 469.920 | 416.519 | 443.208 | 437.982 |
| 26 | 473.728 | 475.480 | 470.129 | 429.893 | 454.760 | 350.965 |
| 27 | 475.222 | 474.113 | 472.383 | 439.558 | 441.751 | 290.553 |
| 28 | 397.563 | 472.324 | 469.920 | 471.101 | 443.046 | 512.850 |
| 29 | 366.367 | 473.821 | 470.264 | 426.974 | 439.281 | 405.582 |
| 30 | 476.797 | 474.491 | 472.930 | 448.352 | 391.581 | 364.503 |
| 31 | 473.519 | 458.910 | 471.312 | 443.300 | 432.202 | 360.544 |
| 32 | 476.473 | 295.709 | 468.028 | 295.513 | 446.388 | 418.393 |
| 33 | 477.145 | 474.286 | 469.968 | 341.180 | 454.759 | 326.487 |
| 34 | 474.886 | 475.335 | 471.155 | 546.308 | 436.911 | 399.043 |
| 35 | 475.234 | 474.901 | 468.976 | 469.341 | 428.968 | 352.412 |
| 36 | 476.044 | 473.471 | 473.249 | 391.674 | 455.797 | 369.442 |
| 37 | 476.276 | 472.669 | 433.403 | 476.466 | 447.870 | 364.633 |
| 38 | 477.898 | 470.211 | 471.825 | 456.246 | 449.369 | 408.319 |
| 39 | 287.191 | 474.302 | 470.878 | 432.243 | 457.142 | 388.406 |

UP untrimmed target-confirmation Mbps, from second zero through the final
partial settlement bin (41 raw bins; 42 in each MPP cell):

```text
Raw: 5.653,273.197,572.840,478.500,479.172,477.203,479.346,479.809,478.709,476.276,478.825,316.903,476.543,473.867,478.998,478.315,475.002,477.423,477.886,478.998,478.130,479.693,477.782,477.666,479.462,479.172,225.737,479.983,479.311,479.346,478.767,479.230,478.245,479.404,479.346,475.477,478.211,322.638,478.883,477.029,213.194
TCP: 16.776,390.071,374.342,540.541,471.334,196.609,687.342,450.887,211.813,689.963,374.342,174.587,385.352,631.767,336.069,372.244,614.466,459.801,390.070,506.461,375.915,223.347,379.585,600.834,406.847,412.616,503.841,457.702,440.927,448.267,440.402,354.943,328.204,426.246,423.101,466.091,421.004,462.422,436.208,440.401,434.635,416.286
QUIC: 0.000,402.128,474.104,461.802,445.741,462.362,463.248,457.749,468.809,401.228,452.704,454.247,459.106,441.925,450.319,401.091,457.797,461.587,461.011,468.863,453.886,456.330,462.152,447.259,456.461,467.237,390.537,454.285,448.163,458.944,456.417,459.987,456.670,470.042,454.846,459.513,458.736,398.812,452.995,456.883,463.375,204.340
Mixed: 22.017,67.301,0.000,0.000,0.000,0.000,770.381,320.864,528.054,440.690,319.579,402.985,495.740,500.414,355.880,312.631,663.711,80.881,573.218,500.944,484.717,191.574,197.387,719.615,390.735,467.621,210.764,679.714,346.695,309.906,474.909,307.535,362.985,619.280,544.610,323.138,353.642,245.508,770.290,333.122,422.613,301.366
Xray/H2: absent because terminal acknowledgement failed; not zero-filled.
```

## Disposition

The information forecast is met: this current matched cohort exposes material
healthy-link problems that an aggregate Mbps gate would miss. Mixed upload's
five-second interruption is a higher-impact next attribution target than a small
healthy-rate difference. TCP's sustained small-request delay also remains a
practical concern, distinct from the measured native/common-path inflation.
Exact mechanism attribution belongs to the active plan's bounded follow-up;
neither inferred cause nor a new threshold change is accepted here. All previous
outage, random-loss, aggregation, bidirectional and browser gates remain intact.

## Separate TCP direct-echo companion: useful but limited overlap

This predeclared follow-up is not another panel entry or a full-duration matched
control. Root used the unchanged ordinary TCP-only binary/profile with the
existing direct-echo helper, bypassing MPP while sharing the remote echo service,
cut and hosts. The question was whether direct echo inherits roughly 0.9-second
latency or stays near the roughly 0.2-second native/common-path delay while MPP
echo remains slow. No runtime, socket-buffer, controller or profile change was
made. Inputs are `./.tmp/reflection/direct-echo-tcp-return-round-0910.jsonl` and
the five files in `./.tmp/reflection/results/tcp-combined-down-return-round-direct-echo-0910/`.
Root created and listed the separate [seven-file archive](RETURN_ROUND_TCP_DIRECT_ECHO_20260910.raw.tar.gz), including the helper.

The companion starts at Unix 1789015626.296837 seconds, but the first foreground
server/client management generations are 1789015662.352/5662.356 seconds: about
36 seconds later. Only its final roughly 14 seconds overlap foreground activity.
There is no exact ordinary-probe request wall-clock anchor and no post-load quiet
margin. Therefore the full 50-second direct median of 100.288 ms is mostly quiet
time, **not loaded latency**. All 100 direct attempts succeed without disconnect.

| Observation band | Successful attempts | p50 / p95 / max, ms |
|---|---:|---:|
| Direct offsets 0–35 s, quiet | 70 | 100.278 / 100.379 / 100.465 |
| Direct offsets 36–50 s, includes load transition | 28 | 202.849 / 238.801 / 306.360 |
| Direct offsets 42–46 s, conservative loaded interior | 8 | 206.424 / 218.267 / 218.267 |
| MPP foreground offsets 6–10 s, nearby overlap band | 5 | 869.616 / 979.631 / 979.631 |
| MPP foreground offsets 5–11 s, wider overlap band | 7 | 863.044 / 979.631 / 979.631 |

The direct interior is Unix 1789015668.296837–1789015672.296837 seconds.
The foreground bands have deliberate margins around the management anchor;
they are overlapping observations, not paired requests. Server native TCP
RTT samples at foreground service offsets 6–10 seconds range 196.707–219.203 ms;
aggregate native NOTSENT ranges 32,920,756–53,959,373 bytes. These are unsent
socket bytes, not physical flight, an exact echo queue position or a memory leak.
Do not subtract the unmatched medians to manufacture a measured MPP stage delay.

Across the whole foreground capture, HTTP 200 and `ok` accompany an intentional
duration-partial 2,088,353,044-byte body in 40.000306 seconds (417.667 Mbps), first
body at 0.583877 seconds. Maximum body gap is 0.400881 seconds at
11.239049–11.639930 seconds. All 45 tunnel echoes succeed; p50/p95/max are
890.719/1237.213/1380.460 ms. Stderr is empty. All 41 service rows retain the
healthy 500/500 Mbps, 30/70 ms profile with no class/qdisc drops or blackhole.
The sampled class window ends at 40.004413 seconds: DOWN/UP bytes are
2,384,432,656/22,194,513; peak backlogs are 11,060,182/48,564 bytes. Full foreground
bins and both attempt histories remain in the separate raw evidence.

The limited simultaneous separation supports a material carrier-local service
component beyond the approximately 200 ms common delay in this early slice.
It does not identify a specific FIFO, actor, native-write or MPP admission cause,
establish full-run direct behavior, or justify a queue threshold. This partial
discriminator is useful without rerunning it into a falsely cleaner control;
the higher-impact mixed upload stall remains the active priority.
