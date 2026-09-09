# TCP Product evidence observation — 2026-09-09

Status: **ordinary-policy diagnostic, not an improvement comparison or release
acceptance**. The healthy mixed capture receives398.006Mbps with80/80 successful
echoes,293.756/408.021/639.834ms median/p95/maximum echo latency and a.273840s
maximum body-read gap. Those outcomes describe the observed workload; they do
not establish that a different rate model improves performance.

## Question and observation boundary

CURRENT_CLOSURE_PLAN predeclared one narrow discriminator: does the actual TCP
Product completion input consumed by response preparation expire or erode while
the native diagnostic rate remains fresh? The preceding live-hedge ablation
identified a material recovery/allocation cost, not the mechanism connecting
copy ambiguity, numeric Product evidence and subsequent placement.

Continuously fresh Product inputs would falsify numeric collapse in this
capture. Observed expiry would justify reviewing that owner, not automatically
restoring a kernel rate: reduced allocation can also reduce Product evidence.
Ambiguity, durable qualification and numeric evidence must remain separate.
The shared scalar also participates in request feedback batching, so a proposed
change is not automatically rank-only or timing-neutral.

The fixed-stream observer records actual computed preparation snapshots under
the existing output lock: initial/source/freshness/qualification transitions
and periodic min/max/last values accumulated over observations. These are not
selected or admitted placement decisions, time-weighted samples or receiver
throughput. Advisory and fenced preparation calls are both covered. Product
and native advisory values retain separate identities and provenance. Exact
rate-event attribution and final per-output observation coverage are reported
separately from the workload accounting below; an unflushed tail must not be
silently treated as a complete lifetime record.

## Provenance and effective profile

Ordinary comparatorb2aa215 is observed with a ten-source-file feature overlay,
including the existing Original/copy/receipt accounting. No suppression,
protocol preference, congestion-control or native-rate substitution is enabled.
The clean optimized build takes3m36s. Parent freezes and fully removes the
overlay before traffic, then restores and byte-compares the ordinary executable.
No runtime/RFC change is accepted by this diagnostic.

Inputs are `./.tmp/reflection/tcp-product-evidence-build-0909.log`,
`tcp-product-evidence-run-0909.log`, and
`results/mixed-combined-down-tcp-product-evidence-0909/`. The latter contains
`probe.json`, empty `probe.err`, `service.jsonl`, `server.log` and `client.log`.
The runner exits0 after41.009478s. Existing HTB quantum warnings remain visible;
the profile was not adjusted to remove them.

[Raw archive](TCP_PRODUCT_EVIDENCE_20260909.raw.tar.gz),288100B, retains the five
complete result files, ten-file observer patch, build/run logs, wrapper and
derived cost JSON. Gzip integrity and decompressed byte-for-byte comparisons
against all ten inputs pass. Frozen executable is
`./.tmp/reflection/bin/tcp-product-evidence-20260909/mptunnel`; no binary is
archived. Source/RFC and ordinary executable restoration were verified before
starting the next separately recorded diagnostic transaction.

All41 effective service samples independently confirm500Mbps in both directions,
30ms DOWN/70ms UP, zero jitter, no configured loss and no blackhole. HTB rate
and ceiling are62500000B/s, burst/cburst65536; netem limit8192 throughout. All
class and qdisc drop deltas are zero. The chronological epoch counter changes
without changing those effective settings; it is not evidence of QoS here.
There is no15/25s restriction or recovery transition in this capture.

The workload is one duration-limited8GiB HTTP response plus persistent64B TCP
echoes scheduled every500ms with3s timeout. It is not Cloudflare or a concurrent
browser acceptance test. The bulk worker starts after the first successful
echo or its existing readiness timeout; first-body timing includes that prelude.
Successful echo timing begins at request send, excluding initial SOCKS/connect
setup. All times below are relative to the shared workload start unless stated.

## Full useful service and timing

| Outcome | Observed |
|---|---:|
| Received body B /duration s | 1990114072 /40.001662 |
| Whole useful goodput Mbps | 398.006280 |
| First body s | .579869 |
| Maximum body-read gap s | .273840 |
| Maximum-gap start → end s | .579869→.853709 |
| Body bytes before /after that gap | 58192 /123728 |
| HTTP code /requests /complete /duration-partial | 200 /1 /0 /1 |
| Echo successes /attempts /failures | 80 /80 /0 |
| Echo request /response B | 5120 /5120 |
| Echo p50 /p95 /maximum ms | 293.756 /408.021 /639.834 |
| Maximum successive echo-completion gap s | .931634 |

The complete8GiB body is not claimed: the successful response is intentionally
stopped at40s. The maximum body gap is immediately after first body, not a
late steady-state stall. All80 echo entries have sequential unique indices,
successful outcomes and consistent finite start/end/latency values. There is
no disconnect or recorded echo error. First echo spans.103152→.203754s; the last
spans39.661052→39.874663s. No failure or out-of-window completion is removed.

| Healthy chronology slice | Body Mbps | Echo n | Echo p50 /p95 /max ms |
|---|---:|---:|---|
| 0–5s | 297.273 | 10 | 237.284 /322.563 /322.563 |
| 5–15s | 421.018 | 20 | 284.403 /482.915 /639.834 |
| 15–25s | 419.821 | 20 | 330.047 /396.032 /424.010 |
| 25–40s | 401.731 | 30 | 283.212 /359.717 /476.113 |

Body means use every raw bin. Echo slices use attempt start; quantiles use the
probe's rounded `(n−1)p` index, including ties-to-even, rather than interpolation
or a different percentile convention. Recalculation matches the probe's whole
p50/p95/maximum. These slices share one healthy condition, not four independent
controlled experiments. The trimmed421.387Mbps field is not substituted for
the398.006Mbps whole workload result.

| Slowest echo /index | Start → end s | Latency ms |
|---|---|---:|
| 16 | 8.010705→8.650539 | 639.834 |
| 23 | 11.651173→12.134089 | 482.915 |
| 66 | 33.157188→33.633301 | 476.113 |
| 39 | 19.652955→20.076965 | 424.010 |
| 21 | 10.650844→11.058865 | 408.021 |

Largest successful-completion spacing is7.718906→8.650539s (attempts15→16).
It includes scheduled spacing and service latency; it is not another body gap
or interchangeable with the639.834ms echo maximum.

`probe.err` is empty. Client Broken-pipe at09:36:58.182UTC coincides with body
cancellation; server RemoteClosed follows09:36:58.258 and H3_NO_ERROR shutdown
09:36:59.188. These retained closure warnings are not failed probe requests or
new lifecycle attribution.

## Actual prepared-rate evidence

Root and independent consumer analysis agree on131 events covering509539
actual prepared observations across three exact TCP outputs. All remain in
qualification epoch1 with active authority; no output becomes stale or loses
its durable assignment qualification after gaining it. These are server stream1
identities, not management flow ordinals or interchangeable client incarnations.

| TCP path /physical /output incarnation | Observations | First numeric Product /durable qualification s | Scalar min–max Mbps after5s | Last scalar Mbps /observation s |
|---|---:|---|---|---|
| 0 /4 /2 | 169153 | 1.100 /1.208 | 16.175190–79.132167 | 41.823363 /39.228 |
| 1 /2 /1 | 169480 | .926 /1.291 | 22.603976–188.434833 | 60.805597 /39.310 |
| 2 /3 /3 | 170906 | 1.145 /1.234 | .350751–86.855345 | 4.306521 /39.783 |

These times use the **server observer's monotonic origin**, not the probe
workload origin used by the earlier timing tables. The extrema use accumulated
interval reports at/after5s; the first interval can straddle5s. Counts are not selected bytes or elapsed
time weights. TCP0/1 retain Product-sourced scalar input after startup. TCP2
has two measured fallback episodes despite qualified native advisory rates:

| Observer time s | Product state /actual scalar Mbps | Native advisory Mbps | Eligible ACK B | Ambiguity-excluded ACK B |
|---|---|---:|---:|---:|
| 29.751 | expired /.350751 | 38.446480 | 151992105 | 3105956 |
| 29.765 | absent /.350751 | 38.446480 | 151998833 | 3105956 |
| 30.130 | qualified /10.543652 | 38.446480 | 152480757 | 3105956 |
| 37.598 | expired /.350751 | 22.462712 | 158039125 | 3135156 |
| 37.841 | absent /.350751 | 17.551416 | 158075125 | 3135156 |
| 38.332 | fresh unqualified /.350751 | 17.551416 | 158247733 | 3135156 |
| 38.783 | qualified /2.226438 | 17.551416 | 158337369 | 3135156 |

The logged actual scalar remains the startup prior for379ms and1185ms. The old
raw Product rate at each expiry is9.981447/4.721340Mbps, aged886464/1006492us;
native advisory remains fresh. At38.332s,172608B of a new numeric epoch produce
raw2.809413Mbps but not yet numeric qualification; by38.783s262244B qualify at
2.226438Mbps. This is numeric evidence rebuilding, not revoked durable authority.

Crucially, ambiguity is unchanged through both episodes:3105956B is already
present at23.242s, and3135156B at31.130s. Eligible bytes continue increasing,
and other exclusions remain zero. With stable incarnation and no revocation,
Original admission is reconstructible as the change in all three released-byte
categories plus the change in exact Original flight. This gives113536B during
28.245–29.245s and12000B during36.133–37.133s, approximately.908/.096Mbps;
both intervals end with0 Original debt while native work remains. Thus actual
Original admission declines before both expiries, without another counter hook.
Thus the capture proves numeric fallback with fresh native advice but does
**not** establish contemporaneous repair ambiguity as its cause, nor prove
that fallback initiated the lower allocation. A feedback-loop effect remains
distinct from that unsupported one-way attribution.

Final emitted eligible/ambiguity/other counters are204146307/3212982/0B,
403677961/189800/0B and159031529/3135156/0B for paths0/1/2. Eligible values
match the emitted Product Original-ACK counters; total observation increments
also reconcile. Their sums766855797/6537938/0B are **censored per-output totals**,
not a complete whole-run partition: last observations differ at39.228/39.310/
39.783s and the observer explicitly marks the unflushed final interval censored.
They must not be equated with all repair bytes or the final global perf totals.

## Aggregate observation and resource accounting

All795 server and829 client perf rows pass interval-count/byte/time sums versus
their cumulative totals. There is one PID per role and no invalid-receipt
component. Absence means no observed invalid arithmetic event, not a fabricated
zero-valued metric. Inactive components can stop emitting before the final
global flush, so rows do not all have a common final observation timestamp.

| Payload accounting B | TCP | QUIC | Total |
|---|---:|---:|---:|
| Accepted Originals, all lanes | 781701983 | 1230414297 | 2012116280 |
| Accepted persistent-gap copies | 201585771 | 2635040 | 204220811 |
| Accepted live-tail copies | 65680904 | 1055232 | 66736136 |
| All accepted copies | 267266675 | 3690272 | 270956947 |
| Receiver new | 779555347 | 1212057765 | 1991613112 |
| Receiver duplicate | 265824671 | 3690272 | 269514943 |
| Receiver ordered-triggered | 254473893 | 1735669507 | 1990143400 |

No other accepted repair cause or requalification component is emitted in the
flushed capture. Receiver149582 DATA calls exactly partition into TCP37887 and
QUIC111695 calls; all receipt triplets align in count. New+duplicate exactly
equals2261128055B of mux input. New−ordered leaves1469712B buffered at that
observation, not proof of a post-teardown leak. Ordered-triggered bytes can
release a suffix received on another path, so they are not ingress ownership
or completed application delivery. Source admission, received bytes and body
completion have different transit, cancellation and flushing boundaries.

Server TCP encoded plaintext is1050114985B/38240 calls. QUIC encoded bytes are
1237964193B/9471 calls, but successful write-wait is1237635493B/9470 calls: one
328700B encoded transaction lacks successful write-wait coverage at closure.
Those domains are deliberately not equated. Client return encoded TCP/QUIC is
18269561/7008046B; client QUIC encode/write-wait totals agree. Encoded sizes are
not physical-wire packet counts or native retransmission accounting. Perf
bookkeeping's1us floor and concurrent aggregate timers are not critical delay.

| Sampled cost | Observed |
|---|---:|
| DOWN /UP class byte deltas | 2404663708 /96010363 |
| DOWN /UP class packet-counter deltas | 1768396 /846536 |
| Peak DOWN /UP class backlog B | 20817226 /282719 |
| Client peak /final RSS KiB | 90628 /90628 |
| Server peak /final RSS KiB | 358456 /355892 |
| Client peak /final ps CPU % | 127 /127 |
| Server peak /final ps CPU % | 202 /202 |
| Client QUIC RTT p50 /p95 /max ms | 259.113 /390.121 /398.683 |
| Server QUIC RTT p50 /p95 /max ms | 265.650 /388.649 /396.395 |
| Server QUIC flight p50 /p95 /max B | 9937488 /15189372 /16475090 |

The sampled class window spans40.009256s, not exactly body duration or perf
flush intervals. Packet counters are offload-sensitive. Queue peaks are sampled,
not continuous maxima or exact critical-byte position; CPU is process-lifetime
multicore utilization, not interval cause attribution. Sampled RSS cannot prove
leak freedom. Parent's independently derived
`./.tmp/reflection/tcp-product-evidence-costs-0909.json` agrees with these costs
and verifies one stable QUIC physicalinstance1, session and role-specific native
epoch throughout all41 samples. Server median QUIC RTT remains265.650ms on a
100ms-propagation link. Native advisory rate/RTT and actual Product inputs remain
separate; none of these aggregate costs alone establishes rate-expiry causation.

## Disposition

The diagnostic supplies complete ordinary-policy workload evidence and actual
preparation inputs for the predeclared question. Two numeric expiries are real,
but flat ambiguity and earlier declining allocation do not support the proposed
direct ambiguity-causes-collapse sequence. Other TCP outputs do not expire.
Changing that scalar is not thereby proven to improve ordered service.
Event-driven counts are observation-weighted, not time-weighted; an observed
prepared input is not an accepted choice. No sampler arithmetic defect is
established. The next separately predeclared score-only counterfactual tests
whether existing qualified local TCP carrier advice changes Throughput Original
placement usefully, without changing returned/admission snapshots, feedback,
repair, Latency/echo lanes or QUIC. Native socket service includes copies and
other traffic; it is not this flow's unique service. That source substitution
could overpredict service and increase queueing; it is not a selected model fix.
No healthy/impairment comparison, baseline victory, safe hedge suppression,
native-rate restoration or release acceptance follows from this single run.
README/PERFORMANCE remain unchanged.

## Complete raw timing series

All40 one-second body Mbps bins, including startup; bursts above500Mbps reflect
application read timing and do not establish physical capacity above the link.
Original-precision timing and outcomes remain in the raw probe JSON.

```text
body = [2.621,110.5,274.616,625.89,472.737,419.855,291.038,548.363,225.207,633.903,404.12,320.944,487.352,467.684,411.719,411.08,464.732,331.19,532.689,349.524,434.369,434.601,466.293,363.441,410.287,455.229,408.19,373.352,376.374,428.388,417.026,433.44,408.402,249.288,497.831,402.288,370.342,397.755,398.685,409.376]
```

All80 attempts below; offsets rounded to6 decimals and milliseconds to3 only
for display. Every row is successful; no success-only subset is substituted.

```csv
index,start_s,end_s,latency_ms,outcome
0,0.103152,0.203754,100.602,success
1,0.502744,0.603944,101.200,success
2,1.002808,1.320974,318.166,success
3,1.502872,1.825435,322.563,success
4,2.002950,2.319842,316.891,success
5,2.503039,2.740323,237.284,success
6,3.003095,3.104519,101.423,success
7,3.503176,3.676832,173.656,success
8,4.003257,4.261221,257.964,success
9,4.503348,4.764268,260.920,success
10,5.003522,5.217326,213.803,success
11,5.503600,5.788003,284.403,success
12,6.003680,6.269412,265.732,success
13,6.503743,6.759869,256.126,success
14,7.003817,7.290070,286.253,success
15,7.504584,7.718906,214.322,success
16,8.010705,8.650539,639.834,success
17,8.650559,8.843060,192.501,success
18,9.150642,9.311332,160.690,success
19,9.650727,9.830256,179.529,success
20,10.150773,10.483744,332.970,success
21,10.650844,11.058865,408.021,success
22,11.150917,11.487464,336.546,success
23,11.651173,12.134089,482.915,success
24,12.151286,12.375715,224.429,success
25,12.651340,12.935049,283.709,success
26,13.151400,13.495255,343.855,success
27,13.651472,14.019272,367.799,success
28,14.151553,14.447257,295.704,success
29,14.651740,14.879868,228.128,success
30,15.151830,15.498321,346.491,success
31,15.651983,16.043081,391.099,success
32,16.152343,16.497290,344.947,success
33,16.652436,16.996615,344.179,success
34,17.152484,17.449459,296.975,success
35,17.652616,17.980744,328.128,success
36,18.152690,18.474920,322.230,success
37,18.652764,18.959981,307.217,success
38,19.152887,19.548919,396.032,success
39,19.652955,20.076965,424.010,success
40,20.153039,20.534801,381.762,success
41,20.653118,20.965517,312.399,success
42,21.153219,21.405131,251.912,success
43,21.653322,21.931539,278.216,success
44,22.153397,22.359492,206.095,success
45,22.653473,22.763662,110.189,success
46,23.153634,23.466006,312.372,success
47,23.653700,24.034547,380.847,success
48,24.153805,24.510724,356.919,success
49,24.653868,24.983915,330.047,success
50,25.153985,25.477449,323.464,success
51,25.654060,26.008503,354.443,success
52,26.154105,26.419410,265.306,success
53,26.654184,26.923749,269.564,success
54,27.154265,27.429641,275.376,success
55,27.654341,27.937553,283.212,success
56,28.154434,28.263931,109.497,success
57,28.654499,28.922343,267.844,success
58,29.154590,29.473474,318.884,success
59,29.655042,29.902887,247.845,success
60,30.155129,30.447593,292.464,success
61,30.655230,31.014947,359.717,success
62,31.155286,31.506041,350.755,success
63,31.655433,31.965322,309.889,success
64,32.156533,32.459728,303.194,success
65,32.656668,32.950423,293.756,success
66,33.157188,33.633301,476.113,success
67,33.657241,33.760926,103.685,success
68,34.158965,34.354013,195.048,success
69,34.659058,34.913332,254.274,success
70,35.159131,35.421227,262.096,success
71,35.659211,35.976565,317.354,success
72,36.159265,36.473090,313.825,success
73,36.659365,36.977118,317.753,success
74,37.160696,37.440459,279.763,success
75,37.660776,37.986291,325.516,success
76,38.160862,38.446316,285.454,success
77,38.660934,38.902079,241.145,success
78,39.161026,39.289172,128.146,success
79,39.661052,39.874663,213.611,success
```
