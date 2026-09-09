# Logical ACK cadence: ordinary comparison

Recorded: 2026-09-09. Category: existing mixed return-feedback service owner.
**REJECTED for promotion: useful throughput improves, but critical read and
echo tails worsen.** No healthy/upload follow-on, README claim or release is
justified by this pair. The ordinary comparator remains `b2aa215`; intermediary
candidate `8a0413d` is an unaccepted trial, not the next baseline. Root reversed
its source/RFC/tests and verified an empty src/RFC/Cargo difference against
`39dc2ad`; the ordinary executable matches the frozen scoped-ACK comparator.
The active CURRENT/PROGRESS disposition records continuation, not closure.

## Mechanism, forecast and component boundary

The final section of [SCOPED_ACK_SERVICE_MODEL](SCOPED_ACK_SERVICE_MODEL.md)
records the selected contract and its conditional proof. The current client
forces changed receipt feedback before every local write. The candidate
instead uses one logical pending-receipt owner, the existing rate-free bounded
service quantum, unique receipt `next_offset + reorder_bytes`, and a frozen
existing PTO/2 deadline. First/nonbulk/terminal and important gap feedback stay
prompt. Every materialized generation is still offered independently to all
live exact attachments. Materialized capacity retries and receive credit have
separate ownership; no per-sibling delay, preferred carrier or new knob.

Client and server retain one application write/flush future while servicing
due receipt and existing output retries. Credit for that batch follows only
successful consumption. Independent review corrected candidate-only waiter
ordering: due server generation must precede capacity reconciliation, and a
new client generation must invalidate a saved waiter covering an older subset.
Closed client outputs no longer prevent retaining the cumulative desired ACK;
that does not report successful publication or grant credit without admission.
RFC8.3 explicitly changes ACK timing rather than claiming trace equivalence.

The real publisher RED produces generations `[1,2,3,4,5]` for five small bulk
receipts after validating real queue facts and the unforced control. Candidate
GREEN is `[1,1,1,1,1]`, with an immediate terminal tail. The634 unique focused
checks include actual subsequent-bulk blocked write/flush in both directions,
first publication, deadline freezing, sparse extension, urgent gaps and credit.
These are mechanism checks, not ordinary performance acceptance. Optimized
candidate build took123s and retained one unused private helper warning
(`reliable_path_product_bdp_bytes`, remaining uses test-only). The warning is
not a performance explanation; no cleanup occurred during the pair.

Forecast was deliberately conditional: at smooth500/200Mbps,64KiB arrives in
1.05/2.62ms, so fewer callback generations could reduce return work without a
bulk stop-and-wait dependency. The previous ~80% forced-only diagnostic fraction
was not a saving forecast. Sampling, intermediate gaps, native/shared queues,
MAX and allocation remain coupled. Any materially adverse timing stops this
candidate, without a compensating sampler, threshold or controller change.

## Artifacts and effective cells

Complete inputs are retained in:

- `./.tmp/reflection/results/mixed-combined-down-ack-cadence-control-0909/`
- `./.tmp/reflection/results/mixed-combined-down-ack-cadence-candidate-0909/`

Each contains `probe.json`, `probe.err`, `service.jsonl`, `client.log` and
`server.log`. Source identity is ordinary scoped ACK `b2aa215` versus committed
candidate `8a0413d`. Both executables are ordinary optimized builds, without
observers or unsafe suppression. Root ran control then candidate, with no
concurrent build. Both runners completed successfully; candidate runner elapsed
was41.004729s. Probe stderr is empty in both cells.

The workload is one40s HTTP download plus serial64B echoes at nominal500ms
cadence and3s timeout. Three TCP carriers and QUIC share the same cut. All82
effective service samples confirm DOWN500Mbps, UP500→10→500Mbps,30/70ms delay,
zero jitter, no configured random loss and no blackhole. HTB rate equals ceil,
burst/cburst is65,536B, and netem limit is8,192 packets. Queue overflow losses
below are observed despite zero configured random loss.

| Cell | First restricted service sample, s | First restored sample, s | Entire sampled interval, s |
|---|---:|---:|---|
| Control | 15.004843 | 25.005991 | 0.000049–40.008646 |
| Candidate | 15.001708 | 25.002849 | 0.000046–40.004574 |

Probe phase boundaries are nominal probe-clock intervals, not exact packet
membership at the shaper transition. The runner records its own monotonic
origin after launching the probe but no corresponding wall timestamp.
Management timestamps are cached sampler times, not retrieval times. Socket
and router outputs are collected serially within each service sample. Do not
manufacture a subsecond cross-source join. Class costs use their own sampled
elapsed endpoints; echo phases use recorded attempt-start membership.

## Complete receiver outcomes

Both responses are HTTP200 with8GiB content length. Each is one deliberately
duration-stopped **partial** response and zero completed full-object requests;
neither is an8GiB completion result. Probe and bulk status are `ok` in both.

| Cell | Received body bytes / elapsed s | Whole Mbps | 0–5 / 5–15 / 15–25 / 25–40 raw-bin means, Mbps | First body, s |
|---|---|---:|---|---:|
| Control | 1,596,977,092 /40.000152 | 319.394 | 298.982 /414.276 /151.552 /374.835 | 0.586386 |
| Candidate | 1,673,191,328 /40.000368 | 334.635 | 293.026 /426.007 /195.641 /380.249 | 0.582960 |

| Cell | Maximum body gap, s | Gap interval, s | Body-counter endpoints, B | Echo success / failure | Echo p50 / p95 / max, ms | Max successful-completion gap, s |
|---|---:|---|---|---|---|---:|
| Control | 0.563265 | 16.820401–17.383665 | 752,211,606–752,223,606 | 78 /0 | 256.109 /656.240 /756.803 | 1.051431 |
| Candidate | 0.754914 | 22.971826–23.726741 | 929,262,848–929,274,848 | 70 /0 | 235.303 /1471.908 /1957.233 | 2.004825 |

All individual attempts are retained, including serial-cadence stretch. Fewer
successful attempts are not hidden failures or fixed-schedule missing slots:
the worker waits for each reply, so slow replies reduce the number it can make.
There are no recorded connection, mismatch or I/O failures in either series.

Echo phase entries are `attempts / p50 / p95 / max` in milliseconds. Quantiles
use the probe's sorted `round((n-1)*rank)` index, not interpolated percentiles.

| Cell | 0–5s | 5–15s | 15–25s | 25–40s |
|---|---|---|---|---|
| Control | 10 /308.198 /676.102 /676.102 | 20 /253.629 /326.030 /346.210 | 18 /469.130 /755.445 /756.803 | 30 /225.198 /370.351 /547.413 |
| Candidate | 10 /275.253 /576.602 /576.602 | 20 /290.075 /434.552 /577.374 | 11 /517.578 /1957.233 /1957.233 | 29 /208.626 /312.007 /845.112 |

Control has one attempt crossing each5s/15s boundary. Candidate has one
crossing5s and one crossing25s. These retain their start-phase membership.
The worst control echo is16.802997–17.559800s. Candidate has four large
restricted-phase episodes:15.688304–17.645537s,18.645720–20.238101s,
20.238124–21.710032s and21.710054–23.338424s. Its later35.386853–36.231966s
echo is also worse than control's restored maximum; do not erase it.

Every raw one-second body bin is below, without the probe's optional trim.
They measure buffered application reads, not instantaneous physical capacity;
bins above500Mbps must not be marketed as link throughput.

```text
second:   control   candidate  (Mbps)
 0          2.097       2.620
 1        165.675     128.854
 2        183.067     240.744
 3        725.320     722.689
 4        418.749     370.225
 5        472.575     494.911
 6        236.594     277.653
 7        592.920     568.240
 8        401.598     361.042
 9        424.088     459.504
10        446.464     437.642
11        311.250     379.835
12        454.226     397.836
13        403.540     476.813
14        399.509     406.589
15        260.210     291.395
16        119.811     127.678
17        108.512      49.761
18        202.188     429.670
19        129.831     211.914
20        138.597     177.868
21        135.527     199.546
22        110.257     221.073
23        167.126      32.673
24        143.459     214.832
25        307.396     448.897
26        332.512     390.539
27        374.177     387.025
28        408.673     377.709
29        358.403     387.139
30        337.347     389.267
31        421.271     371.775
32        409.147     378.867
33        334.187     396.539
34        499.766     377.339
35        343.691     317.590
36        342.442     368.563
37        399.623     379.485
38        378.069     372.289
39        375.826     360.710
```

## Traffic, queues and resource costs

Parse the router string as three concatenated JSON arrays, followed by process
text. Count one HTB child per direction, not parent plus child/netem. Bytes and
packet units are kernel class accounting, not codec bytes or exact physical
packets. Whole totals are last-minus-first sample counters, approximately40s.

| Cell | DOWN bytes / packet units | UP bytes / packet units | DOWN / UP peak backlog, B | DOWN / UP new drops |
|---|---|---|---|---|
| Control | 2,063,745,262 /1,528,895 | 81,593,810 /731,985 | 15,345,142 /841,196 | 0 /0 |
| Candidate | 2,116,832,650 /1,548,708 | 78,111,446 /696,338 | 13,862,680 /1,099,839 | 0 /3950 |

Total return bytes fall4.27% and packet units4.87%, far from a forecast based
on old forced-decision counts. This ordinary run has no codec/category observer,
so it cannot assign those savings to ACK, MAX, protection or native packets.

Interior cost intervals use rows2–14,16–24 and26–39, not nominal probe bins:

| Cell / service phase | Exact elapsed interval, s | DOWN bytes / packet units / Mbps | UP bytes / packet units / Mbps | UP backlog min / median / max, B | New UP drops |
|---|---|---|---|---|---:|
| Control healthy | 2.000263–14.004730 | 735,574,009 /527,177 /490.200 | 21,373,423 /199,400 /14.244 | 60,625 /124,948 /255,081 | 0 |
| Candidate healthy | 2.000291–14.001602 | 744,323,225 /527,142 /496.161 | 20,258,016 /184,647 /13.504 | 57,870 /117,067 /200,661 | 0 |
| Control restricted | 16.004944–24.005891 | 221,874,748 /177,374 /221.848 | 10,115,136 /90,250 /10.114 | 91,735 /539,984 /816,486 | 0 |
| Candidate restricted | 16.001822–24.002720 | 252,586,776 /187,924 /252.558 | 9,992,593 /84,424 /9.991 | 301,158 /908,957 /1,099,839 | 3035 |
| Control restored | 26.006094–39.008535 | 799,598,418 /596,551 /491.968 | 39,033,733 /343,370 /24.016 | 93,120 /226,600 /265,366 | 0 |
| Candidate restored | 26.002972–39.004457 | 804,373,157 /601,798 /494.942 | 36,771,803 /328,120 /22.626 | 70,787 /216,535 /365,724 | 0 |

The return class remains essentially saturated under restriction. Candidate
already has915drops at the first restricted sample, reaches3548at19.002149s
and3950at22.002492s. Its sampled peak queue length is8191 against the8192
configured limit. Control has no recorded class drops. Do not convert these
counter samples into exact per-echo loss or packet timing.

All41 samples contain one MPP process per endpoint. RSS is KiB; `%CPU` is
`ps` lifetime-average multicore utilization, not instantaneous CPU or a core
saturation proof. Management has no retrieval errors or admission rejection.

| Cell | Server peak / final RSS | Client peak / final RSS | Server / client peak reported %CPU |
|---|---|---|---|
| Control | 416,016 /369,624 | 90,592 /57,784 | 194 /109 |
| Candidate | 389,888 /389,888 | 91,752 /72,184 | 189 /105 |

Final cached management shows4active carriers,0suspect/failed and2active logical
flows in both runs. It is not a post-teardown resource-reclamation check.
Only end-of-workload reset/BrokenPipe/RemoteClosed warnings are present;
control additionally logs H3_NO_ERROR connection closure during teardown.
Those logs are retained, not described as error-free or joined to individual
echo failures. Neither probe records an echo failure.

## Existing traces narrow the failure but do not locate its blocking byte

Three established TCP carrier tuples remain stable at both endpoints through
each sampled phase. Below, restricted TCP statistics are27observations
(3sockets ×9service samples), not independent network experiments. `ss` native
RTT is transport evidence; Send-Q includes unacknowledged socket work, not a
measurement of a particular unsent packet's queue position.

| Restricted evidence | Control | Candidate |
|---|---|---|
| Server TCP native RTT min / median / max, ms | 248.458 /472.214 /720.962 | 119.780 /762.036 /902.929 |
| Client TCP native RTT min / median / max, ms | 102.293 /463.851 /715.261 | 258.624 /759.493 /914.535 |
| Client aggregate sampled Send-Q maximum, B | 282,221 | 756,621 |
| Client TCP retransmitted-byte delta, rows16–24 | 0 | 140,008 |
| Client QUIC native RTT min / median / max, ms | 169.954 /463.359 /711.597 | 477.911 /736.170 /889.447 |

QUIC remains Active with the same local physical instance1 in all41rows in
both cells. Its RTT source is native-carrier telemetry. The large return queue,
overflow, native TCP retransmissions and elevated TCP/QUIC RTTs demonstrate
worse actual return/native service during the adverse phase, not merely an
inaccurate displayed Product rate. They do not prove the new ACK deadline is
irrelevant: altered feedback, assignment and packet timing can change that
shared service, and the native and Product effects are not additive delays.

The clean100ms example gives87.5ms PTO/2, **not** a fixed runtime added-delay
bound. Selected snapshots during the restricted phase can have much larger
RTT/variation; the actual deadline freezes at first pending receipt. Ordinary
captures retain neither logical generation/deadline timestamps nor exact
winning echo/packet joins. Therefore they cannot reconstruct which receipt
expired, whether a particular echo waited at Product or native ownership, or
how much of its delay was queue versus retransmission versus service timing.
The serial socket/class/management snapshots do not justify exact wall-clock
joins to the probe's intervals.

## Outcome versus forecast and next boundary

The forecast partially holds only at the broad work/service level: return
traffic decreases and nominal restricted body service rises151.552→195.641Mbps.
But whole echo p95 more than doubles, maximum echo grows0.757→1.957s, the
restricted phase stretches from18to11attempts, and maximum body gap worsens.
The newly observed queue overflow and larger native RTT show why lower total
bytes are not sufficient to claim better service. The exact causal scheduling
change is **not attributed** by this capture, and the component proof cannot
substitute for it.

Stop and reverse this candidate under the predeclared rule. Do not rerun a
favorable average, alter the quantum/PTO, restore a guessed rate or add native
controller tuning to rescue it. No fresh observer or lab was required to
make this rejection. Continue only with an evidence-backed decision inside the
existing return-feedback owner; global failure/recovery, asymmetric upload,
aggregation and browser/baseline acceptance remain open and unchanged.
