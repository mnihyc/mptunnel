# Response prepared service after capacity restoration

2026-09-08. Four restored-link and five capacity-collapse-only ordinary cells are complete.
**No performance acceptance. Bulk recovers in both cohorts, but every QoS-only
echo probe times out, and adverse mixed timing and costs remain.** This report follows the
[22:10 forecast](CURRENT_CLOSURE_PLAN.md) and
[mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md). The preceding
[verified mechanism and healthy/harsh results](RESPONSE_PREPARED_SERVICE_20260908.md)
remain separate evidence.

Independent audit verified the restored phase/resource/epoch fields and the
QoS interior S/T, physical-class and Native-domain joins. All360 embedded raw
bins match their probes exactly. The
[raw archive](RESPONSE_RESTORED_SERVICE_20260908.raw.tar.gz) preserves43 result
files, nine run logs and the unchanged two runner/shaper sources (54 members).
Gzip integrity, exact membership and byte-for-byte comparisons pass. It does
not contain a new harness or any runtime correction.

## Question, fixed setup and interpretation

The forecast asks whether retained mixed-mode state continues to damage service
after impairment ends while a singleton and baseline recover. This is not a
repeat seeking a favorable harsh-profile number. The existing
`./.tmp/reflection/run.py` `reorder-recovery` scenario runs a 40-second
download with 64-byte loaded TCP echoes at a nominal 500ms cadence and 3s
timeout. Both routed directions retain 500Mbps capacity. Initially download
has 70ms delay/20ms jitter/3% random loss and return has 30ms delay/5ms jitter/1%
loss. At nominal 8s the existing runner removes jitter and loss, retaining
70+30ms propagation and the original queues for the remaining 32s. There is
no QoS collapse or UDP blackhole in this first cohort.

Order and executable identity are fixed:

1. Mixed control: ordinary `d999fea`,
   `./.tmp/reflection/bin/ack-support-20260908/mptunnel`.
2. Mixed candidate: response prepared ownership, checkpoint `0449b9f`,
   frozen ordinary `./.tmp/reflection/bin/response-claim-20260908/mptunnel`.
3. QUIC candidate: the same frozen candidate.
4. Existing Hysteria2: unchanged configuration with 500Mbps up/down hints.

MPP initial-rate hints remain unset. Hysteria2's configured priors are not
identical to MPP's startup. No observer, runtime change, queue adjustment,
threshold or restarted-path intervention is introduced. The independent
initial loss realizations are not packet-identical controls. All four runner
results are exit 0; elapsed times are 41.007742/41.007042/41.008976/41.009963s.

The predeclared severe persistent-collapse hypothesis is not reproduced:
even the old mixed control regains sustained delivery without restarting.
This does not prove instantaneous recovery or eliminate a lesser mixed-mode
penalty. Candidate mixed is lower than both old mixed and candidate QUIC in
the final 20 seconds. Those differences alone do not identify retained
allocation, native congestion history, physical queueing or actor cost as
the cause. The earlier harsh UP receive-handoff and already-native delays
remain unresolved.

## Complete user timing

Every bulk request is HTTP 200 for an 8GiB object. All cells deliberately stop
the load at about 40s: **0 complete objects / 1 duration-limited partial object**
each. `bulk_status=ok` means the duration-limited probe succeeds, not that an
8GiB object completes. Times below are seconds and rates are decimal Mbps;
all body bytes are exact. There are no failed or censored echo attempts.

| Cell | Body bytes | Body time | Whole-run Mbps | First body | Maximum read gap | Exact maximum-gap interval |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Mixed control | 1559729098 | 40.000893 | 311.939 | 0.593049 | 0.387542 | 0.669835–1.057377 |
| Mixed candidate | 1660023040 | 40.005617 | 331.958 | 0.684370 | 0.418070 | 0.684370–1.102440 |
| QUIC candidate | 1861940998 | 40.000052 | 372.388 | 0.511709 | 0.412389 | 6.238780–6.651169 |
| Hysteria2 | 1839123524 | 40.003145 | 367.796 | 0.426870 | 1.241019 | 7.896407–9.137426 |

| Cell | Echo success/attempts | p50 / p95 / maximum, ms | Maximum successful-response gap, s |
| --- | ---: | ---: | ---: |
| Mixed control | 79/79 | 163.182 / 287.066 / 1145.711 | 1.476370 |
| Mixed candidate | 78/78 | 170.761 / 383.212 / 1185.524 | 1.405799 |
| QUIC candidate | 80/80 | 107.639 / 232.173 / 817.512 | 1.067643 |
| Hysteria2 | 79/79 | 113.564 / 538.772 / 707.074 | 0.707096 |

Nominal echo cadence is not a promise of exactly 80 attempts: requests are
serialized, so a slow reply shifts later request starts. Mode/session-wide
carrier membership also does not prove which exact outputs were eligible for
an individual echo. No slow-echo carrier attribution is available here.

### Recovery and adverse phases

These are arithmetic means of every raw 1s bin in the specified windows,
without trimming. The windows describe the declared restoration and retained
tail, not newly invented recovery thresholds.

| Cell | Initial 0–8s Mbps | Transition 8–10s Mbps | 10–20s Mbps | Final 20–40s Mbps | Echo maximum for requests starting 20–40s, ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mixed control | 10.312 | 5.447 | 395.771 | 421.332 | 443.788 |
| Mixed candidate | 139.921 | 285.452 | 374.426 | 392.256 | 703.350 |
| QUIC candidate | 153.748 | 332.796 | 422.338 | 438.824 | 253.725 |
| Hysteria2 | 47.264 | 172.048 | 468.577 | 465.224 | 118.753 |

The old mixed control stays low in bins 8 and 9, then has a 629.613Mbps
buffered-delivery bin at 10; its final 20s mean is 421.332Mbps. Candidate
mixed delivers substantially more during initial impairment, but its final
20s mean is 392.256Mbps versus QUIC's 438.824 and Hysteria2's 465.224.
Its first body, maximum read gap, whole-run echo p95 and restored late echo
maximum are all worse than the old mixed control. Thus the higher 40s mean
331.958 versus 311.939Mbps is not a clean recovery improvement.

Hysteria2's 1.241019s maximum gap crosses the restoration boundary and bin 8
is zero; it subsequently sustains about 468Mbps through most of the restored
window. MPP's global maximum gaps occur before restoration, are below 0.419s,
and bound every other recorded read gap in these cells. The probe does not
retain each individual bulk read event, so the low raw bins cannot be turned
into an exact missing-prefix or native-recovery interval. The probe's unused
`bulk_recovery_gap_s=0` is not proof of zero recovery time: its failover
trigger is disabled in this scenario.

### Sampled stage and epoch checks

For this DOWN workload, S is server reliable `io.to_peer_bytes` (locally read
source) and T is client `io.from_peer_bytes` (locally delivered response).
These aggregate counters include HTTP framing and echo traffic. S is not
claimed C, T is not mux F, and neither is exactly the bulk-body counter.

| Cell / sample seconds | S, bytes | T, bytes | Router DOWN HTB backlog, bytes |
| --- | ---: | ---: | ---: |
| Mixed control / 8.001580 | 77407530 | 10313266 | 131565 |
| Mixed control / 10.001794 | 78718506 | 11675178 | 100619 |
| Mixed control / 11.001876 | 153279466 | 88226218 | 6062316 |
| Mixed control / 40.007582 | 1623861530 | 1557871226 | 10654506 |
| Mixed candidate / 8.001940 | 205797697 | 139922017 | 2694328 |
| Mixed candidate / 10.002154 | 276380225 | 210475009 | 1937604 |
| Mixed candidate / 11.002258 | 319455969 | 253907969 | 5575690 |
| Mixed candidate / 40.006903 | 1721922885 | 1655957669 | 1719996 |
| QUIC candidate / 8.001861 | 219513974 | 153298678 | 2218212 |
| QUIC candidate / 10.002041 | 299141334 | 234921430 | 6567696 |
| QUIC candidate / 11.002146 | 353738614 | 292720534 | 4127166 |
| QUIC candidate / 40.008811 | 1925162518 | 1860122966 | 0 |

The first `jitter_removed=true` samples are at elapsed
8.001580/8.001940/8.001861/8.001072s in the declared order. Router snapshots
show unchanged 70/30ms delays, zero jitter and removed random-loss fields;
500Mbps class rates remain. A sample's elapsed value is not an atomic
timestamp for both shaping operations. All 41 service rows per cell retain
the full before/after history.

MPP per-side generated Unix-ms endpoint ranges are respectively
1788876874634–1788876914633 (client) and
1788876874634–1788876914634 (server);
1788876960896–1788877000895 and 1788876960897–1788877000897;
1788877082121–1788877122121 and 1788877082120–1788877122120.
For exact joins use each side's generated timestamp, not an assumed common
instant from the runner row. Hysteria2 has no corresponding Product
management source/delivery fields. Stable sampled PID pairs are
305660/311471, 306607/312413, 307559/313350 and 308507/314291
(client/server), with no observed restart. Final snapshots are end-of-load
observations, not post-load settled-state measurements.

## Sampled resources and physical traffic

RSS is KiB. CPU is the `ps %CPU` process-lifetime average at each observation,
not exclusive handler time, an instantaneous utilization sample or additive
critical-path delay. Values above 100% can reflect multiple cores. More bytes
were delivered in some cells; these totals are not normalized equal-work
cost comparisons.

| Cell | Client RSS peak / last | Server RSS peak / last | Client CPU max / last, % | Server CPU max / last, % |
| --- | ---: | ---: | ---: | ---: |
| Mixed control | 143544 / 95660 | 358196 / 338176 | 123 / 123 | 143 / 143 |
| Mixed candidate | 124468 / 96224 | 402624 / 383096 | 132 / 131 | 178 / 178 |
| QUIC candidate | 56392 / 56392 | 292096 / 286368 | 87.3 / 87.3 | 133 / 133 |
| Hysteria2 | 46720 / 42704 | 221956 / 221956 | 93.6 / 93.6 | 87.8 / 86.6 |

| Cell | DOWN bytes / packets / drops | UP bytes / packets / drops | Maximum DOWN / UP class backlog, bytes |
| --- | ---: | ---: | ---: |
| Mixed control | 1909697147 / 1639230 / 833 | 251720187 / 802215 / 204 | 15262242 / 606898 |
| Mixed candidate | 2094923809 / 1774457 / 1560 | 138014113 / 858055 / 415 | 16324792 / 380687 |
| QUIC candidate | 2022873357 / 1627754 / 1270 | 36894440 / 315409 / 334 | 8270478 / 46105 |
| Hysteria2 | 2388312979 / 1930267 / 19020 | 24371470 / 215535 / 543 | 39704778 / 89741 |

Traffic values are **router HTB class first→last deltas**, not combined
parent+child qdisc sums. They include protocol framing, feedback, retransmission,
copies and the interactive workload; they are not repair-only overhead.
Hysteria2's larger sampled peak DOWN backlog and drop count are contextual
physical costs, not proof of a particular record's delay or transport defect.
Candidate mixed server RSS and sampled lifetime-average CPU are higher than
control (peak 402624 versus 358196KiB; CPU max 178 versus 143%). Neither
the differing work nor the independent random realizations permit a causal
CPU-cost attribution from this single cohort.

## Full untrimmed bulk timing

Each list is raw 1s Mbps in index order; `0–9` denotes bins [0,1) through
[9,10), and so on. Above-500 bins represent buffered ordered application
delivery, not a physical wire-capacity claim. All 160 bins are preserved.

### Mixed control

```text
0–9: 0.466, 23.718, 19.923, 13.107, 8.913, 6.291, 6.291, 3.787, 6.175, 4.719
10–19: 629.613, 440.210, 190.550, 689.862, 401.551, 305.781, 22.544, 830.045, 374.150, 73.400
20–29: 855.123, 403.938, 338.025, 384.259, 388.401, 376.823, 424.577, 364.380, 355.252, 440.241
30–39: 468.233, 439.929, 386.510, 368.753, 466.019, 417.110, 432.293, 384.327, 388.918, 343.529
```

### Mixed candidate

```text
0–9: 0.466, 17.882, 275.561, 78.451, 15.204, 2.621, 457.317, 271.864, 303.422, 267.483
10–19: 347.795, 425.919, 412.462, 281.495, 423.155, 411.182, 280.354, 204.645, 664.938, 292.315
20–29: 507.601, 367.016, 371.148, 338.863, 387.648, 372.488, 368.214, 368.515, 360.873, 396.719
30–39: 381.431, 371.900, 397.655, 418.430, 242.802, 548.105, 444.942, 423.560, 379.033, 398.183
```

### QUIC candidate

```text
0–9: 1.241, 150.585, 165.439, 166.768, 209.082, 169.410, 139.320, 228.141, 283.321, 382.270
10–19: 461.204, 351.185, 373.513, 443.820, 439.695, 445.125, 439.238, 380.392, 444.910, 444.303
20–29: 449.261, 448.501, 444.560, 444.287, 427.747, 432.805, 463.919, 464.931, 388.827, 434.353
30–39: 438.242, 453.755, 426.432, 442.122, 454.882, 433.402, 426.533, 447.178, 425.328, 429.407
```

### Hysteria2

```text
0–9: 63.336, 78.248, 63.569, 44.212, 44.757, 54.002, 14.316, 15.669, 0.000, 344.095
10–19: 465.990, 467.927, 469.238, 472.121, 466.778, 467.343, 467.403, 469.488, 471.003, 468.481
20–29: 468.765, 471.495, 470.468, 469.682, 467.663, 470.408, 470.588, 468.956, 467.373, 464.934
30–39: 468.166, 467.183, 468.169, 468.189, 467.350, 467.980, 465.425, 466.497, 415.064, 460.133
```

## Raw evidence and next declared discriminator

The four result directories under `./.tmp/reflection/results/` are:

- `mixed-reorder-recovery-down-response-claim-restored-control-0908/`
- `mixed-reorder-recovery-down-response-claim-restored-candidate-0908/`
- `quic-reorder-recovery-down-response-claim-restored-candidate-0908/`
- `h2-reorder-recovery-down-response-claim-restored-candidate-0908/`

Each retains `probe.json`, `probe.err`, `service.jsonl`, `client.log`
and `server.log`. All probe error files are empty. The four root-owned
run logs are `./.tmp/reflection/response-claim-0908-restored-{control,mixed,quic,h2}.log`.
The full echo attempt series, raw timings and physical snapshots remain
available there; no samples or failures are trimmed away.

The separately declared capacity-collapse ablation is reported below. Its
question and outcomes remain distinct from this initial-loss restoration.

## Capacity-collapse-only ablation: five complete ordinary cells

The predeclared next question uses the same frozen binaries and existing
`combined` runner with `REFLECTION_NO_LOSS=1`,
`REFLECTION_NO_JITTER=1` and `REFLECTION_NO_BLACKHOLE=1`.
`REFLECTION_NO_QOS` is not enabled: DOWN capacity changes
500→10Mbps at nominal15s and 10→500Mbps at25s; UP stays500Mbps.
The delays stay70/30ms. No runtime, resource-window, packet-queue or protocol
prior is changed. Order is control mixed, candidate QUIC, candidate mixed,
raw TCP, Hysteria2. These are five new ordinary ablation cells, not diagnostic
instrumentation and not a repeated favorable selection from either earlier
cohort.

**Outcome versus forecast:** none shows persistent bulk collapse after the
capacity cut ends. The final10s averages are405.506/439.528/451.067/455.680/
470.396Mbps in that order. Candidate mixed still has materially poorer
ordered service during QoS and worse first body/echo timing than old mixed;
its2.833589s maximum read gap is inside the10Mbps interval, not a long
post-restoration hold. The original persistent-damage hypothesis is not
established in this ablation. This does not waive the mixed penalty during
the cut, startup/cost regressions or the independent harsh UP receive boundary.

### Exact completion, gaps and loaded experience

All runner exits are0, with elapsed41.004724/41.004528/41.085079/
41.004519/41.010786s. All HTTP responses are200; again every8GiB object is
duration-limited partial (0 complete/1 partial), not fully transferred.
Every probe has `bulk_status=ok` but overall `status=loss` because its
loaded echo fails. All `probe.err` files are empty.

| Cell | Body bytes | Body time, s | Whole-run Mbps | First body, s | Maximum read gap, s | Exact gap interval, s |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Mixed control | 1500245008 | 40.000608 | 300.044 | 0.439916 | 0.936306 | 24.042014–24.978320 |
| QUIC candidate | 1596443720 | 40.000261 | 319.287 | 0.408140 | 0.106309 | 0.408140–0.514449 |
| Mixed candidate | 1521698264 | 40.001943 | 304.325 | 0.585181 | 2.833589 | 22.140152–24.973740 |
| Raw TCP | 1646031376 | 40.000748 | 329.200 | 0.404615 | 0.100382 | 36.884904–36.985286 |
| Hysteria2 | 1754588102 | 40.000257 | 350.915 | 0.407623 | 0.029989 | 16.440743–16.470731 |

| Cell | Echo successes / attempts | Timeout / later unavailable slots | Successful p50 / p95 / max, ms | Exact timed-out request interval, s |
| --- | ---: | ---: | ---: | --- |
| Mixed control | 30/75 | 1 / 44 | 313.051 / 417.523 / 439.476 | 15.005934–18.007605 |
| QUIC candidate | 30/75 | 1 / 44 | 103.790 / 314.590 / 315.401 | 15.011086–18.013799 |
| Mixed candidate | 28/74 | 1 / 45 | 347.181 / 567.749 / 1202.045 | 14.826725–17.829635 |
| Raw TCP | 30/75 | 1 / 44 | 103.383 / 281.183 / 298.408 | 15.002770–18.005673 |
| Hysteria2 | 30/75 | 1 / 44 | 111.388 / 115.396 / 118.832 | 15.003611–18.004542 |

Successful-response maximum gaps are respectively0.692173/0.697472/1.284762/
0.681062/0.508079s. They exclude the subsequent failed wait. The timeout
closes the probe's single echo connection. Later unavailable slots are not
new network requests or post-restoration reconnect attempts; the finite
capture cannot show whether a newly opened echo would recover after25s.
Successful-only latency percentiles cannot hide the lost service.

The raw TCP and Hysteria2 controls also time out at the same capacity cut.
Therefore this timeout pattern alone is not evidence of an MPP-specific
ownership defect. Candidate mixed's timeout begins14.826725s, before the
nominal cut, and straddles it; its pre-cut successful maximum1.202045s is
also worse than control0.439476s. The candidate failure must not be summarized
as an exclusively post-cut latency penalty.

### Full phase comparison and restoration validity

| Cell | 0–15s Mbps | 15–25s Mbps | 25–30s Mbps | 30–40s Mbps |
| --- | ---: | ---: | ---: | ---: |
| Mixed control | 391.518 | 10.965 | 392.885 | 405.506 |
| QUIC candidate | 402.625 | 11.466 | 444.428 | 439.528 |
| Mixed candidate | 382.763 | 5.397 | 373.481 | 451.067 |
| Raw TCP | 422.346 | 10.240 | 434.669 | 455.680 |
| Hysteria2 | 456.531 | 12.294 | 472.342 | 470.396 |

These means use every raw bin and are not a new recovery threshold.
Transition bins mix buffered delivery and shaping timing: e.g. candidate
mixed bin25 is486.050Mbps but bin29 is117.965 and bin30 is705.504.
Its stronger final10s mean does not erase those variations or prove a
causal speed gain over the other independent cells. Bin values exceeding
the instantaneous10Mbps or500Mbps line rate are not physical-capacity claims.

All205 service rows agree with the declared schedule: zero jitter, no random
loss field, no UDP blackhole, class rates62,500,000→1,250,000→62,500,000B/s
on DOWN and62,500,000B/s on UP. First reduced/restored sample elapsed times
in run order are15.001683/25.002723,15.001815/25.002860,
15.081987/25.083069,15.001681/25.002732 and15.007989/25.009079s.
Each sample's per-side and router collection is sequential; do not align
subsecond probe bins and these snapshots as a single atomic clock.

### QoS cut: busy physical service, lower ordered conversion

The interior interval below uses each cell's own service rows17→25
(one-based), safely within the10Mbps phase. It does not normalize mismatched
probe bins. Logical T is the client aggregate delivered response counter.
No Product counterpart is available for raw TCP or Hysteria2.

| Cell | Client Unix-ms start→end | S increase, B | T increase, B | DOWN HTB bytes / packets / drops | DOWN backlog start→end, B |
| --- | --- | ---: | ---: | ---: | ---: |
| Mixed control | 1788877371708→1788877379708 | 7713332 | 7689332 | 9983330 / 7307 / 0 | 26125942→19838480 |
| QUIC candidate | 1788877462420→1788877470419 | 4069568 | 9478784 | 9950040 / 6660 / 0 | 12945510→9720954 |
| Mixed candidate | 1788877596110→1788877604110 | 3735552 | 3473408 | 9881946 / 6944 / 0 | 38861536→32078500 |

Server Unix-ms bounds for these rows are1788877371708→1788877379709
(control),1788877462421→1788877470421 (QUIC), and
1788877596115→1788877604115 (candidate mixed). The corresponding elapsed
intervals are16.001797→24.002606,16.001913→24.002759 and
16.082100→24.082947s. Candidate mixed does not leave the shared physical
cut unused: it carries9,881,946B while delivering3,473,408 logical bytes,
and tens of MB remain queued. A narrower candidate interval at client
Unix1788877602110→1788877604110 has T+393216B while the DOWN class
sends2,379,640B and backlog falls34,451,540→32,078,500B.

This establishes poorer ordered conversion beside continuing physical
service, **not an exact duplicate fraction**. Management's logical totals
exclude reinjection/retransmission; it publishes no Original/copy counters,
exact missing range or reorder-occupancy series here, and response-path
`data_level_bytes_in_flight` is null. The difference can include useful
out-of-order bytes, repair/native retransmission, framing and changing
in-flight residence. These totals cannot distinguish them.

Candidate native sender evidence is also changing, not globally silent.
Within server session1335993094403592641, the four exact path/native epochs
stay stable over the interior interval. Native ACKed increases by280912B
for TCP(path0,instance4),4,009,512B for TCP(1,2),1,308,992B for TCP(2,3)
and3,380,282B for QUIC(0,1). These are native domains, not unique Product
ACK progress or repair byte totals. QUIC srtt reaches7.579s by row24,
while TCP(1,2) reaches8.240s and TCP(2,3)8.402s at row25.
A persistent queue after a50× capacity reduction is a competing physical/
congestion-history explanation; no exact record's queue position or
removable wall-clock saving is established.

After restoration, candidate T goes724297620→774138556 between client
Unix1788877605109 and1788877606110; it then reaches1515843624 by the last
sample. This directly contradicts a persistent post-restoration delivery
freeze in this cell, without asserting that every native path or echo
connection has recovered.

### QoS-only resources and traffic

The same RSS KiB, process-lifetime `ps %CPU`, directional HTB and
unequal-work caveats from the first cohort apply. Raw TCP has no tunnel
process, so its Product/tunnel RSS and CPU are unavailable rather than zero.

| Cell | Client RSS peak / last | Server RSS peak / last | Client CPU max / last, % | Server CPU max / last, % |
| --- | ---: | ---: | ---: | ---: |
| Mixed control | 97816 / 97816 | 298364 / 298364 | 88.6 / 87.9 | 131 / 119 |
| QUIC candidate | 37604 / 37604 | 347228 / 334328 | 89 / 73.5 | 139 / 116 |
| Mixed candidate | 104096 / 94060 | 280868 / 280868 | 82.9 / 75.3 | 166 / 139 |
| Raw TCP | — | — | — | — |
| Hysteria2 | 27772 / 27296 | 47588 / 46356 | 87.6 / 70.1 | 78.1 / 62.6 |

| Cell | DOWN bytes / packets / drops | UP bytes / packets / drops | Maximum DOWN / UP class backlog, B |
| --- | ---: | ---: | ---: |
| Mixed control | 1807624759 / 1311937 / 0 | 200553686 / 539416 / 0 | 26125942 / 460693 |
| QUIC candidate | 1694053931 / 1133911 / 0 | 26552256 / 245984 / 0 | 15429528 / 56625 |
| Mixed candidate | 1792445798 / 1287832 / 0 | 94631416 / 469730 / 0 | 39323940 / 262842 |
| Raw TCP | 1728375568 / 1141634 / 0 | 1943395 / 29434 / 0 | 16543608 / 2904 |
| Hysteria2 | 1856633100 / 1293043 / 0 | 12035588 / 150037 / 0 | 8738493 / 14269 |

The physical-class sampled drop deltas are zero in every cell; do not
translate that into a proof of no native retransmission or no loss anywhere.
The queues remain large during QoS, including raw TCP and Hysteria2.
Candidate mixed peak DOWN backlog39,323,940B exceeds old mixed26,125,942B.
This is an observed cost/latency tradeoff boundary, not evidence that all
queued bytes are avoidable copies.

Sampled tunnel PID pairs remain stable: control308972/314751,
QUIC309924/315695, mixed310873/316638, Hysteria2 312273/318028.
MPP client/server Unix-ms endpoint ranges are
1788877355707–1788877395707 /1788877355709–1788877395708;
1788877446419–1788877486419 /1788877446420–1788877486423;
1788877580110–1788877620109 /1788877580115–1788877620114.
These end-of-load observations are not a settled RSS or ownership-leak test.

### Full untrimmed QoS-only timing

All200 raw1s Mbps bins follow. Echo attempt timestamps and failure classes
are retained in the original probe files, not replaced by their successful
percentiles.

#### Mixed control

```text
0–9: 2.596, 163.959, 136.027, 701.209, 351.509, 440.498, 247.796, 736.240, 338.823, 397.270
10–19: 654.002, 310.192, 518.876, 470.152, 403.625, 27.514, 6.754, 7.707, 6.432, 7.200
20–29: 8.470, 11.630, 6.063, 7.067, 20.816, 244.198, 393.358, 311.479, 618.070, 397.321
30–39: 339.131, 352.722, 514.158, 416.616, 408.312, 418.105, 426.960, 399.257, 385.793, 394.010
```

#### QUIC candidate

```text
0–9: 9.607, 376.247, 452.608, 450.792, 443.075, 455.591, 439.080, 444.884, 383.971, 456.138
10–19: 417.966, 458.097, 449.516, 429.069, 372.739, 9.510, 9.489, 9.427, 9.456, 9.575
20–29: 9.474, 9.342, 9.489, 9.533, 29.364, 424.632, 449.273, 451.046, 459.541, 437.646
30–39: 426.520, 466.996, 436.711, 380.156, 440.288, 453.122, 450.962, 433.149, 458.192, 449.181
```

#### Mixed candidate

```text
0–9: 2.621, 109.170, 202.040, 712.411, 359.498, 528.438, 365.818, 349.002, 441.825, 441.894
10–19: 535.106, 352.312, 461.681, 441.121, 438.508, 26.180, 5.243, 3.146, 4.194, 3.670
20–29: 4.194, 3.670, 2.621, 0.000, 1.049, 486.050, 255.574, 611.979, 395.837, 117.965
30–39: 705.504, 401.080, 457.799, 443.644, 398.982, 461.567, 384.375, 452.099, 413.676, 391.946
```

#### Raw TCP

```text
0–9: 5.699, 366.274, 475.882, 476.647, 469.036, 477.492, 475.616, 476.601, 475.581, 471.990
10–19: 473.670, 283.437, 476.855, 475.848, 454.556, 9.731, 9.731, 9.267, 9.731, 9.731
20–29: 9.267, 9.731, 9.731, 9.267, 16.218, 281.155, 470.658, 473.438, 472.917, 475.176
30–39: 474.712, 475.813, 475.118, 477.840, 472.338, 475.813, 323.043, 432.929, 473.206, 475.987
```

#### Hysteria2

```text
0–9: 281.260, 470.024, 470.524, 468.293, 471.437, 470.069, 472.942, 473.581, 474.230, 470.103
10–19: 472.214, 468.646, 469.202, 470.720, 444.723, 9.550, 9.265, 9.392, 9.528, 9.609
20–29: 9.437, 9.541, 9.437, 9.595, 37.591, 474.218, 473.956, 472.017, 470.993, 470.526
30–39: 469.407, 470.721, 470.923, 469.331, 473.728, 468.028, 469.694, 467.938, 472.383, 471.803
```

### QoS-only raw paths and bounded disposition

Results under `./.tmp/reflection/results/`:

- `mixed-combined-down-response-claim-qos-control-0908/`
- `quic-combined-down-response-claim-qos-candidate-0908/`
- `mixed-combined-down-response-claim-qos-candidate-0908/`
- `raw-combined-down-response-claim-qos-candidate-0908/`
- `h2-combined-down-response-claim-qos-candidate-0908/`

The corresponding run logs are
`./.tmp/reflection/response-claim-0908-qos-{control,quic,mixed,raw,h2}.log`.
Tunnel cells retain five result files; raw TCP has the probe JSON/error and
service JSONL, with no artificial tunnel endpoint logs.

The forecast selected restoration rather than an arbitrary10Mbps-phase
target: all five regain high-capacity bulk service, without a persistent
post-cut collapse. The candidate mixed penalty during the cut remains
material and unexplained at exact-byte scope, alongside physical backlog.
No controller/queue tuning or new runtime correction follows from aggregate
rates. Full performance acceptance and the separate UP receive boundary
remain open.
