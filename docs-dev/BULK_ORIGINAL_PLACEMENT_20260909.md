# Bulk Original placement discriminator — 2026-09-09

Status: **diagnostic completed, no promotion or runtime fix**. Suppressing TCP
bulk Original choices when usable QUIC exists reduces their observed byte share
from42.16% to1.20%, but goodput falls393.901→275.892Mbps. Winning QUIC echo
postwrite residence remains179→192ms median. This does not recover the previously
observed QUIC-only31ms median, even with substantially less useful bulk service.
Realized echo membership also differs, preventing a clean isolated latency-win
claim. Independent source conservation now proves at least313432617B of TCP
recovery payload in the intervention. That bound does not establish the ordinary
control's copy volume, identify a specific recovery cause, or prove copies harmful.

## Predeclared question and exact intervention

[The preceding same-build echo comparison](ECHO_MEMBERSHIP_COUNTERFACTUAL_20260909.md)
found that adding QUIC membership alone did not remove mixed-mode latency;
its QUIC-only ablation delivered more useful work with much lower residence.
That established a material mixed context, not whether TCP data, feedback,
membership or native service caused it. CURRENT_CLOSURE_PLAN predeclared this
next discriminator before implementation and traffic.

Question: does withholding new TCP Throughput Originals, while retaining mixed
membership and normal feedback/repair machinery, recover the missing service?
The potential information envelope was roughly200ms median response residence,
not a guaranteed latency saving or Mbps gain. Lower useful load, replacement,
or ineffective intervention prevents a favorable inference. A protocol-specific,
non-work-conserving diagnostic cannot itself become a shipped policy.

The feature/env-only filter preserves the complete target set through live
counts, lead/FirstPath/AdditionalPath geometry and resource admission. Only the
final admitted Original choices exclude TCP, and only for Throughput while an
exact Regular QUIC target is Active, nonstale, admission-active and ordinarily
schedulable, with native authority present. The rule does not require writer
Ready or latch historical QUIC use. It runs at preselection and final fenced
selection; all original waits, pre-FINAL bounds, old TCP debt, repair, feedback
and Latency choices remain unchanged. This intentionally can leave Ready TCP
unused while waiting for QUIC. Ordinary config backup/bulk flags would not
isolate this boundary because they also change startup/ranking/repair.

Both cells use ordinaryb2aa215 plus the same frozen feature binary:
`./.tmp/reflection/bin/bulk-original-placement-20260909/mptunnel`.
The optimized build completed cleanly in3m36s. The full13-file overlay was frozen
as `./.tmp/reflection/bulk-original-placement-observer-0909.patch` and reversed
before traffic. Ordinary source/RFC and executable are restored; no candidate
runtime commit or public documentation claim follows this capture.

Both client wrappers enable the same echo-membership intervention, and both
server wrappers enable periodic perf recording. Only the intervention server
sets `MPTUNNEL_LAB_BULK_QUIC_ORIGINALS=1`. Per-frame bulk logging stays disabled.
Periodic server perf was off in the preceding echo captures, so those earlier
Mbps values are context, **not a matched control for this pair**.

[Raw archive](BULK_ORIGINAL_PLACEMENT_20260909.raw.tar.gz) retains the exact
observer, wrappers, build/run logs and complete result records. Result directories
under `./.tmp/reflection/results/` are:

```text
mixed-combined-down-bulk-original-control-0909/
mixed-combined-down-bulk-original-quic-0909/
```

## Full useful service and timing

Control runs before intervention. Each uses the same healthy500/500Mbps shared
router classes,30ms DOWN/70ms UP, zero configured jitter/loss, no blackhole,
HTB burst65536 and netem limit8192. There is no15/25s QoS transition here.
An8GiB HTTP object supplies40s bulk load alongside64B TCP echoes every500ms,
with the unchanged3s echo observation timeout.

Both runners exit0. Each returns HTTP200, one intentionally duration-stopped
partial request and zero completed8GiB requests; both `probe.err` files are empty.
All80 echoes succeed per cell, without censoring failures from the distributions.

| Outcome | Control | Original-placement intervention |
|---|---:|---:|
| Ordered received body bytes | 1969563115 | 1379469532 |
| Body duration s | 40.001169 | 40.000235 |
| Whole goodput Mbps | 393.901115 | 275.892286 |
| First body s | .578749 | .583508 |
| Maximum body-read gap s | .383157 | .201553 |
| Maximum-gap interval s | 22.030117–22.413274 | .583508–.785061 |
| Gap bytes before / after | 1069335615 / 1069350215 | 58192 / 70192 |
| Echo successful / attempted | 80 / 80 | 80 / 80 |
| Echo p50 / p95 / max ms | 269.131 / 375.669 / 402.840 | 266.119 / 325.783 / 379.504 |
| Maximum success-to-success gap s | .720039 | .612388 |

Smaller body gaps and some better echo tails coexist with~30% less useful
goodput, not a free performance gain. Different realized echo membership and
the lower offered/received useful service confound an isolated latency claim.
The intervention does not show substantial median-residence relief anyway.

Client Broken-pipe warnings occur at the duration-stop boundaries07:00:19.007
and07:02:36.831UTC; server RemoteClosed follows73/76ms later, and H3_NO_ERROR
connection closure~1.01/1.04s later. These recorded teardown events are not
failed echo attempts during the workload; all warnings remain in the raw logs.

## Original accounting and actual intervention strength

`response.original_claim.<underlay>.<lane>` records only a successful Original
commit's payload and count. It does not measure repair, native write or delivered
bytes. Every observed component's summed interval count/bytes exactly equals its
last cumulative record. All1µs averages/maxima and matching total-us/count fields
are the zero-duration bookkeeping floor, **not CPU or service measurements**.

| Component | Control count / payload B | Intervention count / payload B |
|---|---:|---:|
| TCP Throughput Original | 21331 / 838402551 | 429 / 16858412 |
| QUIC Throughput Original | 26347 / 1150030720 | 37043 / 1386275912 |
| TCP Latency Original | 31 / 2128 | 32 / 2192 |
| QUIC Latency Original | 50 / 3200 | 49 / 3136 |
| TCP share of Throughput Original bytes | 42.163977% | 1.201482% |
| Total Throughput Original bytes | 1988433271 | 1403134324 |

There are exactly208 TCP Latency bytes beyond the selected echo's1920/1984B
in each cell; process-wide lane counters must not be labelled echo-only.
Claimed Throughput bytes exceed received body by18870156/23664792B at stop.
These differences are not automatically loss: claim, framing, native acceptance,
ordered receipt and stop boundaries are different accounting domains.

Final stream-close flushes are present. Original cumulative records reach
1788937219080ms in control and1788937356903ms in intervention; the server's
last overall perf flush ends1788937219080/1788937356907ms. Components with no
new interval work are omitted, so the last TCP Latency record can be earlier.
Counters cover observed records through those fences, not proof of any unlogged
process-lifetime tail. `interval_ms=1000` is the nominal configured duration;
actual Unix stamps, not that field, determine elapsed-window comparisons.

The intervention's429 remaining TCP Throughput commits occur in eight recorded
groups, including later workload phases, not only startup:

```text
flush_unix_ms    count  payload_bytes
1788937318023       14       910160
1788937319024       70      2721340
1788937324027       68      3096768
1788937331040       45      1652192
1788937336043       94      3534560
1788937341047      113      4148736
1788937352058        5       229536
1788937356903       20       565120
```

The conditional filter is not a permanent TCP ban. These aggregate groups do
not identify which usable-QUIC predicate was absent at each bulk decision.
The intervention is nevertheless effective at sharply reducing TCP Originals.

Successful TCP encoded-plaintext frame bytes remain1133386244→856280475B,
while QUIC length-prefixed frame bytes are1159520230→1390631314B. The TCP
component records `encode_buffer.len()` after successful Noise plaintext write;
it excludes Noise/native-wire overhead and is not a TCP retransmission counter.
Those counters alone do not classify the remaining traffic. The following
independent source audit supplies a deliberately conservative compatible bound.

### Verified copy lower bound and its limit

Server ordinary TCP writes contain one encoded frame per transaction, except
the tiny initial SessionReady/PathStatus pair. New response Originals all pass
the observed prepared-claim producer; the alternate old Original emitter is
test-only. The only other large runtime frame in this two-stream L4 workload is
StreamRequalifyData, with at most14600 payload bytes and14638 encoded bytes.
ACK (at most4127B), configured64-path peer status (at most9173B), validation and
other exercised server replies are smaller. There is no IP/datagram payload,
server-originated host-bearing open, or production capacity payload train.

Charging EVERY successful TCP write14638B, plus ALL observed TCP Original
payload, therefore leaves a conservative lower bound on StreamData recovery
payload. This overcharges Original and repair frames rather than mistaking
their headers for copies. Two independent source audits agree:

| Byte conservation | Control | Intervention |
|---|---:|---:|
| Successful encoded TCP plaintext W | 1133386244 | 856280475 |
| All TCP Original payload O | 838404679 | 16860604 |
| Successful TCP write calls N | 42741 | 35933 |
| Pessimistic other-frame/header allowance N×14638 | 625642758 | 525987254 |
| Recovery payload lower bound max(0,W−O−N×14638) | 0 | 313432617 |

The intervention has material actual copy traffic; it is not merely a native
retransmission counter or unused buffer capacity. It is incorrect to label the
entire W−O remainder as recovery. The ordinary control and every checked
cumulative prefix have a zero lower bound under this allowance, so the result
must not be silently transferred to ordinary mixed traffic. Its exact echo
trace proves only one losing64B TCP copy and one losing64B QUIC copy.

The active retained-frontier, scoped ACK-gap, stale/failed-owner and completion
branches all legitimately can emit copies. In particular, assignment-time
recovery may mature while an Original remains inside native/network service;
this is a reachable model sequence, not attribution of this capture. Count
actual successful acceptance by cause and actual new/duplicate receipt before
considering a correction. Accepted copies need not be written or arrive first;
first receipt itself does not prove a counterfactual latency benefit.

The504523B raw archive contains17 regular files and passes gzip integrity and
byte-for-byte extraction comparisons. Ordinary source/RFC and the restored
ordinary executable remain unchanged.

## Exact echo membership, winners and residence

C/S refer to physical client/server log lines. Joins use exact byte ranges,
role-local identities and Unix milliseconds; equal stamps do not imply zero
work, and a successful native write is not physical departure or local delivery.

Both clients have the injection flag enabled, but realized membership differs:

- Control stream0/session18086727023601709164 naturally adds QUIC at
 1788937180183ms (C11), accepted1788937180287 (C12). It has one TCP plus QUIC;
  no `echo_membership_probe` fires because two TCP members never qualify.
- Intervention stream0/session17711400076103122023 first adds its second TCP
  at1788937317607 (C11), then injects QUIC at the same time (C13). Actual QUIC
  attachment completes1788937317716 (C14),109ms later. It has two TCP plus QUIC.

| Carrier identity | Server wire / physical / attachment | Client runtime index / physical / attachment |
|---|---|---|
| Control TCP | 1 / 2 / 1 | 0 / 2 / 0 |
| Control QUIC | 0 / 1 / 2 | 0 / 1 / 1 |
| Intervention TCP | 2 / 4 / 1 | 0 / 4 / 0 |
| Intervention TCP | 1 / 2 / 2 | 1 / 2 / 1 |
| Intervention QUIC | 0 / 1 / 3 | 0 / 1 / 2 |

All41 snapshots per cell retain QUIC physicalinstance1 and one native epoch
per role: control client7059984765233201552/server8173018612892486265;
intervention client9351673562440313667/server11569415450350725638.
Different client/server namespaces and epoch numbers are not a mismatch.

Every cell has80 source reads/enqueues/Original claims and5120 unique response
bytes covering[0,5120) without gaps/overlap. All80 Originals win. Control has
82 positive writes/decodes/mux applies (5248B), intervention81 (5184B);
the extra two/one arrivals are nonadvancing copies. Each winning range has
exactly one joined enqueue, claim, write, decode, mux and local delivery;
all mux frontier transitions and all local5120B reconcile, with no unmatched
write/decoder records. Control winners are30TCP/50QUIC, intervention31TCP/49QUIC.

| Winning-stage time ms | Control TCP | Control QUIC | Intervention TCP | Intervention QUIC |
|---|---:|---:|---:|---:|
| Native write→decode p50 / p95 / max | 196 / 303 / 325 | 179 / 282 / 313 | 161 / 254 / 280 | 192 / 241 / 308 |
| Source enqueue→claim max | 30 | 3 | 3 | 5 |
| Original claim→native write max | 1 | 1 | 1 | 1 |
| Decode→mux max | 1 | 1 | 1 | 1 |
| Mux→local max | 1 | 1 | 1 | 1 |

Across all winners, postwrite residence p50/p95/max is189/282/325→190/254/308ms.
Control has two committed QUIC views marked Failed/no-native-authority and
unselected (S130,696), while physical incarnation remains stable. Intervention
has no non-Active committed candidates. A sampled candidate state alone does
not prove a separate transport failure or a wrongly ignored better opportunity.

Critical whole-echo boundaries:

- Control worst attempt66,[4224,4288),33.018995→33.421835s:402.840ms.
  Request1788937212024 (C274); target/enqueue/claim/QUICwrite1788937212211
  (S1427/1429/1433/1438); decode/mux/local1788937212427 (C275–277).
  Approximately187ms precedes response availability and216ms follows native write.
- Control longest postwrite,[1664,1728),attempt26:397.341ms whole.
  Request1788937192014 (C114), target2085 (S557), enqueue/claim/TCPwrite2086
  (S559/563/568), decode/mux/local2411 (C115–117):325ms after write.
- Intervention worst attempt25,[1600,1664),12.507816→12.887320s:379.504ms.
  Request1788937329338 (C109), target/enqueue9408 (S580/582), claim/QUICwrite
  9409 (S587/593), decode/mux/local9717 (C110–112):70ms request-side,
  1ms claim and308ms native-write→decode.

These captures still place the material residual after native acceptance and
before authenticated decode; they do not distinguish emitted shared-queue delay,
native pacing, peer read/framing or CPU scheduling individually. Lower useful
load and unequal membership prevent claiming isolated latency improvement.

## Actual physical and resource context

All82 service rows independently preserve the stated500/500Mbps,30/70ms,
zero-jitter/no-blackhole profile. Every observed HTB/netem drop delta is zero.
The41-row spans are40.017967/40.022345s. Client management Unix endpoints are
1788937178941–1788937218938 and1788937316778–1788937356777; these sample
origins differ from probe offsets and perf component origins.

| Observed whole-run cost | Control | Intervention |
|---|---:|---:|
| DOWN / UP class byte deltas | 2412971861 / 93185369 | 2358092695 / 79384527 |
| DOWN / UP class packet-counter deltas | 1759076 / 827111 | 1753506 / 715626 |
| Peak DOWN / UP backlog B | 18806602 / 557712 | 18900672 / 222380 |
| Client peak RSS KiB | 81300 | 40960 |
| Server peak RSS KiB | 329160 | 370272 |
| Client peak / last ps CPU % | 131 / 131 | 116 / 115 |
| Server peak / last ps CPU % | 202 / 202 | 201 / 201 |
| Client QUIC RTT sample p50 / p95 ms | 253.809 / 331.710 | 258.260 / 306.525 |
| Server QUIC RTT sample p50 / p95 ms | 248.668 / 341.539 | 253.836 / 314.513 |
| Server QUIC flight sample p50 / max B | 7488210 / 13562559 | 9709524 / 11793144 |

The intervention carries almost as many DOWN class bytes despite much less
useful body, with a similar peak DOWN backlog and nearly unchanged server CPU.
That is a material composition/cost observation, not attribution of the extra
bytes or exact echo queue position. Class counters include all native/protocol
traffic and are offload-sensitive; packet counts are not physical wire counts.
Sampled queues do not establish continuous maxima or individual-byte residence.
`ps %CPU` is lifetime multicore utilization, not interval CPU work; RSS samples
do not prove either a leak or universally improved memory usage.

## Disposition

The intended placement intervention is observed, but its practical forecast is
not met: throughput loses~118Mbps and QUIC median residence does not improve.
Better body-gap/tail numbers at lower useful service are not acceptance. The
conditional, protocol-specific non-work-conserving filter is not a production
model, and this result gives no reason to ship it or compensate with thresholds.
It also does not prove all TCP Original allocation is harmless: remaining TCP
frame traffic and unequal realized membership prevent that stronger conclusion.
Conservation now confirms material intervention copy traffic, not a defective
recovery cause or its ordinary magnitude. The next bounded discriminator is
ordinary mixed accepted-copy cause accounting and new/duplicate receipt, with
no placement intervention, suppression, timer or controller change. This is
information gathering for the existing mixed-service issue, not a new audit
inventory. README/PERFORMANCE and release status remain unchanged.

## All one-second body bins

Mbps, all40 bins per cell including startup, without trimming or interpolation.
Every original-precision echo attempt/start/end/outcome is retained in raw JSON.
Application bins can exceed500Mbps while draining previously queued bytes;
they are not evidence of sustained delivery above the configured link capacity.

```text
control = [3.145,109.339,353.467,624.924,328.715,531.41,257.611,578.837,393.934,418.09,373.89,494.928,400.721,367.305,387.34,411.869,365.83,415.486,435.925,413.921,435.903,444.344,274.948,487.526,397.8,402.745,365.91,411.784,415.103,384.668,438.064,382.211,388.447,228.568,568.629,376.914,415.254,453.677,385.877,431.157]
intervention = [1.433,186.146,371.912,288.595,286.335,301.798,303.513,323.446,214.666,307.548,302.866,296.592,270.008,280.284,262.649,301.01,274.162,279.736,270.811,313.811,294.246,278.06,281.832,279.733,331.088,245.913,298.065,282.416,281.544,274.505,237.748,295.813,280.229,278.844,279.875,302.436,246.265,283.256,274.195,272.277]
```
