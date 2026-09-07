# Exact selection inputs and pre-QoS frontier stalls

Date: 2026-09-07 UTC. Scope: read-only attribution of the frozen pruned runtime
with the temporary selection-input diagnostic, not a performance acceptance or
a policy-change recommendation. Invocation, binary lineage, profile and samples:
[capture artifact](SELECTION_INPUTS_DIAGNOSTIC_20260907.json).

Raw directory: `./.tmp/reflection/results/mixed-combined-down-selection-inputs-diag-0907/`.
Line numbers refer to its complete logs. Server PID 215779; client PID 209418;
session 15389858161903269837; bulk wire stream 1. Cross-peer durations below use
`ts_unix_ms`, not endpoint-local monotonic origins. Endpoint-local path IDs and
receive-source indices are not equated without an attachment mapping.

The new capture distinguishes **three observed selection branches**. In
particular, its longest pre-QoS unchanged frontier was **not** caused by QUIC
being absent or unavailable.

## A. Ready QUIC loses a 0.699-ms comparison; raw queue guard defeats retention

At server line 20913, Unix ms **1788757127955**, the original 65,536-byte range
`[113231608, 113297144)` is published on TCP path 2. The exact selection input
group is lines 20905–20908: stream 1, `next_offset=113231608`, generation 2753.
Every target has Product admission active, nonstale original-data observation,
`can_enqueue_stream_data=true`, Active state, Available peer usage, bulk allowed,
no backup/control-only policy, and an available score. All three command-queue
readiness bits are true. Candidate evaluation lines 20909–20912 suppress none.

| Input | TCP 2 / incarnation 1 | TCP 0 / incarnation 2 | TCP 1 / incarnation 3 | UDP 0 / incarnation 4 |
| --- | ---: | ---: | ---: | ---: |
| ETA ms | **499.502607** | 893.918890 | 1958.462436 | **500.201997** |
| Rate scope | PerFlowGoodput | PerFlowGoodput | PathCapacity | PathCapacity |
| Effective delivery Mbps | 8.126681 | 2.048096 | 0.350751 | 514.184384 |
| Native delivery Mbps | 5.281000 | 4.066208 | unavailable | 514.184384 |
| Product progress Mbps | 8.126681 | 2.048096 | unavailable | unavailable |
| Fresh bulk evidence | true | true | false | true |
| Product assignment qualified / durable progress | true / true | true / true | true / true | true / true |
| Confidence | 1 | 1 | 0.3 | 1 |
| Original-data exposure bytes | 327,680 | 65,536 | 0 | 29,491,200 |
| Exact native queue bytes | 176,898 | 9,388 | 0 | 0, unobserved |
| Native drain observed | true | true | true | false |
| Snapshot / completion queue bytes | 176,898 | 9,388 | 0 | 262,144 |
| Native flight bytes | 146,248 | 128,872 | 14,480 | 6,478,667 |
| RTT / jitter ms | 121.414 / 19.407 | 112.134 / 15.520 | 133.391 / 18.202 | 53.597119 / 13.541381 |
| Loss fraction | 0.126011 | 0.116588 | 0.122776 | 0 |

All are non-application-limited. Each evaluated candidate has one active flow
and zero active latency flows, so this Throughput score has **zero active-flow
penalty**. TCP 2 and UDP 0 also have zero low-confidence penalty. The stream's
Data-ACK outstanding count is 29,884,416 bytes and its frontier classification is
`AuthoritativeGap`.

The exact oldest lower owner is **UDP 0 / incarnation 4**, present and live;
the lead reference is also UDP 0. Thus no missing/different-owner explanation is
needed. The existing score is reproduced by:

```text
work = max(completion queue + native flight, original-data exposure)

TCP 2: work = max(176898 + 146248, 327680) = 327680 bytes
ETA = 387.086419 serialization + 60.707 half RTT
    + 19.407 jitter + 32.302187 loss penalty = 499.502607 ms

UDP 0: work = max(262144 + 6478667, 29491200) = 29491200 bytes
ETA = 459.862056 serialization + 26.798560 half RTT
    + 13.541381 jitter = 500.201997 ms
```

Serialization includes the next 65,536-byte quantum. The nominal advantage is
**0.699390 ms, or 0.1398%**, smaller than the observed 19.407-ms maximum jitter.
However `src/scheduler/policy.rs:146` requires **both** timing and raw queue-byte
conditions to retain the lower owner:

```text
500.201997 <= 499.502607 + max(13.541381, 19.407)       true
262144 <= 176898 + 65536                               false
```

The raw queue guard fails by 19,710 bytes. Consequently response scheduling
selects the lowest ETA candidate despite the timing-retention test passing.
This is an exact source/input explanation, not a proposal to remove all queue
or Product admission accounting.

The five immediately preceding originals were all UDP 0; each was 65,536 bytes:

| Server line | Unix ms | Starting offset | Carrier |
| --- | ---: | ---: | --- |
| 20868 | 1788757127951 | 112,903,928 | UDP 0 |
| 20877 | 1788757127952 | 112,969,464 | UDP 0 |
| 20886 | 1788757127952 | 113,035,000 | UDP 0 |
| 20895 | 1788757127953 | 113,100,536 | UDP 0 |
| 20904 | 1788757127954 | 113,166,072 | UDP 0 |
| **20913** | **1788757127955** | **113,231,608** | **TCP 2** |
| 20922 | 1788757127955 | 113,297,144 | UDP 0 |

The next four originals also use UDP 0. This is a real one-frame departure and
return, not merely a changing lead label in the diagnostics.

Client line 11581 establishes frontier **113,231,608** at Unix ms
**1788757128586**. It remains unchanged until line 13166 at
**1788757129255**: **669 ms**, the longest confirmed pre-QoS unchanged frontier
in this capture. There are 1,584 hole observations at this frontier. Reorder
bytes grow from 6,184,384 to 23,527,424. The releasing TCP source-index-0 frame
matches the original full range and advances the frontier to 116,836,088,
releasing 3,604,480 bytes. Original publication to that release is 1,300 ms.

All server dispatches covering byte 113,231,608 are the original above and one
14,600-byte TCP-path-1 repair at line 30888, Unix ms 1788757129268. That repair is
published **13 ms after** the observed release and cannot explain it.

## B. Faster QUIC really is excluded by data-command readiness

For another original head, **139,052,792**, generation 3255, server input lines
24762–24765 expose the previously missing readiness distinction:

| Input | TCP 2, selected | TCP 0 | TCP 1 | UDP 0 |
| --- | ---: | ---: | ---: | ---: |
| ETA ms | 1135.598 | 1207.233 | 2102.593 | **854.708** |
| Effective delivery Mbps | 8.126681 | 2.048096 | 0.350751 | 514.184384 |
| Fresh bulk / qualified / durable | true / true / true | true / true / true | false / true / true | true / true / true |
| Enqueue / data-command ready | true / true | true / true | true / true | **false / false** |
| Completion queue bytes | 680,068 | 80,232 | 11,856 | 8,585,216 |
| Native flight bytes | 191,136 | 136,112 | 10,136 | 5,758,771 |
| Original-data exposure bytes | 983,040 | 196,608 | 0 | 51,778,976 |

All four remain Active, Product-admitted, nonstale, policy-eligible and scorable.
QUIC's priority and reinjection command readiness remain true. It is excluded
specifically by the **data enqueue filter** before evaluation. The lower owner
and lead are still live UDP 0 / incarnation 4. Evaluated TCP candidates have
`suppression=none`; TCP 2 is the best ready candidate. The faster unready QUIC
ETA is advisory and cannot be chosen or cause a deferred placement in this
source. The selected TCP is **fresh PerFlowGoodput**, not the unknown 351-Kbps
TCP branch.

Server line 24769 publishes original `[139052792,139118328)` on TCP 2 at Unix ms
1788757128419. Client line 14893 establishes this frontier at 1788757129813;
line 16265 releases the matching full TCP frame at 1788757130156: **343 ms** of
unchanged frontier and **1,737 ms** from original publication. Reorder bytes
grow from 17,188,896 to 32,255,712 over 1,371 hole observations.

The only other covering dispatches are 14,600-byte repairs starting 139052792:
TCP 1 at server line 35526 / Unix ms 1788757129874; UDP 0 at line 35572 /
1788757130074; TCP 0 at line 35606 / 1788757130169. The last is after release.
Neither earlier repair has the releasing full 65,536-byte frame boundaries.

## C. Startup prior and qualification are a distinct admission branch

Original head **8,308,472**, generation 214, provides a separate startup witness
(server inputs 1345–1348, evaluated candidates 1349–1352, dispatch 1353).
All four carriers are Active, nonstale, ready and scorable, but none yet has
qualified or durable Product progress. The live contiguous owner, TCP 2, has
6,750,208 original bytes exposed, a 350,750.751-bps prior, and a 155,566.887-ms
score. It is admitted as `ContiguousFrontier`.

The other TCPs have 509,533 and 524,288 original bytes. QUIC has 466,043, native
rate 1.857160 Mbps, and a 2,349.054-ms score; its `fresh_bulk_evidence=true` does
**not** mean Product assignment qualification. A further 65,536-byte quantum
would take QUIC to 531,579 bytes. All three alternatives are explicitly rejected
by `startup_flight_limit`, not queue readiness or policy. This is evidence of
the existing permission/discovery tradeoff, not justification for erasing its
unique-original qualification guard.

The TCP original is published at 1788757123994. Client line 6284 receives its
matching full range at 1788757127193 and releases **66,817,184 bytes**, advancing
the frontier from 8,308,472 to 75,125,656. The current frontier was established
273 ms earlier, not at original publication; the 3,199-ms publication delay
must not be mislabeled one unchanged-frontier residence. Two covering repairs
are only 14,600 bytes each: UDP 0 at server line 14941 / 1788757126961 and TCP 1
at line 14965 / 1788757127140.

## Interpretation boundaries

The first witness proves the existing raw-byte retention veto can switch away
from a ready live lower owner for a sub-jitter ETA difference. The second proves
the busy-fast/free-slow branch really occurs with fresh qualified TCP evidence.
The third separates that from unqualified startup admission. These must not be
collapsed into one explanation or one proposed threshold change.

Product exposure is **unacknowledged original data**, not a measurement that all
those bytes still await native transmission. Conversely, this capture does not
prove that the excess over native flight was already received: it lacks native
byte-range progress/unsent mapping, and Product/native ACKs can progress at
different boundaries. Zero exact native QUIC queue with `native_drain_observed=false`
is unavailable evidence, not proof of an empty native sender. No exact division
of the original-to-release delay into network service, loss recovery and local
software scheduling is established here.

The 66.8-MB ordered release explains a receiver-bin burst without claiming the
physical 500-Mbps link suddenly increased capacity. Logs are opt-in and sizable;
this run is for ownership attribution, not a matched throughput improvement.
No source policy edits, new experiments, or assertion of a universally safe
deferral/hysteresis correction are part of this analysis.
