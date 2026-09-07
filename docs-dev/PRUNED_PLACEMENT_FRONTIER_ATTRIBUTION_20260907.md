# Pre-QoS mixed-path frontier attribution

Date: 2026-09-07 UTC. Scope: read-only attribution of the unchanged pruned
composition in [the placement diagnostic](PRUNED_PLACEMENT_DIAGNOSTIC_20260907.json).
This is diagnostic evidence, not an ordinary performance acceptance run or a
new allocator recommendation.

Raw directory: `./.tmp/reflection/results/mixed-combined-down-pruned-placement-diag-0907/`.
The line numbers below refer to the complete `client.log` and `server.log` there.
Bulk uses wire stream 1; the separate interactive stream is 0. Endpoint-local
path indices must not be equated with the other endpoint's path IDs.

## Confirmed longest pre-QoS unchanged frontier

The longest **observed unchanged frontier**, from a release establishing its
value to the next release advancing it, is **1,059 ms** at byte **67,914,337**.
It is not the run's later 4.451-second application read gap during QoS.

| Client log line | Unix ms | Event and exact byte state |
| --- | ---: | --- |
| 6140 | 1788756341215 | UDP index 0 releases `[67,906,873, 67,914,337)`; frontier becomes 67,914,337; reordered bytes 7,403,876. |
| 6141 | 1788756341216 | First explicit hole at 67,914,337. |
| 7554 | 1788756342261 | Same hole, now 22,845,123 reordered bytes; highest observed end 96,155,484. |
| 7555 | 1788756342274 | TCP index 2 supplies `[67,906,873, 67,972,409)`; frontier advances to 67,972,409, releasing 58,072 new bytes. |

There are exactly 1,414 `receive_hole` events at that same frontier. They are
repeated observations of one missing prefix, not 1,414 independent stalls.
The receive buffer grew by 15,441,247 bytes while that frontier remained fixed.
The first explicit hole-to-release interval is 1,058 ms; the preceding release
establishes one additional millisecond of confirmed residence.

## Original publication and every covering repair

Selecting **all** `server_sender_dispatch` events for stream 1 satisfying
`offset <= 67914337 < offset + payload_bytes` gives these three events:

| Server log line | Unix ms | Lane / endpoint-local carrier | Published interval |
| --- | ---: | --- | --- |
| 8317 | 1788756339988 | Data / TCP path 0 | `[67,906,873, 67,972,409)` (65,536 bytes) |
| 11172 | 1788756341136 | Reinjection / TCP path 1 | `[67,914,337, 67,972,409)` (58,072 bytes) |
| 11453 | 1788756341440 | Reinjection / UDP path 0 | `[67,914,337, 67,972,409)` (58,072 bytes) |

The releasing TCP frame has exactly the original full-frame boundaries, not
either suffix-only repair's boundaries. Original publication to that release
is 2,286 ms. This range witness identifies the original-publication delay and
the carrier type that finally released the frontier; it does **not** establish
that client TCP index 2 equals server TCP path 0 without their attachment
mapping. The trace also does not split those 2,286 ms into socket queue service,
native loss recovery, receive decoding, and task scheduling.

The preceding UDP release supplied only the first 7,464 bytes of the original
range. It therefore did not make the remaining 58,072-byte prefix hole disappear.

## What the placement decision actually knew

Immediately before the original publication, server lines 8315–8316 record:

| Candidate | Product delivery Mbps | Native delivery / pacing Mbps | Native queue bytes | ETA ms | Confidence | Durable progress |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| TCP 0, incarnation 3, selected | 0.351 | 10.172 / 11.177 | 1,695,006 | 45,322.099 | 70% | true |
| TCP 2, incarnation 4 | 0.351 | 7.456 / 17.670 | 3,038,787 | 78,415.805 | 80% | true |

Both are not application-limited. The selected TCP has 65,536 original bytes
in flight and 59,356,903 external ordering-debt bytes. Both events name an older
TCP 1 / incarnation 1 lower owner with `lower_owner_live=false`; TCP 0 becomes
the lead reference. `suppression=none` describes the evaluated placement decision,
not proof that every physical carrier was available.

There is no UDP candidate in this evaluation batch. The selection pipeline in
`src/runtime/sender/response/scheduling.rs:126` filters publication readiness
**before** candidate evaluation; other preceding filters include staleness and
Product admission, followed by policy and scoring. Thus absence of a UDP event
does **not** by itself prove “QUIC was busy.” This capture lacks the rejected
input's exact permission/readiness owner at the original-publication instant.

Management nevertheless establishes that UDP path 0 was active around the event:

| Management elapsed s | Native rate Mbps | Native flight bytes | Delivery age ms |
| --- | ---: | ---: | ---: |
| 4.0005 | 189.466 | 2,281,152 | 9 |
| 5.0006 | 189.466 | 10,757 | 490 |
| 6.0007 | 194.725 | 2,783,905 | 5 |
| 7.0008 | 173.155 | 1,986,834 | 16 |

These samples demonstrate a functioning faster native carrier nearby in time;
they cannot substitute for exact structural readiness, qualified Product
delivery evidence, or publication permission. In particular the dashboard's
zero QUIC queue is not a measurement of all native QUIC buffering.

## Why the receiver's one-second rate swings

Three release events directly show buffered ordered delivery catching up:

| Client line | Unix ms | Releasing carrier | Frontier before → after | Newly released bytes |
| --- | ---: | --- | --- | ---: |
| 5447 | 1788756340476 | UDP 0 | 9,374,666 → 67,841,337 | 58,466,671 |
| 7559 | 1788756342312 | TCP 2 | 68,037,945 → 78,645,372 | 10,607,427 |
| 8765 | 1788756342565 | UDP 0 | 86,468,156 → 98,907,996 | 12,439,840 |

The nominal receiver bins around this interval include 476.545, 0.117, and
302.091 Mbps. They measure ordered application delivery, not instantaneous
link service. The large releases explain catch-up bursts without inventing
additional link capacity. They also show successive different missing
frontiers: the whole early slow interval must not be described as one fixed
TCP-owned hole, and UDP can be the carrier releasing a frontier too.

For clock alignment, the management origin is approximately Unix ms
1788756335165 (`generated_unix_ms - elapsed*1000`, with collection skew).
The 1,059-ms residence is approximately management seconds 6.050–7.109;
the large releases above are approximately seconds 5.311, 7.147, and 7.400.
Client/server diagnostic monotonic origins differ, so cross-peer durations use
Unix timestamps. The probe establishes its own monotonic origin after process
startup; it has no exact Unix origin in `probe.json`. Bin association is therefore
coarse, not an assertion that probe and management epoch boundaries coincide.

## Bounded verdict

This capture proves a real ordered-delivery bottleneck involving a TCP original
publication, with substantial already-arrived later data waiting behind its
prefix. It is consistent with the known placement problem and identifies an
exact legal byte-range witness. It does **not yet prove** the narrower
“busy-fast/free-slow” exclusion at this instant: the unavailable UDP selection
input was not recorded. Nor does it prove a TCP congestion-controller defect,
an additional software stall, or that any proposed deferral would improve this
run without harming unknown-path discovery. No policy, threshold, or production
code change follows from this attribution alone.
