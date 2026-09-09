# Ordinary mixed repair causes and receipt conservation — 2026-09-09

Status: **diagnostic attribution, not a runtime fix or performance acceptance**.
The ordinary-policy capture records318381268B of accepted Data copies,99.31%
over TCP; persistent TCP ACK-gap recovery supplies75.75% of all accepted copy
bytes. TCP ingress includes314106580B of duplicate payload. These are material
volumes, but do not prove unnecessary recovery: a useful copy can arrive first
and make its later Original the duplicate. Exact winning cause remains unresolved.

## Predeclared contract and provenance

[The Original-placement diagnostic](BULK_ORIGINAL_PLACEMENT_20260909.md)
removed most TCP bulk Original assignment but lost useful speed and retained
high QUIC residence. A conservative source bound proved at least313432617B of
TCP recovery payload in that intervention, but the same bound is zero in its
ordinary control. Neither echo-copy volume nor intervention ratios establish
the ordinary bulk cause. The next question was explicitly recorded in
CURRENT_CLOSURE_PLAN before this observer and run.

Question: which existing accepted repair cause produces substantial work under
ordinary mixed policy, and how much arriving TCP/QUIC Data is new or duplicate?
Competing explanations were useful recovery, premature/repeated copies, or
predominantly non-copy native/shared-queue service. The information forecast
was classification, not a predicted Mbps improvement. Small copy volume would
stop this attribution direction; substantial copies/duplicates justify examining
their exact assignment/native-service/receipt timing, not suppressing recovery.
An accepted copy is not necessarily a successful write or winning arrival.

The frozen binary is ordinaryb2aa215 with four feature-only observation files:

```text
src/runtime/relay/client.rs
src/runtime/sender/response/dispatch.rs
src/runtime/sender/response/prepared.rs
src/runtime/sender/response/service.rs
```

It records successful Original commits, accepted copy causes/underlays, separate
requalification, and new/duplicate/ordered-triggered receipt bytes. No allocation,
membership, native controller, timer, queue, payload retention or batching policy
changes. The first compile failed only because the diagnostic used nonexistent
`TrafficClass::Realtime` in two arms; changing it to the existing
`RealtimeDatagram` fixed that observer typo. The successful optimized build
finished cleanly in3m34s. This was not a platform/model defect or a failed lab.

Exact patch: `./.tmp/reflection/repair-cause-observer-0909.patch`.
Frozen executable: `./.tmp/reflection/bin/repair-cause-20260909/mptunnel`.
The overlay was reversed before traffic. Both ends use
`./.tmp/reflection/repair_cause_observer.sh`, which explicitly unsets the previous
echo-membership and bulk-Original intervention flags, sets periodic perf on and
per-sample perf off. Neither removed intervention exists in this new overlay.
This is ordinary policy with observation overhead, not an ordinary-build speed
comparison or permission to reuse an earlier feature binary as a matched control.

[Raw archive](REPAIR_CAUSE_20260909.raw.tar.gz) retains the full results and exact
observer/build/run inputs. The263187B archive passes gzip integrity and
byte-for-byte extraction comparisons. The result directory is
`./.tmp/reflection/results/mixed-combined-down-repair-cause-ordinary-0909/`.
The two build logs and run log are `repair-cause-build-0909.log`,
`repair-cause-build2-0909.log` and `repair-cause-0909-run.log` under the same
reflection directory. Runner exit0, elapsed41.006489s. The existing HTB quantum
warnings are retained; no shape changes were made to silence them.

## Complete useful service and timing

The40s healthy mixed DOWN workload uses500Mbps router classes in both directions,
30ms DOWN/70ms UP, no configured jitter/loss or blackhole, HTB burst65536 and
netem limit8192. All41 actual service rows confirm that profile. There is no
QoS transition at15/25s. An8GiB HTTP response supplies bulk work alongside64B
TCP echoes every500ms with the unchanged3s echo observation timeout.

| Outcome | Observed |
|---|---:|
| Received body bytes / duration s | 1938707460 / 40.001183 |
| Whole useful goodput Mbps | 387.730025 |
| First body s | .578157 |
| Maximum body-read gap s | .317860 |
| Maximum-gap interval s | 22.179301–22.497161 |
| Bytes before / after gap | 1115079432 / 1115091432 |
| HTTP code / complete / partial requests | 200 / 0 / 1 |
| Echo successful / attempted | 78 / 78 |
| Echo request / response bytes | 4992 / 4992 |
| Echo p50 / p95 / maximum ms | 296.971 / 503.781 / 947.406 |
| Maximum success-to-success gap s | 1.121511 |
| Body0–5 / 5–15 / 15–25 / 25–40s mean Mbps | 275.910 / 442.667 / 420.818 / 366.344 |

The HTTP transfer is intentionally duration-stopped, not a completed8GiB file.
`probe.err` is empty. All78 generated echoes succeed; slow attempts reduce how
many fit in the load window, so this is not an80-attempt result with two hidden
failures. The last attempt ends39.770788s. All original-precision attempt records
remain in JSON; the largest four whole-echo intervals are:

| Attempt | Start → end s | Latency ms |
|---|---|---:|
| 18 | 9.267167 → 10.214572 | 947.406 |
| 1 | .500434 → 1.265283 | 764.849 |
| 42 | 21.883611 → 22.589418 | 705.808 |
| 21 | 11.214768 → 11.877177 | 662.408 |

Client Broken-pipe appears at the duration-stop boundary07:37:32.245UTC;
server RemoteClosed follows at07:37:32.321 and H3_NO_ERROR closure at
07:37:33.264. These are retained teardown records, not failed probe echoes.
Timing still includes substantial loaded tails; no average-only acceptance.

## Accepted source work and accounting domain

ServerPID375409 records the following successful logical admissions/commits.
Copy acceptance does not assert native completion, useful receipt, or a unique
range: repeated copies of a range contribute repeatedly to these totals.

| Source component | Count | Payload B |
|---|---:|---:|
| TCP Throughput Original | 15348 | 632992353 |
| TCP Latency Original | 79 | 5200 |
| QUIC Throughput Original | 32277 | 1332911963 |
| TCP persistent ACK-gap accepted copy | 20179 | 241184388 |
| TCP active-tail accepted copy | 2555 | 74989600 |
| QUIC persistent ACK-gap accepted copy | 148 | 2076208 |
| QUIC active-tail accepted copy | 2 | 131072 |
| All accepted copies | 22884 | 318381268 |

Original total is1965909516B. Accepted copies are16.1951% of that payload,
or13.9379% of Original-plus-copy payload. TCP copy bytes total316173988B;
QUIC2207280B. TCP persistent-gap is75.7533% of all copy bytes and TCP supplies
99.3067% of copies. Persistent TCP events span the run, not just startup: the
counter is active from~2.003s through closure and its last~1s periodic group
contains~10.49MB versus~4.52MB in an early group.

No rows were emitted for other repair causes, QUIC Latency Originals or
requalification. Given enabled counters and covering source-close flushes, this
means no such accepted events were observed in this capture, **not a measured
zero-valued row or a claim about every possible path/runtime**. Per-source
Original/repair components are process-wide, not per-flow or physical-incarnation
ledgers. Actual underlay comes from the admitted target, not the preferred path.

All762 server perf rows pass interval/cumulative reconciliation. Original
counters reach1788939452316ms; the final global `stream_close` flush at
1788939452320 includes two more persistent copies/14600B and two TCP encoded
writes/14660B. Inactive components are omitted, explaining earlier last rows
for TCP tail, QUIC persistent gap and QUIC tail. The source close follows body
drop and path close, covering the accepted relay producers; arbitrary later
native/control work must not be assumed categorically absent.

Successful TCP encoded-plaintext writes total950324842B/38498calls; QUIC
length-prefixed encode and successful-write totals both1339292287B/9937calls.
Compared with accepted Original-plus-copy payload, the arithmetic remainder is
1153301B on TCP and4173044B on QUIC. It is **not** proof that the whole remainder
is overhead or that no admitted payload remains queued. TCP plaintext excludes
Noise/native-wire overhead and native retransmissions; these domains cannot be
substituted for router bytes or completed application bytes.

## Receiver conservation and the useful-copy proof boundary

The observer measures each successfully applied StreamData frame using checked
wide arithmetic: newly received bytes are the increase in frontier plus buffered
bytes; duplicate bytes are input minus new; ordered-triggered bytes are the
frontier increase. Buffered chunks are disjoint. A hole-filling frame can release
earlier bytes from another carrier, so ordered-triggered is not carried payload
or completed local-socket delivery. Requalification is handled elsewhere and
does not enter these receipt counters.

| Client receipt component | TCP | QUIC | Total |
|---|---:|---:|---:|
| Applied StreamData count | 37942 | 120817 | 158759 |
| New unique payload B | 631923875 | 1312243509 | 1944167384 |
| Duplicate payload B | 314106580 | 2163480 | 316270060 |
| All input payload B | 946030455 | 1314406989 | 2260437444 |
| Ordered-triggered bytes | 141135725 | 1797600935 | 1938736660 |
| Duplicate / input | 33.2026% | .1646% | 13.9915% |

ClientPID370023's final `multipath_stream_close` flush is1788939452244ms.
All40 interval groups/component sum to their final cumulative values; each
new/duplicate/ordered triplet's counts align at every observed flush. The three
records could structurally straddle a periodic flush, but do not here.
`mux.receive_data` independently records158759 applications/2260437444B,
exactly equal to summed TCP/QUIC input. No invalid-arithmetic component is
emitted. This is absence of a triggered diagnostic failure, not fabricated0B
samples. All1µs duration fields are bookkeeping floors, not service/CPU evidence.

New minus ordered-triggered leaves5430724B not released by the measured receipt
events; this is not a claim about live allocations after stream teardown.
Ordered-triggered exceeds body plus echo by24208B. HTTP bytes,
local write/cancellation and stop boundaries differ; do not manufacture exact
application equality or count an unlocked suffix as this carrier's new receipt.

The final server Original fence is72ms later than the client receipt fence,
covering all possible Original payload for this prefix. The conservative lower
bound on unique TCP bytes delivered by a copy is therefore:

```text
max(0, TCP new receipt − covering all-lane TCP Original payload)
= max(0, 631923875 − 632997553)
= 0 B
```

Zero is an uninformative lower bound, **not proof no copy wins**. A winning copy
followed by a losing Original and the reverse ordering can have the same
aggregate totals. Cause tags exist at admission but not on received wire Data;
the measured314MB duplicate TCP payload cannot be assigned wholesale to the
316MB accepted TCP copies. The next exact-range join must retain original/copy
publication identity, first receipt and repeated/late arrivals. Accepted-copy
time is not a physical write/departure timestamp; a native-service delay claim
requires that additional boundary if receipt/authorization alone cannot decide.

## Physical/service cost and limits

The41 service samples span40.006317s, with client management timestamps
1788939412185–1788939452185. Actual router rate/delay, jitter, queue limits and
blackhole state match the declared profile in every row; all HTB/netem drop
deltas are zero. Service snapshots stop before final receipt/source flushes,
so these windows are close but not byte-identical.

| Whole sampled cost | Observed |
|---|---:|
| DOWN / UP class byte deltas | 2406619447 / 101690175 |
| DOWN / UP class packet-counter deltas | 1790705 / 883550 |
| Peak DOWN / UP backlog B | 28022430 / 271638 |
| Client peak / final RSS KiB | 89536 / 89536 |
| Server peak / final RSS KiB | 375444 / 362040 |
| Client peak / final ps CPU % | 130 / 130 |
| Server peak / final ps CPU % | 202 / 202 |
| Client QUIC RTT sample p50 / p95 / max ms | 272.723 / 432.672 / 482.037 |
| Server QUIC RTT sample p50 / p95 / max ms | 291.438 / 410.944 / 482.965 |
| Server QUIC flight sample p50 / p95 / max B | 9908448 / 17474522 / 21275049 |

QUIC stays physicalinstance1 through all41 samples; role-local epochs are
client8627434252457344741/server10201368044847488012. This excludes a hidden
physical replacement in this sampled run, not queue/service variation.
Class counters include all protocol/native work and are offload-sensitive;
packet counters are not physical-wire packet counts. Sampled backlog is not
the exact queue position of a delayed echo or a continuous maximum. Process
`ps %CPU` is lifetime multicore utilization, not interval CPU work, and sampled
RSS is not a memory-leak proof. Diagnostic perf work itself has a cost.

## Outcome versus forecast and next decision

The small-copy hypothesis is falsified for this ordinary-policy capture:
accepted copy work is material and dominated by persistent TCP ACK-gap recovery.
Substantial duplicate arrival also exists, alongside~388Mbps bulk service and
echo tails near1s. This selects an existing recovery timing/ownership mechanism
for exact attribution, not a new issue inventory or a demonstrated defect yet.

The decisive ambiguity is whether the dominant copies reach a missing prefix
usefully or repeat work that already has equivalent timely service. No aggregate
ratio answers it. One bounded range-level join can compare Original assignment,
accepted cause, native handoff and first receipt; it must preserve late losing
Originals as well as copies. No recovery suppression, timer/controller tweak,
protocol preference or performance acceptance follows these counters alone.
The earlier stall/restart/ownership fixes remain intact. Public README and
PERFORMANCE updates, broader competitiveness claims and release remain deferred.

## All one-second body bins

Mbps, all40 raw bins including startup, without trimming or interpolation.
One-second application rates may exceed500Mbps when queued data arrives in a
burst; they do not establish sustained service above the configured class rate.
All original-precision echo outcomes/start/end times are in the raw probe JSON.

```text
ordinary = [2.597,113.01,214.266,745.212,304.465,582.839,226.328,579.797,453.318,415.835,414.631,363.537,541.467,416.453,432.467,458.271,440.662,353.355,477.764,447.464,420.672,433.098,421.303,384.758,370.833,401.455,374.228,320.786,309.769,419.854,347.565,340.326,381.787,347.046,434.587,375.189,384.358,369.241,363.192,325.779]
```
