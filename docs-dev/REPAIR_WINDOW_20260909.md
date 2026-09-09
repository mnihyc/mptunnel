# Ordinary mixed repair-window attribution — 2026-09-09

Status: **diagnostic evidence, not a runtime correction or performance acceptance**.
Every byte in the preselected16MiB window first arrives as Original data. All
347856B of accepted copy payload intersecting that window arrive later as
duplicates. All35 queued repairs use the retained fallback deadline, not an
earlier loss/ETA authorization. This identifies the actual recovery mechanism
and losing work in this window; it does not yet prove that the sender could
have known the Original had arrived before it queued the copy.

## Predeclared question and provenance

[The ordinary cause capture](REPAIR_CAUSE_20260909.md) found material accepted
TCP persistent-gap recovery and duplicate receipt, but aggregate counters could
not distinguish a winning copy followed by a late Original from the reverse.
CURRENT_CLOSURE_PLAN selected one fixed Product window before instrumentation
and traffic: `[536870912,553648128)`, or `[512MiB,528MiB)`. Its position is beyond
startup at the previously observed workload rate; its16MiB extent is diagnostic
coverage, not a Product threshold. Full intersecting frame extents are retained
from process start through teardown, with offline clipping and no sampling.

The exact question is whether established-service persistent-gap copies beat
Originals, arrive late, or queue after Original receipt, and which assignment,
loss/fallback deadline and target estimate authorized them. Competing causes
are useful hedging, delayed feedback, premature assignment clocks and native/
shared-queue service. The information forecast selects the next existing
owner to inspect; it promises no Mbps improvement. Absent copies or ambiguous
identity stop attribution rather than permit another favorable window search.
The earlier316MB duplicated payload has a roughly63Mbps/40s service-budget
equivalent, not a guaranteed removable goodput or latency cost.

The frozen build is ordinaryb2aa215 plus six feature-only observation files:

```text
src/lab_diagnostics.rs
src/runtime/relay/client.rs
src/runtime/relay/server.rs
src/runtime/sender/response/dispatch.rs
src/runtime/sender/response/prepared.rs
src/runtime/sender/response/service.rs
```

It adds no policy, recovery suppression, timer/controller change, payload
retention, queue or resource limit. Successful Original claims, actual queue
IDs, accepted carrier copies, completed dispatch and validated receiver ranges
are recorded. Signed decision offsets refer to the existing immutable
evaluation time; head-range context is separate from the aggregate scored range.
The periodic cause/receipt observer remains enabled, per-sample perf disabled.
Zero-duration perf bookkeeping is not measured service time.

Build `repair-window-build-0909.log` finished cleanly in3m34s. The six-file
overlay was frozen and fully reversed before traffic; the ordinary executable
was also restored and byte-compared by the parent. Both products run the frozen
observer through `repair_window_observer.sh`, explicitly unsetting the prior
echo-membership and bulk-Original intervention flags. Ordinary membership and
placement policy remain unchanged, but observation overhead remains present.

Inputs under `./.tmp/reflection/` are the exact
`repair-window-observer-0909.patch`, wrapper, build log,
`repair-window-0909-run.log` and frozen
`bin/repair-window-20260909/mptunnel`. Results are
`results/mixed-combined-down-repair-window-ordinary-0909/`.
[Raw archive](REPAIR_WINDOW_20260909.raw.tar.gz) is the durable record of the
capture and inputs. Runner exit0, elapsed41.009737s. Existing HTB quantum
warnings remain; no profile changes were made to silence them.

The307265B archive passes `gzip -t` and decompressed `tar -d` comparisons
against every archived input/result. It excludes the frozen executable;
the exact observer patch and build/run provenance are retained.

## Complete useful service and timing

The existing40s mixed DOWN workload has500Mbps in each direction,30ms DOWN/
70ms UP, zero configured jitter/loss and no blackhole. All41 effective service
rows confirm the rates, delays, HTB burst65536 and netem limit8192; all class/
qdisc drop deltas are zero. Epoch updates do not introduce a15/25s QoS transition
in this healthy run. An8GiB HTTP response supplies bulk backlog alongside64B
echoes every500ms with the unchanged3s observation timeout.

| Outcome | Observed |
|---|---:|
| Body bytes / duration s | 2041810736 / 40.002032 |
| Whole useful goodput Mbps | 408.341408 |
| First body s | .578086 |
| Maximum body-read gap s | .338428 |
| Maximum-gap interval s | 22.138701–22.477129 |
| Bytes before / after gap | 1108411456 / 1108426056 |
| HTTP code / complete / partial requests | 200 / 0 / 1 |
| Echo successful / attempted / failed | 80 / 80 / 0 |
| Echo request / response bytes | 5120 / 5120 |
| Echo p50 / p95 / maximum ms | 336.738 / 445.177 / 773.080 |
| Maximum success-to-success gap s | .778925 |
| Body0–5 / 5–15 / 15–25 / 25–40s mean Mbps | 277.628 / 430.608 / 435.335 / 419.093 |

The HTTP request is duration-stopped, not a completed8GiB transfer.
`probe.err` is empty. All80 generated echoes succeed, including the last
attempt39.963886→40.197668s that finishes after the bulk window. Nothing is
discarded from the attempt record. The largest whole-echo intervals are:

| Attempt | Start → end s | Latency ms |
|---|---|---:|
| 66 | 33.187233 → 33.960314 | 773.080 |
| 56 | 28.018079 → 28.685515 | 667.436 |
| 65 | 32.687130 → 33.181389 | 494.259 |
| 61 | 30.686697 → 31.133114 | 446.416 |

Client connection-reset appears at bulk duration-stop08:00:58.980UTC;
server RemoteClosed follows at08:00:59.053 and H3_NO_ERROR closure at
08:01:00.008. These retained shutdown warnings are not failed probe echoes.
The observer's408Mbps does not establish an improvement over an earlier
capture: this transaction has no ordinary-build performance control.

## Exact first-arrival result

Independent reconstruction uses the union of logged receiver and sender
interval boundaries, clips atomic spans offline, and finds their first actual
receiver event in local sequence order. A unique Original/copy candidate on
the mapped ingress identifies the arrival; no matching span is unresolved,
conflicting or ambiguous in this window.397 Original events and977 receive
events cover all16777216 unique window bytes. All first arrivals are Originals:

| First-arrival owner, wire path ID | Unique window bytes |
|---|---:|
| TCP0 | 2220584 |
| TCP1 | 5811448 |
| TCP2 | 1217920 |
| QUIC0 | 7527264 |
| Total Original / copy | 16777216 / 0 |

There are no late Originals behind a winning copy in this selected window.
The35 actual queued persistent-gap frames contain450000 full-extent bytes /
435456 clipped bytes. Only28 reach accepted dispatch:362400 full bytes /
347856 clipped bytes. All accepted copies arrive as late duplicates:87656
clipped bytes on TCP1 and260200 on QUIC0. Seven queue IDs—10044,10045,10047,
10049,10051,10053,10055—have no accepted dispatch in the complete capture;
their87600B must not be called sent or lost. No accepted/dispatch event is orphaned.

| Receiver conservation domain | Input B | New B | Duplicate B |
|---|---:|---:|---:|
| Whole977 intersecting frames | 17167368 | 16804968 | 362400 |
| Offline-clipped16MiB window | 17125072 | 16777216 | 347856 |
| Boundary overhang | 42296 | 27752 | 14544 |

Raw ordered-triggered bytes are17061576. They can release earlier buffered
data outside this window and are not this ingress's first-arrival ownership
or completed local application delivery. The byte-weighted late-copy arrival
lag has min/p50/p95/max187/218/249/250ms. Byte weights describe coverage, not
independent experimental trials; a single frame covers many bytes.

All41 management samples retain one session and stable mapping. Client physical
TCP instances2/4/3 map to wire0/1/2; QUIC physical1 maps to wire0. Client
attachment numbers for TCP1/0/2/QUIC0 are0/1/2/3, while server incarnations are
1/2/3/4. These are separate namespaces, not interchangeable numbers. Each
endpoint's physical→wire mapping, actual underlay and lifetime establishes
the join; matching numeric allocators alone would be invalid.

## Which clock authorized the late copies?

Every queued event has effective candidate equal to retained fallback: none
uses an earlier loss/ETA authorization. Actual accepted-copy owners are TCP;
eight accepted targets are TCP physical4/wire1/incarnation1, twenty are QUIC
physical1/wire0/incarnation4. Six accepted targets have ETA at least their
owner ETA, consistent with fallback authority rather than an ETA-win gate.
For the28 accepted events, aggregate assignment age is752.575–858.822ms and
the retained fallback is already483.406–572.637ms past. Owner ETA ranges
426.620–770.585ms; target ETA344.174–483.968ms. These are the actual decision
estimates and ages, not measured remaining delivery time. The observation
does not support describing these repairs as simply an early loss timer.

All35 evaluation-age observations are3–129us. Subtracting that age from the
millisecond event timestamp approximates the existing evaluation epoch, with
timestamp granularity and logging overhead still explicit. All queued bytes
already had Original receipt. Among accepted copy bytes, byte-weighted
Original-before-queue min/p50/p95/max is56/67/70/72ms. Across all35 queued events,
the conservative corrected observation interval is51.993–71.996ms.
This difference survives timestamp uncertainty but is comparable
to the configured70ms return propagation: **it alone does not prove delayed ACK
publication or that the server already had receipt authority**.

Head assignment differs in six records and does not cover three queued frames.
The recorded aggregate scored-prefix owner and timing, not unrelated head age,
are the correct decision context. Queue delay is at most2ms; queue-log→carrier
acceptance wall timestamps differ0–3ms. These are local admission/dispatch
boundaries, not asynchronous native-write completion or physical departure.

One exact example, enqueue10025, stream1:

| Boundary | Unix ms | Exact fact |
|---|---:|---|
| Original claim | 1788940830226 | `[537204832,537270368)`,65536B; TCP wire2/physical3/incarnation3 |
| Original receipt | 1788940830922 | 65536B new; frontier537204832→537532512 |
| Copy queue | 1788940830986 | `[537204832,537219432)`,14600B; persistent-gap cause |
| Copy acceptance / dispatch | 1788940830987 | TCP wire1/physical4/incarnation1; queue-delay0ms |
| Copy receipt | 1788940831156 | 14600B duplicate; no frontier movement |

At that decision the aggregate assignment age is760787us, raw loss offset
−536480us, and raw fallback/effective candidate/retained fallback all−491618us
relative to the immutable observation. Owner/target ETA are770179/366474us;
the observation is4us older than its log. The Original precedes queue64ms and
acceptance65ms; the copy arrives234ms after it. Cross-process ordering uses
same-host Unix millisecond timestamps, not independent monotonic origins.
Equal wall timestamps would not establish event order.

## Whole-capture context and cost

Periodic source counters reconcile all791 interval rows with their cumulative
totals. Original payload is TCP969904212B and QUIC1092150556B. Accepted copies
are TCP192340110B (153496518 persistent-gap,38843592 active-tail) and QUIC
3970856B (2773992 persistent-gap,1196864 active-tail). No other cause or
requalification component is emitted. This is coverage of actual accepted
events, not a fabricated zero-valued measurement. The selected window is not
an unbiased sample permitting extrapolation of its zero copy wins to all
196310966 accepted-copy bytes or to loss/outage conditions.

Whole receiver input2238517958B reconciles as2043287392B new and195230566B
duplicate. TCP new/duplicate are966054900/191354446B; QUIC1077232492/3876120B.
All interval sums match cumulative totals; no invalid-arithmetic component is
emitted. Ordered-triggered2041869600B leaves1417792B of new receipt not released
by the measured events. This is not a live allocation after teardown. TCP's
last receipt flush is1788940858980; QUIC/mux ends1788940859319, reflecting
inactive component tails rather than one common snapshot. Ordered release
also exceeds probe body plus echo response by53744B; it is not completed
local-delivery accounting.

Successful TCP encoded plaintext totals1163279889B/34548 transactions. QUIC
encoding is1099553206B/8828, but successful write-wait is1099553146B/8827:
the final60B encode has no successful write-wait row. Do not silently equate
encoding, queue acceptance, native retransmission and received wire bytes.
Main producer components flush39.901s; post-close control reaches40.095s.

The41 service samples span40.009517s; client management timestamps are
1788940818926–1788940858925. Native counter snapshots and source/receiver
closure windows are close, not identical.

| Whole sampled cost | Observed |
|---|---:|
| DOWN / UP class byte deltas | 2385119385 / 83459701 |
| DOWN / UP class packet-counter deltas | 1726706 / 742886 |
| Peak DOWN / UP backlog B | 23018420 / 275956 |
| Client peak / final RSS KiB | 106408 / 106408 |
| Server peak / final RSS KiB | 299752 / 299752 |
| Client peak / final ps CPU % | 122 / 122 |
| Server peak / final ps CPU % | 196 / 196 |
| Client QUIC RTT sample p50 / p95 / max ms | 312.120 / 420.243 / 442.080 |
| Server QUIC RTT sample p50 / p95 / max ms | 321.761 / 412.335 / 441.608 |
| Server QUIC flight sample p50 / p95 / max B | 9842186 / 15668461 / 18634968 |

Both endpoint QUIC physical instances remain1 through41 samples, with stable
role-local epochs. Class counters include all protocol/native traffic and are
offload-sensitive; packet counters are not physical-wire packet counts. Sampled
queue peaks are not continuous maxima or the exact blocking byte's position.
Process ps CPU is lifetime multicore utilization, not interval work; sampled
RSS does not establish a leak. Observation itself adds cost.

## Outcome versus forecast

The capture supplies a decisive local ordering result without a policy change:
all accepted copies in this preselected window lose to Originals, and the
existing retained fallback authorizes every attempt. It therefore narrows the
next review to that exact fallback/assignment/feedback ownership rather than
generic QUIC aggressiveness, active-tail-only recovery or another threshold.
It does not establish that an earlier deadline was wrong, that server feedback
was late, or that deleting these copies preserves blackhole recovery. Source
admission timestamps do not localize native write service.

Useful-copy evidence in other intervals and the existing7–14s selected-path
blackhole failures remain constraints. No window extrapolation, blanket repair
suppression, parameter tuning, public performance claim or release follows.
The next model decision must explain this observed timing and retain required
failure recovery; broad competitiveness remains unaccepted.

## All one-second body bins

Mbps, all40 raw bins including startup, without trimming or interpolation.
Application bursts above500Mbps do not establish sustained physical service
above the class rate. All original-precision echo outcomes and start/end times
remain in the raw probe JSON.

```text
ordinary = [2.597,106.058,109.672,723.281,446.532,470.079,389.861,435.395,280.019,602.458,369.426,382.464,481.327,429.695,465.353,438.75,388.515,299.364,618.973,434.417,447.061,479.996,378.671,452.934,414.67,462.804,452.458,464.05,311.373,525.166,408.576,403.903,456.004,125.098,463.434,572.254,221.515,593.096,410.672,415.985]
```
