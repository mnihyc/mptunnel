# Exact response handoff diagnostic

2026-09-08 06:04 +08:00. One completed diagnostic capture, not an ordinary
performance comparison or a root-cause/fix acceptance. The remaining long
response stalls are predominantly before attachment forwarding; the observed
server TCP command/writer stage is prompt. Native receipt versus earlier
client input backpressure is not yet separated.

## Scope and evidence

Cell: `./.tmp/reflection/results/mixed-combined-up-response-handoff-diag-0908/`.
Raw: [RESPONSE_HANDOFF_20260908.raw.tar.gz](RESPONSE_HANDOFF_20260908.raw.tar.gz).
Observer: [RESPONSE_HANDOFF_TRACE_20260908.patch](RESPONSE_HANDOFF_TRACE_20260908.patch),
seven lab-only files atop95a5292, frozen and reversed before this run. No sampler,
controller, quantity, priority, network or queue-policy change. The existing
500Mbps asymmetric changing-loss/QoS/outage profile is unchanged; see
[the ordinary profile](RESPONSE_RETAINED_ORDINARY_20260908.md).

The probe completes exactly259,325,952B in49.986587s, reports41.503Mbps,
first confirmation0.448020s and maximum confirmation gap3.215542s. Instrumented
averages do not promote the candidate or replace its adverse ordinary result.
All line references below are original `server.log`/`client.log` lines in this
cell. Cross-process joins use Unix milliseconds, not independently initialized
role-relative monotonic clocks. Session18281795100200409851, Product stream0.

## Identity and observation boundaries

The observer's `path_index` is not one global namespace. Server TCP writer and
client authenticated-decode events carry wire PathId; client actor/attachment
events carry local runtime index. This capture's stable join is:

| Wire TCP PathId | Server native instance | Client runtime index | Client native instance | Attachment |
| --- | --- | --- | --- | --- |
| 0 | 3 | 0 | 2 | 0 |
| 1 | 4 | 2 | 4 | 3 |
| 2 | 2 | 1 | 3 | 1 |

QUIC is client runtime0/native1/attachment2. Instance and output-incarnation
numbers on opposite endpoints are not interchangeable. Exact stream/range and
per-carrier order, not numeric equality of every identity field, join copies.

`flush_end` is successful local encrypted write/flush acceptance, **not wire
delivery**. Authenticated decode is before the bounded reader-to-actor channel.
The reader may have waited delivering an earlier frame before attempting the
next read. Normal route completion and interlocked `mailbox_full_pending` are
different outcomes; the latter is not a successful mailbox transfer.
Attachment shared-send begins only after that attachment receives the frame.
Send-completion logs bound transfer, not its linearization instant; another
worker may log dequeue first. Queue snapshots are not exact frame positions.

## Actual frontier stalls, not maximum duplicate latency

Each row is an actual period with no ordered response-frontier advance. Every
listed winner is QUIC Original Data; no TCP repair wins these frontiers.

| Held F | Previous advance Unix ms / client line | Winning mux Unix ms / client line | Gap ms | Winning shared-send begin Unix ms / line | Shared-send begin to mux ms |
| --- | --- | --- | ---: | --- | ---: |
| 1119 | 1788818239734 /2180 | 1788818242152 /2315 | 2418 | 1788818242059 /2307 | 93 |
| 1147 | 1788818244193 /2436 | 1788818247409 /2617 | 3216 | 1788818247320 /2609 | 89 |
| 1161 | 1788818247409 /2617 | 1788818249520 /2764 | 2111 | 1788818249412 /2756 | 108 |
| 1203 | 1788818250830 /2867 | 1788818253597 /3039 | 2767 | 1788818253491 /3032 | 106 |

F1147 andF1203 stalls occur after the configured30--33s UDP outage. During
their plateaus the same relay actor applies156 and140 request ACKs, with maximum
adjacent intervals32 and30ms. Thus an already-applied response stuck for seconds
inside local socket writing does not explain these gaps: that branch does not
service those ACKs. Most observed delay precedes attachment shared-send, but
this does not establish when the QUIC native reader first obtained the frame.

## TCP command service versus delayed copies

All149 observed TCP response transactions have logged deltas of at most1ms
(millisecond timestamp resolution) for Product dispatch to command stage, command stage to native write
begin, and write begin to flush completion. There is no seconds-long server
command/writer hold for these recorded frames.146 have exact client decode
matches; three late copies `[1651,1665)`, `[1665,1679)`, `[1679,1693)` flushed at
1788818258945 (server908/913/918) have no decode before capture end.

| Exact copy / wire TCP path | Server dispatch / flush lines | Flush Unix ms | Client decode line / Unix ms | Flush to decode ms | Relation to actual F |
| --- | --- | --- | --- | ---: | --- |
| [1147,1161) /0 | 339 /345 | 1788818242213 | 2738 /1788818249260 | 7047 | QUIC advances at1788818247409, before decode |
| [1147,1161) /2 | 362 /365 | 1788818242414 | 2745 /1788818249345 | 6931 | Same losing-copy control |
| [1147,1161) /1 | 377 /380 | 1788818244238 | 3080 /1788818254401 | 10163 | Largest matched delay; already redundant |
| [1203,1217) /0 | 427 /433 | 1788818250038 | 3190 /1788818256667 | 6629 | QUIC advances at1788818253597, before decode |

These copies were admitted and locally flushed; the old H/F/EOF gate or server
writer backlog cannot explain their subsequent delay. Conversely,10.163s is
the latency of a losing duplicate, **not** the user-visible confirmation gap.

All seven TCP frames that actually advance F are Original Data, at offsets
800/852/969/982/995/1008/1021 (server dispatch176/187/233/238/243/248/253).
Their flush-to-decode is30--97ms and decode-to-mux0--12ms. No TCP repair wins F
in this capture. The F969 gap includes delayed new response production: its
Original dispatch at1788818234322 precedes winning mux1788818234373 by51ms;
it must not be misclassified as a2.422s transport hold of that response.

## Downstream complete-chain controls

Independent joins identify220 attachment/physical-instance/range keys, without
duplicate keys;218 have complete shared-send/dequeue/mux chains. Complete-chain
maxima are9ms shared admission,170ms shared residence,1ms dequeue-to-mux and
172ms total shared-send-begin-to-mux. The two incomplete tail keys are retained
as censored, not folded into these maxima: TCP runtime1/native3/attachment1
`[1245,1259)` has begin+complete only; TCP runtime2/native4/attachment3
`[1259,1273)` has begin only.

The largest useful shared residence is UDP `[1273,1287)`:
client3251/3252/3265/3266 at1788818257194/7196/7366/7366, withF1273->1287.
The queue remains visibly occupied (131 used slots at admission,129 remaining
at dequeue), but its exact observed residence is170ms, not several seconds.

All146 TCP decodes reconcile as76 complete mux chains, two forwarded but
unconsumed tails,51 successfully routed frames without a subsequent attachment
forwarder event, and17 routed after attachment retirement. The51 remaining
route observations are censored at Product completion; they are not necessarily
decoded after completion. This is not proof of a leak or that all146 were
delivered. The maximum complete chain is losing TCP runtime0/native2
`[1161,1175)`: decode client3168 at1788818256299; route3397/3398 at8313/8315;
shared-send3548/3550 at8648/8651; dequeue/mux3602/3603 at8714 (same Unix prefix
178881825). Total2.415s, including2.014s before actor routing. F is already1539.
This proves some delayed client input service, not its contribution to the
earlier QUIC-winning frontier stalls.

## Disposition and smallest unresolved question

Do not change copy deadlines, batching, FIFO, selection bias or congestion
parameters from this capture. Timely local flush plus late decode still spans
native transport and a reader that can be blocked on preceding channel work.
The actual-winning QUIC response's earliest receive boundary is absent; inspect
`run_client_udp_stream` and its shared `spawn_quic_path_reader` call, including
normal and write-interlocked forwarding, before declaring the next observer.
Any new capture must distinguish exact native decode from actor/mailbox service
without logging the unrelated upload payload stream. Ordinary timing acceptance
and the global comparison gates remain unsatisfied.
