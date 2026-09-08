# Persistent writer-idle ordinary observation — promotion stopped

2026-09-08. Category: ordinary practical evidence and bounded next diagnosis.
Read PERFORMANCE_METHOD_AND_LESSONS and CURRENT_CLOSURE_PLAN. This is an
adverse practical result, not a release, speed milestone or full-matrix pass.

## Identity and unchanged conditions

Ordinary candidate **b3dfef1**, frozen executable
`./.tmp/reflection/bin/prepared-idle-20260908/mptunnel`, runs at both endpoints.
The persistent-idle correction removes the independently reproduced all-refused
writer retry recurrence;535 focused controls and independent review preceded
this run. That mechanism proof did not establish the cause of the earlier75s
return hold. The normal optimized build took3m31s; a test-only `Bytes` import
warning is being scoped separately and is not an observed runtime cause.

The [profile and runner settings are unchanged](PREPARED_ORIGINAL_ORDINARY_20260908.md#identity-and-unchanged-conditions):
mixed combined upload, routed/mirrored500Mbps, upload70/20ms and return30/5ms
delay/jitter, the same five-second loss schedule, upload10Mbps at15–25s,
UDP outage30–33s,40s load,85s runner guard and90s probe boundary. Diagnostics
are off; no profile/harness edit or build/lab overlap. No ordinary repeat follows
this result merely to seek a favourable realization.

The result directory is
`./.tmp/reflection/results/mixed-combined-up-prepared-idle-candidate-0908/`.
It contains86 service rows and all five ordinary result files. The runner log
is `./.tmp/reflection/prepared-idle-ordinary-run.log`. Contextual comparison is
the previously archived **9720e4b** candidate, not a new contemporaneous control.
Independent loss realizations and unequal accepted work remain limitations.

Full raw files, ordinary build/run logs and nanosecond archive timestamps are
retained in [the raw archive](PREPARED_IDLE_ORDINARY_20260908.raw.tar.gz).

## Exact probe completion and runner-guard race

| Probe observation | Earlier9720e4b | Currentb3dfef1 |
| --- | ---: | ---: |
| Complete / exact ACK accounting |No / no |Yes / yes |
| Confirmed bytes |106114466 |212402176 |
| Locally accepted bytes |197984256 |212402176 |
| Probe elapsed seconds |85.671962 |85.399950 |
| Reported confirmed Mbps |9.909, partial lower bound |19.897, exact probe total |
| First confirmation seconds |.246881 |.570383 |
| Maximum completed confirmation gap seconds |1.464514, excludes terminal silence |15.955222 |
| First local write seconds |.113258 |.215024 |
| Maximum completed local-write gap seconds |1.286327 |2.327343 |

Current probe has one complete stream, equal accepted/confirmed totals, valid
ACK accounting and no probe errors. Nevertheless, the runner exits1 with
`probe failed to settle` at run.py140. Do not silently convert this into a
successful ordinary runner result or discard the valid probe transcript.

The available order evidence **does not support teardown-caused completion**.
Filesystem modification times captured before archival, inUTC+08:00:

| File | Last modification time |
| --- | --- |
| `probe.json` |2026-09-08 13:57:36.479132486 |
| `service.jsonl` |2026-09-08 13:57:36.639134420 |
| runner log |2026-09-08 13:57:37.131140370 |

The completed probe result precedes the final telemetry write by160.002ms.
The runner writes/flushes that sample at137–138, checks the previously captured
elapsed value at139–140, then enters finally and calls stop_products at151.
It does not recheck child.poll after collecting the sample. Thus these files
support probe completion during the final sampling iteration, before the
guard-triggered teardown, while the runner still reports its stale guard
condition. There is no precise process-exit/signal trace; retain this source/
timestamp inference rather than inventing one. Client/server/probe-error logs
are empty. Preserve these nanosecond timestamps in this report because a
whole-second archive timestamp would lose the ordering evidence.

The last management snapshot is not final target accounting: server Unix
1788847056091 has T183077677, while the later exact probe confirms212402176.
Sequential sampling and the final buffered release explain why these are
different observation boundaries; do not replace the probe total with T or
claim that the intervening release was measured by management. The complete
probe rate is retained, but comparing it to the earlier partial9.909Mbps scalar
does not establish a speedup. Severe timing failure still stops promotion.

## Forward and return phases

S is consumed local source, not native-claimed wire horizon C. T is ordered
target-socket acceptance, not raw receiver/reassembly F. Rs/Rc are server-read
and client-local-written reply bytes; Rc is not mux F. Their differences span
multiple buffers and service owners and are not exact per-carrier debt.

| Current nominal sample seconds |S |T |Rs |Rc |
| --- | ---: | ---: | ---: | ---: |
|7.000812 |180289165 |130476269 |322 |266 |
|10.001192 |181266669 |161659405 |462 |266 |
|12.001412 |182053101 |180337165 |560 |266 |
|15.001728 |183101677 |180337165 |560 |280 |
|16.001823 |183560429 |180337165 |560 |280 |
|31.051194 |190429709 |180783917 |700 |294 |
|40.080962 |194379405 |181254669 |812 |308 |
|50.082071 |200337645 |181480813 |896 |322 |
|69.085370 |212402176 |181480813 |896 |364 |
|81.086696 |212402176 |181480813 |896 |392 |
|82.086829 |212402176 |181492813 |910 |896 |
|85.087161 |212402176 |183077677 |1064 |1064 |

Early target delivery remains substantial: T at10s is161659405 versus the
earlier142105730. During7–14s Rc stays266 while T rises130476269→180337165
and Rs322→560. This is held return work while forward service progresses,
not an exclusively forward-starved interval. Later real forward plateaus
coexist with slow reply release:

| Counter / value | Exact management Unix-ms interval | Sampled hold |
| --- | --- | ---: |
|Rc266 |1788846978085–1788846985085 |7.000s |
|Rc280 |1788846986086–1788847001086 |15.000s |
|Rc294 |1788847002086–1788847009086 |7.000s |
|Rc308 |1788847010085–1788847019085 |9.000s |
|Rc336 |1788847027086–1788847036086 |9.000s |
|T180337165 |1788846983091–1788846987091 |4.000s |
|T180664845 |1788846990091–1788847000091 |10.000s |
|T181254669 |1788847008091–1788847018092 |10.001s |
|T181480813 |1788847021091–1788847052091 |31.000s |

These are constant sampled-counter spans, lower bounds rather than exact
event gaps. The earlier run instead held Rc267 for75.001s and T178383138
for69.000s. Current progress is intermittent rather than completely frozen,
but15.955222s exact confirmation gaps and31s sampled target holds are not
acceptable service. S reaches the entire accepted212402176B at69s; its later
16s flat span is source exhaustion, not by itself a new source-read stall.
At that point S−T is30921363B. Nominal completion−40 is45.399950s, not an
exact EOF/drain timestamp: Product source consumption continues after load40s.

## Native receive consumption and costs

The current client TCP sockets retain ports46998/46982/46980 throughout the
selected interval. All three Recv-Qs are zero at4s. Native received/control
backlog then accumulates and drains slowly:

| Nominal sample seconds |Sum client TCP Recv-Q bytes |Sum(bytes_received−Recv-Q) bytes |
| --- | ---: | ---: |
|7 |369754 |1014279 |
|16 |880955 |1097229 |
|50 |583374 |1523214 |
|69 |416395 |1694617 |
|75 |104165 |2010671 |
|80 |3818 |2112088 |
|81 |0 |2116074 |
|85 |0 |2214958 |

Across16–85s this receive-consumption quantity advances1117729B, versus
111979B on the earlier run's different sockets. These are observed absolute
native byte deltas, not equal-work performance or logical-reply bytes. The
current TCP receive backlog finally empties at81s; Rc is still392 then and
jumps to896 at82s. An asynchronous snapshot cannot locate the exact remaining
response frame or prove decode/actor scheduling order.

Server→client46980 is receive-window limited earlier: at16s Send-Q/notsent is
109108B and cumulative rwnd_limited7896ms. By31s Send-Q is0 and cumulative
rwnd_limited18812ms; that total no longer rises through the selected85s row.
The other two sampled server queues are already empty at31s. Unlike the
earlier endpoint42866's persistent return Send-Q, this later span is not
explained by that same sustained server socket backlog. Conversely, client
46998 still has3324220B Send-Q/3263404B notsent at81s, proving continuing
forward native backlog but not ownership of the exact blocked target range.

| Sampled cost | Earlier9720e4b | Currentb3dfef1 |
| --- | ---: | ---: |
|Upload class byte delta |251008534 |305767105 |
|Return class byte delta |14483460 |12542220 |
|Upload packets / drops |201896 /4047 |243116 /4798 |
|Return packets / drops |66140 /877 |65586 /1055 |
|Peak client RSS KiB |519200 |480664 |
|Peak server RSS KiB |113412 |132188 |
|Client lifetime CPU% peak/final |127 /103 |104 /99.9 |
|Server lifetime CPU% peak/final |40.7 /5.2 |35.7 /5.3 |

Current stable PIDs are client261767/server267837. Current peak client RSS is
at83.086934s; server peak is at3.000389s. Final RSS is386560/103612KiB.
Client lifetime CPU peaks at13.001518s; server at9.001093s. CPU is ps lifetime
average, not interval utilization or time spent in a particular owner/handler.

Class1:10 deltas use router eth1 upload and eth0 return, not summed qdisc
levels. Nominal sampled windows are0–85.090909s earlier and0–85.087161s
current; commands/management snapshots within each row are sequential and
not one atomic timestamp. Class bytes include framing, control, native retries
and Product copies, not repair-only cost. Unequal accepted/confirmed work and
the old censoring forbid normalized cost or amplification-gain claims.

## All86 raw confirmation bins

Untrimmed Mbps by one-second index0–85; the final bin can be partial. Unlike
the earlier invalid-ACK transcript, these bins are present and retained:

```text
0–9:   1.049,5.360,4.171,542.926,61.054,174.684,50.620,0,0,0
10–19: 0,0,0,0,86.648,0,0,0,0,0
20–29: 0,0,0,0,0,0,0,0,0,0
30–39: 57.864,0,0,0,0,0,0,0,29.456,0
40–49: 0,0,0,0,0,0,0,0,23.401,0
50–59: 0,0,0,0,0,40.466,0,0,0,0
60–69: 0,0,0,0,0,36.508,0,0,0.288,0
70–79: 0,116.392,0,0,0,0,0,0,35.268,0
80–85: 0,185.694,10.057,0.857,1.669,234.788
```

The542.926Mbps bin and final234.788Mbps bin describe buffered confirmation
observations, not instantaneous physical link service. Zero confirmation bins
cannot be substituted for zero ordered target writes.

## Disposition and next bounded discriminator

The independently proven metadata retry recurrence is corrected, but practical
receive service and settlement remain severely delayed. The current exact
probe completion near the observation boundary is an important distinction
from the earlier incomplete result, not a successful ordinary gate or proof
that the correction caused every change between realizations.

Next is the declared sparse exact-reply-stage/cost diagnostic: actual response
publication/write, client decode→route/shared input→mux F/local delivery, with
bounded preparation/claim aggregates and honest pending-wait accounting.
Determine which exact response range and service owner explain the persistent
held-return intervals. No next ordinary trial, response-parity stack, controller
tuning or queue-size change is justified by these aggregate counters alone.
