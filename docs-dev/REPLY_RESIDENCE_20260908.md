# Reply residence service diagnostic — 2026-09-08

**Disposition: exact local reader-backpressure evidence, not performance
acceptance.** This unchanged-runtime diagnostic completes387579904B in
52.491225s. The longest already-read reply residence is3.247s, including
1.961027s of measured preceding nondata mailbox-send await contained within
its predecode interval, followed by599ms from decode to local delivery.
The earlier15.686s reply residence is not reproduced. The largest3.508291s
confirmation gap is a different interval, mostly before the next reply is
read by the server. No runtime correction follows from an aggregate label.

## Scope, identity and preservation

The [terminal-service capture](TERMINAL_SERVICE_20260908.md) established
already-read reply[1041,1055) taking15.686s to reach the client but lacked
intervening publication/write/decode boundaries. This capture asks which
actual stage retains the winning reply, not whether all delay is Native.

Runtime b783cd6 plus the independently reviewed18-file
[REPLY_RESIDENCE_TRACE overlay](REPLY_RESIDENCE_TRACE_20260908.patch) was
built warning-free in3m31s and frozen as
`./.tmp/reflection/bin/reply-residence-20260908/mptunnel`.
All hooks were reversed before the one capture; both endpoints used the
frozen diagnostic. No build overlapped the lab, no harness/profile changed,
and no favorable ordinary repeat or performance promotion is authorized.

The [fixed profile](ADVISORY_OWNER_SERVICE_20260908.md#fixed-ordinary-profile-and-order)
is mirrored/routed500Mbps, upload70/20ms and return30/5ms delay/jitter;
five-second upload loss epochs[3,8,5,6,10,3,5,8]% and return
[1,2,.5,3,2,.5,1,2]%; upload10Mbps at15–25s; UDP outage30–33s;
40s offered load,85s runner guard and90s total probe boundary.
Actual sample flags switch outage on at elapsed30.003944 and off33.107218;
the schedule is not an exact packet-by-packet realization.

Raw directory:
`./.tmp/reflection/results/mixed-combined-up-reply-residence-0908/`.
The [raw archive](REPLY_RESIDENCE_20260908.raw.tar.gz) contains all five
result files, `reply-residence-0908-build.log`,
`reply-residence-0908-run.log`, and the observer patch: eight files.
There are53 service samples and2132/1368 client/server log lines; the last
server line is a close warning, not an additional payload event.
The run log records exit0/53.128989s. There is no observation-guard censoring
in this capture; finite sampling and unobserved stages remain limitations.

C/S below mean one-based client/server log lines; L denotes service.jsonl.
All event times in exact-join tables are Unix milliseconds minus1788862600000.
Client PID278711 and server PID284711 have different monotonic origins:
do not subtract their process-relative clocks. Logical stream0 belongs to
session11730546144327377031.

| Carrier | Client runtime / physical / attachment | Server wire PathId / physical | Native response stream |
| --- | --- | --- | --- |
| QUIC |0 /1 /1 |0 /1 |ordinary H3 request4; repair8 |
| TCP |0 /2 /0 |1 /2 |wire1, remote port7443 |
| TCP |1 /4 /3 |2 /4 |wire2, remote port7443 |
| TCP |2 /3 /2 |0 /3 |wire0, remote port7443 |

These exact memberships stay active in all53 management snapshots.
Physical/H3 integers are not globally unique identities. TCP authenticated
decode lacks physical identity; session+wire path+port and subsequent exact
route/shared records supply the join. QUIC repair writes label their wire
path with `path_index`, unlike ordinary writes' `path_id`; native request8
must not be merged with ordinary4. Repair `target_incarnation` is a selector
identity, not interchangeable with writer `path_instance_id` (e.g. S552
target_incarnation1 versus S554 writer physical2).

## Complete probe and all confirmation bins

| Probe field | Result |
| --- | ---: |
| Local accepted = target-confirmed bytes |387579904 |
| Elapsed seconds / goodput Mbps |52.491225 /59.070 |
| First confirmation / maximum closed confirmation gap seconds |.442207 /3.508291 |
| First local write / maximum local-write gap seconds |.101566 /3.211389 |
| Elapsed beyond40s offered-load boundary |12.491225s |
| Complete / failed streams; probe and runner exits |1 /0;0 /0 |
| Accounting |Exact valid target-sink ACK; not a lower bound |
| Probe errors / stderr |None / empty |

All53 raw one-second confirmation bins follow in Mbps. Indices are zero-based
and the last bin is partial. These measure confirmation arrival, not physical
wire capacity or per-second forward target service. Neither the trimmed47-bin
55.255Mbps average nor this diagnostic's59.070Mbps replaces full timing.

```text
 0: 3.144, 11.534, 266.863, 7.34, 222.822, 3.263, 4.602, 256.377, 39.963, 158.742
10: 40.37, 43.633, 82.934, 49.304, 77.595, 197.519, 0, 0, 0, 2.097
20: 17.729, 16.466, 0, 0, 0, 16.798, 347.034, 23.733, 0, 71.163
30: 0, 75.168, 20.253, 116.392, 75.668, 4.714, 166.322, 8.337, 1.573, 0
40: 0.524, 49.187, 0, 68.02, 144.476, 34.055, 58.807, 27.314, 24.354, 42.327
50: 34.774, 78.115, 109.235
```

## Exact reply accounting and winning ranges

All121 positive server reply reads total1646B, followed by one zero-byte
target EOF. The positive reads and121 Original dispatches form gap-free
[0,1646), with no queued response bytes preceding any observed read.
Original publication follows its read by at most6ms (S26→S28 for[20,31));
publication→positive local write is at most4ms. These facts exclude a
multi-second Original admission/publication hold in this capture; waiting
until a later recovery-copy publication is not Original admission latency.

There are151 accepted-copy dispatches totaling2063B, hence272 response Data
transactions /3709 payload bytes including copies. Every transaction matches
a successful local write and an exact client decode/route/shared/mux record:
112 ordinary-QUIC,3 repair-QUIC,157 TCP. No transaction is inferred only from
a frame count. The272 mux applications contain119 frontier-advancing frames:
109 Original and10 copies; by ingress101 QUIC and18 TCP. The other153
applications include two useful out-of-order frames, not153 useless copies.
No QUIC-repair frame wins in this capture.

Client successful local writes total1646B in118 calls. [891,905) and
[905,919) coalesce into one28B write at C990; C1053 writes42B after
[933,947) releases two already-buffered frames[947,961),[961,975).
Those suffix frames arrived on TCP runtime1/physical4, at C1025/C1032,
before TCP runtime2/physical3's prefix at C1052. Thus crediting all42B to
one newly arrived copy would erase the actual byte provenance.
All119 advancing frames were joined to their producer/write/ingress stages;
the ranked and contrary cases below retain the decisive exact records.

### Largest already-read residence: [863,877)

| Stage | Exact record / short Unix time |
| --- | --- |
| Server reads14B from target socket |S557@76734 |
| Enqueue / Original dispatch |S558@76734 /S559@76735 |
| QUIC ordinary4 local write begin / end |S560/S561@76735, result=ok |
| Client same-reader prior response mailbox completes |C868@77417, [849,863) |
| Winning reply authenticated decode |C885@79382 |
| Its reader mailbox completes |C886@79383, measured1584us send-await |
| Client route begin / end |C894/C895@79582, result=queued |
| Shared input send begin / complete |C898@79913 /C899@79916 |
| Shared input dequeue |C900@79981 |
| Mux F863→877 / successful local14B write |C901/C902@79981 |

Read→delivery3247ms =1ms read→publication +2647ms local-write→decode
+599ms decode→delivery. The latter splits into200ms decode→route,
331ms route→shared-send-begin,3ms shared-send-await and65ms
shared-send-complete→dequeue/mux/local-write, at millisecond resolution.
Same-millisecond records do not imply zero CPU cost.

C885 reports996 preceding nondata frames,1,961,027us aggregate send-await,
and112,516us largest individual await. The observer resets this aggregate
only after a response Data mailbox send completes. Its entire scope is
therefore after C868@77417 and before C885@79382: a1965ms wall interval
inside this reply's own2647ms write→decode interval. Approximately1.961s
of that interval is positively accounted for by local reader mailbox-send
await, not by a Native-only explanation. The remaining approximately686ms
of the total predecode interval is not thereby assigned to the network.

The aggregate is elapsed async-send residence, not active CPU, not exclusively
request DataACKs, and not proof of one uninterrupted blocked task or a named
downstream handler. It includes scheduler resumption and mailbox backpressure;
raw frame types and exact consumer service costs are not in this sparse log.
The read/decode boundaries do not establish when the encrypted bytes first
became readable. Existing Native progress and a full mailbox are not a
replacement for this exact contained timing proof.

### Largest postdecode winner and contrary cases

| Winning range / carrier | Read; winning publication; local write | Decode; route; shared begin/complete | Dequeue; mux; local write | Read→local / decode→local |
| --- | --- | --- | --- | --- |
| [849,863), Q ordinary4 |S547/S549/S551@76421 |C867@77415;C869@77692;C873@78008/C874@78012 |C877/C878/C879@78104 |1683 /689ms |
| [1157,1171), Q ordinary4 |S932/S934@92221;S936@92222 |C1451@92994;C1457@93080;C1463/C1464@93174 |C1467/C1468/C1469@93197 |976 /203ms |
| [919,933), TCP wire0 copy |S632@81621;S688/S693@82981 |C1033/C1035/C1039/C1040@83017 |C1043/C1044@83017;C1048@83018 |1397 /1ms |
| [933,947), TCP wire0 copy |S657@81953;S690/S696@82981 |C1034/C1037/C1041/C1042@83017 |C1051/C1052/C1053@83018 |1065 /1ms; releases through975 |
| [0,10), Q ordinary4 |S1/S3@49165;S5@49166 |C1/C3/C5/C6@49205 |C7/C8/C9@49206 |41 /1ms |
| [1633,1646), TCP wire1 copy |S1352@101072;S1364/S1367@101224 |C2116/C2117/C2119/C2120@101254 |C2121/C2122@101254;C2123@101255 |183 /1ms |

The largest postdecode winning interval is689ms for[849,863), not599ms
for the largest total residence. Its C867 preceding-nondata aggregate
(1554 frames /1,264,769us) starts after C735@64614, the previous response
on this ordinary QUIC reader. It spans12.801s and cannot all be charged to
[849,863)'s994ms write→decode interval. Responses on TCP do not reset it.

The larger3.068s write→decode interval S566@76932→C903@80000 belongs
to TCP wire2's losing[821,835) copy; it does not block the current frontier
then. QUIC Original[877,891) likewise has3003ms write→decode
S606@80532→C1078@83535 but loses. These are not substituted for the
winning service path. Final[1633,1646) was originally written on QUIC at
S1357@101073; its later decode C2124@101563 loses to the TCP copy above.
Final logical success C2132@101564 has reply frontier1646, zero
sender_queue_bytes/send_reinjection_bytes, and
payload387581550=request387579904+reply1646.
The server's H3_NO_ERROR close warning follows success, not an earlier
independent transfer failure.

## Confirmation gaps versus forward/return stages

The largest closed confirmation gap is C742@64614 (F751)→C771@68122
(F765),3508ms; the probe's finer clock reports3.508291s. Server read S468
of[751,765) occurs only at67257:2643ms is before the read,865ms after it.
Original TCP wire0 dispatch/write S470/S473 occur at67257; the winning
TCP wire2 copy S480/S483 is at68091 and decode/route/shared/mux/local
C764–C771 at68122. Original wire0 only decodes C772@68644, losing.
This supports useful recovery in that exact episode, not guaranteed
faster copies or an unexplained865ms admission hold.

The second3260ms gap C801@70535→C815@73795 (F793→807) has3224ms
before server read S500@73759 and36ms after. Its Original TCP wire0
S502/S505@73760 reaches decode/route/shared/mux/local C808–C815@73795.
Neither gap is entirely already-produced reply residence. The server-read
boundary also does not distinguish target sink ACK cadence, earlier forward
prefix service or delayed server reads. No exact request claim/receiver
frontier trace was enabled here.

S/T/Rs/Rc below are management I/O counters: client source bytes consumed /
server ordered target-socket bytes written / server target-reply bytes read /
client local reply bytes written. S is not request claim C; T is not raw
Product receipt; Rc is not by definition mux F. Exact event joins above
establish F only at those particular records.

| L / elapsed seconds | S | T | Rs | Rc |
| --- | ---: | ---: | ---: | ---: |
|2 /1.000171 |67501921 |458593 |31 |31 |
|11 /10.001256 |188940129 |121831265 |471 |471 |
|16 /15.002274 |225866097 |158757233 |709 |709 |
|17 /16.002374 |250478433 |183369569 |751 |751 |
|19 /18.002588 |250478433 |183369569 |751 |751 |
|20 /19.002701 |252772193 |185663329 |765 |751 |
|23 /22.003026 |256209265 |189100401 |793 |793 |
|26 /25.003366 |256209265 |189100401 |793 |793 |
|29 /28.003708 |306671985 |255807913 |877 |849 |
|30 /29.003836 |311819793 |257043697 |877 |849 |
|32 /31.107004 |324152561 |257043697 |877 |863 |
|35 /34.126917 |347713536 |280604672 |975 |919 |
|36 /35.127017 |348695940 |281587076 |1031 |1031 |
|41 /40.127539 |370843729 |303734865 |1101 |1101 |
|45 /44.127982 |387579904 |332280489 |1185 |1157 |
|53 /52.128825 |387579904 |377956917 |1619 |1619 |

Longest sampled constant T is189100401 on L23–L26, server
Unix1788862670740→2673740:3000ms, with S/T/Rs/Rc all unchanged and
S−T exactly64MiB. This is a real target-service plateau, not merely delayed
return confirmations. T183369569 is also flat for2000ms at L17–L19.
L17–L20 Rc751 stays flat2999ms while T later adds2293760B and Rs adds14B;
that interval spans both forward and already-produced return waiting.

Around the maximum reply residence, L29–L32 has Rs877 constant3000ms,
Rc849→863, S306671985→324152561 and T255807913→257043697.
T itself is flat2000ms on L30–L32 while S adds12332768B. Thus both
forward ordered service and reply residence matter; no single stage owns
every contemporaneous zero confirmation bin. Millisecond exact joins use
generated Unix timestamps, not the slightly drifting runner elapsed column.

S is first sampled at full387579904 on L45, then stays full through L53
while T adds45676428B. The last sample still lacks9622987 target bytes
and27 delivered reply bytes; exact final completion is later than sampling.
The12.491225s after the40s offered-load boundary is not a clean fixed-work
drain starting from full source acceptance. There is no long sampled
full-target/no-final-reply tail matching the earlier ordinary run.

## Native socket consumption and bounded cost

Three stable established TCP sockets connect client ports60204/60218/60226
to server7443. Their aggregate counters include encrypted protocol/control
traffic, not only the small response payload. The sparse logs do not map
each local port to a physical instance; the exact frame joins use the
session/wire identity table instead.

| Service rows | Client aggregate Recv-Q | Cumulative bytes_received | Implied bytes consumed |
| --- | ---: | ---: | ---: |
| L29→L32 |181965→55277 |2324615→2559978 |362051 |
| L30→L33 |330681→0 |2550424→2563383 |343640 |

Consumption is delta bytes_received minus delta Recv-Q across the same
three live sockets. These intervals overlap the reply-residence episode
but are coarsely sampled; ss collection precedes its management snapshot
and is not atomic with it. They exclude total TCP receive-consumption
silence, not a particular blocked reply/handler. Maximum sampled aggregate
client Recv-Q is330681B at L30; it is zero at L33. Native socket progress
does not erase the independently measured QUIC reader mailbox await.

| Sampled process cost | Client | Server |
| --- | ---: | ---: |
| Initial RSS KiB |99372 |30180 |
| Peak RSS KiB |307032 (L44) |155832 (L29/L30) |
| Final sampled RSS KiB |302128 |150896 |
| Maximum observed ps %CPU |69.8 |34.8 |
| Final observed ps %CPU |50.3 |15.6 |

ps %CPU is process-lifetime-average utilization at the observation, not
per-second CPU, total CPU time, or an actor/Native cost breakdown. The
diagnostic logging cost and allocator-retained RSS remain included; no
latency inference follows from a low sampled process percentage.

Router HTB class1:10 counters, first→last observation (L1→L53):

| Direction / interface | First bytes | Last bytes | Sampled delta |
| --- | ---: | ---: | ---: |
| Upload / eth1 |988542 |508811156 |507822614 |
| Return / eth0 |42556 |22472343 |22429787 |

Use these directional class deltas once; do not sum parent HTB/netem
counters. They cover only the sampled window and include protocol overhead,
retransmissions and copies, not final wire totals or repair-only costs.
The2063 observed reply-copy payload bytes are a different scope from
request copies or those whole-path counters. Different random realizations,
diagnostic overhead and unequal completed work prevent causal normalized
cost comparisons with preceding ordinary or diagnostic runs.

## Narrow conclusion and next boundary

This capture falsifies a Native-only account of the whole longest winning
reply's write→decode delay: a1.961027s local reader-send-await subset is
directly contained. It also falsifies multi-second Original publication or
successful local socket-write residence as the main owner of that reply.
The remaining postdecode599ms/689ms cases are real local handoff residence,
not proof of a specific queue/actor algorithm defect.

The unresolved causal boundary is the actual consumer service responsible
for the measured nondata reader backpressure and downstream route/shared
residence. Before a correction, distinguish useful required work from
avoidable repeated work or unavailable downstream service with the existing
source/model evidence. Do not call all996 frames ACKs, revive a batching
policy from counts, infer Native congestion from predecode time, or transfer
this cause to the prior15.686s interval without its missing boundaries.
No third ordinary run, new runtime proposal or performance promotion is
made by this report.
