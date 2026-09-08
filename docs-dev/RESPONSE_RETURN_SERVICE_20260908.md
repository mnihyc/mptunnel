# Response return service diagnostic — 2026-09-08

**Disposition: current local receive-service coupling is demonstrated; no
performance acceptance or runtime correction.** An exact TCP Original is
already authenticated and decoded before its byte becomes the missing
frontier, yet remains locally unserved throughout a943ms frontier hold.
Other useful replies wait behind typed credit handoffs. Conversely, the
largest6.510292s confirmation gap is almost entirely before the next reply
is read by the server. These are different mechanisms, not one global
“ACK cost” or “Native delay” explanation.

## Declared scope and identity

Following the [active forecast](CURRENT_CLOSURE_PLAN.md) and
[mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md), this is one unchanged
harsh mixed upload on ordinary runtime `0449b9f` plus the19-file
[response-only observer](RESPONSE_RETURN_SERVICE_TRACE_20260908.patch).
The diagnostic build is warning-free1m23s, frozen at
`./.tmp/reflection/bin/response-return-service-20260908/mptunnel`.
All observer changes were reversed before capture; no build overlapped it.
No policy, controller, resource limit, queue, sampler or timer was changed.

The existing mirrored/routed profile retains500Mbps, upload70/20ms and
return30/5ms delay/jitter, five-second upload loss epochs
[3,8,5,6,10,3,5,8]% and return[1,2,.5,3,2,.5,1,2]%, upload10Mbps
at15–25s and UDP outage30–33s. Offered load is40s, runner guard85s and
total probe boundary90s. This is a causal stress diagnostic, not a
high-capacity acceptance result or an equal-work ordinary comparison.

Raw directory:
`./.tmp/reflection/results/mixed-combined-up-response-return-service-0908/`.
The [raw archive](RESPONSE_RETURN_SERVICE_20260908.raw.tar.gz) preserves
the result files and build/run evidence. The observer patch is linked
separately. There are1888 client and850 server log lines, all diagnostic
records, and46 management samples. Runner exit0 takes46.007783s;
`probe.err` is empty.

C/S mean one-based client/server log lines. All exact timestamps below are
Unix milliseconds unless explicitly suffixed microseconds. Client PID312742,
server PID318489, logical stream0 and session13604304550869407499 are
capture-specific. Client/server process monotonic origins differ and must
not be subtracted across endpoints.

| Carrier | Server wire / physical / attachment incarnation | Client runtime / physical / attachment | Native stream |
| --- | --- | --- | --- |
| QUIC |0 /3 /3 |0 /2 /1 |ordinary H3 request4; repair8 |
| TCP wire0 |0 /1 /4 |2 /1 /3 |shared TCP session, remote port7443 |
| TCP wire1 |1 /4 /1 |0 /4 /0 |shared TCP session, remote port7443 |
| TCP wire2 |2 /2 /2 |1 /3 /2 |shared TCP session, remote port7443 |

These are mapped identities, not equality of integer namespaces. TCP
authenticated decode has session/wire path/port but no physical identity;
later exact route/shared records and management supply the physical mapping.
QUIC interlocked routing may have H3 identity only. Repair H3 request8 is
separate from ordinary4. Neither H3 nor Product offsets are packet offsets.

## Complete probe and all raw confirmation bins

| Probe field | Result |
| --- | ---: |
| Local accepted = target-confirmed bytes |388759552 |
| Exact completion / complete streams / failed streams |true /1 /0 |
| Elapsed seconds / goodput Mbps |45.495787 /68.360 |
| First confirmation / largest closed confirmation gap, s |0.475921 /6.510292 |
| First local write / largest local-write gap, s |0.133044 /6.250503 |
| Elapsed beyond40s load boundary |5.495787s |
| Accounting |valid exact target-sink ACK, not a lower bound |
| Probe / runner exits; errors |0 /0; none |

Elapsed after the load boundary is not automatically drain from full source
acceptance: a duration-limited local write may still be finishing.
This workload has no separate persistent echo series. All46 raw1s
confirmation Mbps bins follow; the last is partial. These are buffered
confirmation arrivals, not wire rate or per-second ordered target service.
The trimmed65.082Mbps summary is not substituted for full timing.

```text
0: 4.716, 9.844, 9.437, 409.469, 152.140, 74.117, 33.362, 133.073, 0.000, 56.291
10: 35.556, 78.119, 34.987, 58.903, 77.220, 103.048, 165.483, 0.000, 0.665, 0.000
20: 0.000, 0.000, 0.000, 0.000, 11.010, 278.353, 41.655, 19.975, 11.202, 167.004
30: 0.000, 0.000, 0.000, 0.000, 270.437, 0.000, 0.000, 116.540, 5.147, 24.597
40: 0.000, 151.896, 93.043, 280.035, 114.258, 88.496
```

## Exact source, transaction and delivery conservation

All83 positive target reply reads,83 Product source enqueues and83 successful
Original claims total1122B and form gap-free[0,1122), without overlap.
Each enqueue records U_before=0, U_after=its payload length and
offset=C+U_before; each claim advances C by that payload and leaves U=0.
Every enqueue is paired with its preceding actual socket read, not with
an actor notice. All reads use the regular path in this cell.

Read completion→source enqueue is at most1ms, enqueue→Original claim at
most3ms, and claim→positive local write at most1ms. Examples of the3ms
enqueue-to-claim maximum are S639→S646 for[829,843) and S765→S766
for[997,1011). Those observations exclude a multi-second prepared-source
assignment hold for these small replies, not for all workloads. Socket
availability before a read begins remains unobserved.

The83 Originals plus145 accepted repair dispatches (1990B) produce228
Data transactions /3112 payload bytes. All228 have matching write-begin
and positive local-write completion:69 ordinary QUIC,5 repair QUIC and154
TCP. TCP flush and QUIC write completion mean local acceptance, not wire
receipt. Repair dispatch is not relabeled as Original commitment.

The client has227 exact decode/route/shared/mux transactions totaling3099B.
All227 match their server producer and successful local write by range and
mapped identity; no duplicate write key is ambiguous in this capture.
The only unobserved transaction is final QUIC Original[1109,1122),
S847@1788879511377, whose TCP wire2 copy wins at C1887/C1888@
1788879511564 after S850@1788879511541. Capture ending after logical
success does not prove that losing Original was lost on the network.

There are80 frontier advances and80 positive local writes totaling1122B.
Winning ingress comprises57 QUIC Originals,9 TCP Originals,2 QUIC copies
and12 TCP copies. Of147 nonadvancing mux applications,3 are useful buffered
suffixes, not duplicates:

| Buffered suffix | Initial mux / current frontier | Later prefix release and successful write |
| --- | --- | --- |
| [759,773), TCP runtime2 |C1250@1788879498515 /F717 |C1273/C1274@1788879500968, F745→773,28B |
| [899,913), TCP runtime0 |C1569@1788879507739 /F885 |C1575/C1576@1788879507768, F885→913,28B |
| [983,997), TCP runtime2 |C1666@1788879509406 /F969 |C1731/C1732@1788879509733, F969→997,28B |

The other144 applications are already-covered arrivals. Crediting every
released28B to its newly arrived14B prefix would erase suffix provenance.
Across all80 advancing frames, mux→successful local write is at most7ms.
A fast final local write does not eliminate upstream routing residence.

## Material local counterexample: the missing Original is already decoded

TCP wire2 Original[325,339) is authenticated at C461@
1788879477717. F becomes325 only later, at C463@1788879477742.
Therefore the exact next required bytes are already inside the client for
the entire subsequent943ms frontier hold.

| Stage | Exact event |
| --- | --- |
| Server read / enqueue / Original claim |S209@1788879473420 /S210@1788879473421 /S211@1788879473421 |
| TCP wire2 positive local write |S213@1788879473421 |
| Client Original authenticated decode |C461@1788879477717 |
| Missing frontier becomes325 |C463@1788879477742 |
| QUIC repair local write / decode |S330@1788879478363 /C466@1788879478401 |
| QUIC repair route result / shared send |C467@1788879478405 /C468–C469@1788879478641–1788879478645 |
| Original TCP route begin / result |C470–C471@1788879478656–1788879478658 |
| QUIC repair wins mux / local write |C473–C474@1788879478685 |
| Original TCP shared send / late losing mux |C475–C476@1788879478877–1788879478878 /C478@1788879478919 |

Original decode→route begin is939ms; decode→its late mux is1202ms.
This is not a slow losing-copy age substituted for useful service: the
Original could cover the actual missing prefix before F reaches it, but
the already-decoded bytes remain locally unserved until after a later copy
wins. A missing-native-input-only explanation for this943ms interval is
falsified. The exact downstream handler owning that residence and the
cost of an alternative service design are not established, so943ms is
not a promised removable gain.

Adjacent[353,367) has a similar contrary ordering: TCP Original decode
C490@1788879480197 precedes QUIC-copy decode C492@1788879480241,
but copy mux C497@1788879480436 wins before Original mux C504@
1788879480762. Do not infer that earlier physical arrival necessarily
becomes earlier Product delivery.

## Typed reader work, not an ACK-only CPU explanation

All151 typed predecessor records reconcile the68 ordinary-QUIC decoded
summary records exactly: counts add, summed send-await durations add, maxima
agree, and each group shares the decoder's previous-response reset window.
The final unresolved/idle reader tail is not summarized without a next
response. Counts are frame sends observed before those responses, not bytes
or handler calls.

| Predecessor Frame kind | Count | Sum of send-await elapsed, us | Largest single send-await, us |
| --- | ---: | ---: | ---: |
| STREAM_MAX_DATA |15998 |12405800 |99849 |
| STREAM_ACK |6138 |3276955 |48559 |
| STREAM_REQUALIFY_ACK |23 |3433 |1186 |
| PATH_PROOF_ACK |2 |4 |3 |
| PATH_PROOF_DATA |1 |1 |1 |

Credit frames dominate these measured waits, rejecting the earlier assumption
that every nondata predecessor represents ACK processing. These are elapsed
existing mailbox-send waits, including task scheduling/backpressure, not
exclusive CPU time, lock hold time or a measured consumer's execution cost.
Neither all15.686193s nor a selected kind's sum is a whole-run speed budget.

### A winning head with contained local wait

QUIC Original[269,283) is read/claimed/written at S183/S185/S187@
1788879472095. F269 starts C353@1788879474028 and advances
C399@1788879475281:1253ms. Authenticated decode C388 is at
1788879474749; its actual read starts at1788879474749500us and returns
after15us. Route C392/C393 is1788879475010/1788879475013,
shared send C396/C397 is1788879475209/1788879475213, and local
delivery C400 is1788879475281. Postdecode residence is532ms.

C390's547 STREAM_MAX_DATA predecessors total983611us within
[1788879473526895,1788879474749515)us; all kinds total1209388us.
The window begins before this exact F hold. Subtracting the maximum possible
wait outside the hold leaves conservatively about707ms of all-kind wait
and481ms of credit-kind wait overlapping its predecode portion, allowing
1ms for frontier timestamp quantization. The separate532ms postdecode
portion does not overlap those preceding waits. This is strong contained
local residence, not proof that response bytes were natively available
throughout the entire predecode interval or that all elapsed time is avoidable.

The largest credit window is C481 for[339,353):1041 frames,
1717281us credit wait and1981811us all-kind wait in
[1788879477285434,1788879479273434)us. Its Original write is
S218@1788879473646, decode C479@1788879479273, mux C488@
1788879479721 and local C489@1788879479724. Read begins only8us
before decode. Whole read→delivery age is6080ms, but actual F339
holds only1036ms. Conservatively about581ms all-kind /316ms
credit-specific wait overlaps that actual predecode hold, followed by451ms
postdecode. Charging all1.717s to that one frontier would be incorrect.

### Largest already-read age and largest postdecode winner

| Winning range | Read / winning producer / local write | Decode / route begin / shared begin | Mux / delivery | Read→delivery; decode→delivery |
| --- | --- | --- | --- | --- |
| [381,395), Q Original |S235/S237/S239@1788879474394 |C561@1788879481658 /C585@1788879481818 /C595@1788879481939 |C603@1788879481973 /C604@1788879481980 |7586ms;322ms |
| [885,899), TCP wire1 copy |S691@1788879505989 /S710/S712@1788879506715 |C1542@1788879506978 /C1553@1788879507636 /C1572@1788879507758 |C1575/C1576@1788879507768 |1779ms;790ms |
| [423,437), TCP wire1 copy |S280@1788879475472 /S364@1788879480462 /S368@1788879480463 |C546@1788879481522 /C638@1788879482097 /C693@1788879482263 |C713/C714@1788879482308 |6836ms;786ms |

[381,395)'s actual F hold is only746ms; its7.586s prior residence
overlaps other lower-prefix holds and is not additive delay. Its ordinary
read takes29us at the eventual read call. [885,899)'s actual F hold is
2191ms, of which790ms follows authenticated decode. Its658ms
decode→route-begin delay is inside that hold. The28B local write also
releases already-buffered[899,913). These are current local-service
observations, independent of the preceding candidate's older capture.

## Largest confirmation holds and contrary predecode cases

The largest probe gap6.510292s corresponds to F591, C951@
1788879484552→C962@1788879491062 (6510ms at mux resolution).
The next winning[591,605) is not read until S427@1788879491026:
6474ms occurs before that read. Enqueue S428 is the same millisecond,
claim S429 and positive QUIC write S431 are1788879491027.
Decode C953 is1788879491060; mux C962 is1788879491062,
and local C963 is1788879491063. Read→delivery is37ms and
postdecode3ms. Fine probe timing and rounded event stamps need not equal
to the last millisecond.

Only43 preceding ordinary-reader frames appear before this reply:
41 STREAM_ACK with17us send-await and2 STREAM_MAX_DATA with0us.
Thus this specific6.510s gap is not explained by the typed local wait
seen in the earlier windows. The pre-read interval could include forward
ordered upload service, sink ACK cadence or delayed server read entry;
this response-only observer does not establish which.

F717 holds5250ms, C1218@1788879495711→C1262@
1788879500961. Server read/Original claim/write S559/S561/S563
occur at1788879497320,1609ms after the hold starts.
TCP wire2 copy S579/S582@1788879500931 decodes C1251@
1788879500960 and wins the next millisecond. Original QUIC only
decodes C1303@1788879502617, losing after a5297ms write→decode
age. Its final read call takes1us; this does not identify when native
bytes became available or distinguish the outage from earlier local reads.

The five repair-QUIC read calls need a further availability limit. The
longest read,12.764026s ending C1069@1788879493006 for[605,619),
starts at1788879480242764us, long before that losing copy is written
S491@1788879492976. Winning[325,339)'s8.526562s repair read
likewise begins before its copy is published; its local write→decode is
only38ms. Do not call either full idle-inclusive read duration a native
transport stall. Conversely, ordinary reads at most12549us do not prove
the reader was polling the response earlier or that the network was healthy.

The capture therefore selects two different boundaries: a proven already-
decoded local service hold and typed credit-heavy predecessor residence,
alongside longer gaps originating before reply production or before decoded
ingress. No single aggregate timer or losing transaction is promoted into a
causal explanation for all of them.

## Service and resource context

S/T/Rs/Rc retain their I/O domains: source consumed / ordered target written /
server reply read / client reply delivered. S is not claimed C; Rc is not
automatically mux F. These sampled counters corroborate, not replace, the
exact response event joins.

| Service line / elapsed seconds | Client / server Unix ms | S | T | Rs | Rc |
| --- | --- | ---: | ---: | ---: | ---: |
|L20 /19.003826 |1788879485027 /1788879485026 |247169338 |180060474 |591 |591 |
|L25 /24.004350 |1788879490026 /1788879490026 |247169338 |180060474 |591 |591 |
|L31 /30.004978 |1788879496027 /1788879496025 |314999098 |247890234 |717 |717 |
|L33 /32.005178 |1788879498027 /1788879498026 |321618234 |264351770 |745 |717 |
|L34 /33.005272 |1788879499027 /1788879499026 |358575002 |291466138 |773 |717 |
|L35 /34.005374 |1788879500027 /1788879500025 |358575002 |291466138 |773 |717 |
|L36 /35.005476 |1788879501027 /1788879501025 |358575002 |291466138 |773 |773 |
|L46 /45.007648 |1788879511026 /1788879511025 |388759552 |379520586 |1081 |1081 |

L20–L25 has all four counters constant for five sampled seconds, with
S−T exactly64MiB. This is real forward target-service stalling, corroborating
the pre-reply origin of the largest confirmation gap. In contrast, L31–L35
keeps Rc717 while T advances43,575,904B and Rs717→773: already-produced
reply delay coexists with forward service around the outage/recovery interval.
L46 precedes exact final delivery; it is short9,238,966 target bytes and41
reply bytes. It is not a full-target settled sample.

The46 rows retain stable client/server PIDs312742/318489. Sampled client
RSS peaks511444KiB at L27 (elapsed26.004570s); server RSS peaks140000KiB
at L30 (29.004876s). Maximum observed `ps %CPU` is119% client at L17
(16.003508s) and43.6% server at L9 (8.001113s). These percentages are
process-lifetime averages, not exclusive handler cost or instantaneous CPU.
Observer overhead and differing completed work preclude ordinary cost
acceptance from this diagnostic.

| Physical router HTB class | First→last byte / packet / drop deltas | Maximum sampled class backlog |
| --- | ---: | ---: |
| eth0, return |24933897B /130669 packets /1911 drops |95411B, L7 |
| eth1, upload |519421408B /427328 packets /8180 drops |5103898B, L16 during QoS |

These are class deltas, not sums of parent and child qdiscs. They include
framing, native retransmission, copies and feedback, not repair-only cost.
The unchanged netem limit is8192 packets. Upload class rate is62,500,000B/s
except L16–L25 at1,250,000B/s; return remains62,500,000B/s. Observed epoch
starts are elapsed0,5.000610,10.001344,15.001845,20.003928,25.004447,
30.004978 and35.005476s; UDP blackhole is true at L31–L33.
Per-side generated stamps and non-atomic router sampling must remain separate
when aligning exact events. No queue-total/native-input or packet-loss
attribution is inferred for an individual reply.

## Evidence disposition

The predeclared diagnostic selects current local receive service: a required
Original is already decoded throughout an actual943ms hold, and typed
credit-frame handoffs occupy substantial portions of other winning holds.
It also rejects that mechanism as the explanation of this capture's largest
6.510292s confirmation gap. Prepared-source claim is prompt here; Native
availability before late reads and precise consumer cost remain unmeasured.
No elapsed aggregate is an additive removable-time forecast, no diagnostic
Mbps is an ordinary win, and no runtime correction or performance promotion
is established by this report.
