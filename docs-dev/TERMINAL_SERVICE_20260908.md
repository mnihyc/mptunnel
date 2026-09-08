# Upload terminal-service diagnostic

Recorded:2026-09-08. Category: completed bounded causal capture on b783cd6.
**The prior post-full-target terminal pause is not reproduced. No terminal
policy correction or performance promotion follows.** The capture instead
retains a long incomplete-target plateau and already-produced reply delays.

## Scope and artifacts

The [preceding ordinary pair](PREPARED_STALE_SERVICE_20260908.md) completed
326041600B but held confirmation for6.082958s at the tail, with full target
acceptance and flat server/client reply counters. The exact question was where
source EOF, FIN publication/native service, server FIN readiness, target
half-close, final reply production and response delivery spent that interval.
Full target-socket acceptance did not itself establish sink consumption or EOF.

One warning-free3m31s optimized diagnostic build used runtime b783cd6 plus
the independently reviewed11-file
[TERMINAL_SERVICE_TRACE overlay](TERMINAL_SERVICE_TRACE_20260908.patch).
It was frozen as
`./.tmp/reflection/bin/terminal-service-20260908/mptunnel`; every hook was
reversed and the source diff verified empty before the lab. Both endpoints
used that frozen binary. Enabled events were terminal_service,
terminal_fin_replay, client_stream_fin_received and client_relay_result.
No compilation overlapped the run, no policy or harness changed, and no repeat
was selected to improve the result.

The [unchanged profile](ADVISORY_OWNER_SERVICE_20260908.md#fixed-ordinary-profile-and-order)
is routed/mirrored500Mbps; upload70/20ms and return30/5ms delay/jitter;
five-second upload loss epochs[3,8,5,6,10,3,5,8]% and return
[1,2,.5,3,2,.5,1,2]%; upload10Mbps at15–25s; UDP outage30–33s;
40s offered load,85s runner guard and90s total probe boundary.

Full raw directory:
`./.tmp/reflection/results/mixed-combined-up-terminal-service-0908/`.
The five result files, build/run logs and observer patch are preserved in
[TERMINAL_SERVICE_20260908.raw.tar.gz](TERMINAL_SERVICE_20260908.raw.tar.gz).
The archive contains eight files and passes compressed-integrity checking.
There are57 service samples and100/103 client/server log lines, including a
final server close warning;203 lines are not203 terminal publications.

Write completion is local native/protected acceptance, not packet delivery or
peer ACK. Reply-read timestamps are server relay reads, not sink emission times.
The hooks add no per-Original claim trace, payload decode trace, native packet
trace or response publication-to-decode chain. Absence of those stages must
remain explicit rather than be reconstructed from aggregate counters.

## Complete probe and full confirmation history

| Probe field | Result |
| --- | ---: |
| Local accepted = target-confirmed bytes |424017920 |
| Elapsed seconds / exact goodput Mbps |56.939769 /59.574 |
| First confirmation / maximum closed confirmation gap seconds |.437606 /7.996908 |
| First local write / maximum local-write gap seconds |.095848 /4.374934 |
| Elapsed after40s offered-load boundary |16.939769s |
| Complete / failed streams; probe and runner exits |1 /0;0 /0 |
| Runner elapsed seconds |57.284792 |
| Accounting / errors |Exact valid target-sink ACK, not lower bound; none |

All57 raw one-second confirmation bins follow in Mbps. Labels are zero-based
starting indices and the final bin is partial. These are confirmation-arrival
rates, not physical capacity. The diagnostic rate is not an ordinary-build
comparison or acceptance result; the long zero spans remain part of the outcome.

```text
 0: 0.737, 8.698, 16.777, 145.708, 445.785, 128.355, 149.518, 167.964, 112.914, 4.525
10: 4.624, 285.213, 1.882, 14.584, 193.966, 0, 2.621, 0, 0, 336.185
20: 0, 0, 0, 0, 120.062, 71.827, 26.386, 85.651, 0, 182.452
30: 33.362, 0, 0, 0, 58.933, 2.717, 0, 0, 0, 0
40: 0, 0, 0, 187.911, 0, 0, 0, 0, 86.271, 0
50: 0, 77.07, 0, 151.016, 33.03, 5.767, 249.631
```

## Exact same-flow terminal handoff

C/S denote one-based client/server log lines. Short event times below are
Unix milliseconds minus1788861500000; process-relative clocks have different
anchors and are not compared. Stream0 belongs to session16886573837468305640.
C1 binds client QUIC runtime0/physical1 to ordinary H3 request4; publication
adds attachment3. Server decode carries the same session, wire PathId0 and
connection-local H3 request4, with its separately owned physical1 identity.
Bare physical or H3 integers are not globally unique path identities.

| Boundary | Exact event evidence |
| --- | --- |
| Local source EOF |C77@52966; ordinary source read returns EOF |
| First pending FIN state |C78@52966: C391950079, queue_bytes=data_bytes=32067841, ready=false, retry_pending=false |
| Request FIN publication |C85/C87@61875: final_offset424017920, QUIC runtime0/physical1/attachment3; C88 records terminal replay |
| Actual local FIN writes |C86/C89 and C90/C91@61875: begin/complete twice on ordinary H3 request4 |
| Authenticated server FIN decode |S87/S88@63133: final_offset424017920 |
| Server actor receipt |S89/S90@63134: receive_frontier419210455, ready=false |
| Final data makes FIN ready |S92@63384: final_offset424017920, source=data |
| Target write-half-close |S93/S94@63384: shutdown begin/complete, result=ok |
| Last reply reads and target EOF |S95@63384 reads14B; S96@63385 reads13B; S97@63385 reads0B |
| Response FIN writes |S98 replay, S99–S102 begin/complete twice@63385, response final_offset1208 |
| Client terminal delivery |C98@63421 delivers27B through response F1208; C99 receives ready FIN1208 |
| Logical completion |C100@63422: ok=true, both directions closed, no pending FIN, sender_queue/reinjection bytes0 |

At source EOF, C+U=391950079+32067841=424017920. The remaining32.07MB
are real prepared Data, not an invented final offset or a blocked empty FIN
queue. EOF-to-publication is8.909s, but this observation does not show that FIN
was eligible throughout that interval. Waiting for U to be claimed preserves
the existing source/terminal ownership requirement. There is no continuous C
or claim-refusal trace to attribute that whole interval to a particular gate.

Both successful FIN writes precede decode by1.258s. This bounds a post-local-
acceptance/predecode interval; it does not separate native ordered debt,
network service or reader scheduling. Actor receipt follows decode by1ms.
At receipt,4807465 payload bytes remain below the final offset, so ready=false
is correct. Data makes FIN ready250ms later, and target shutdown begins and
completes in that same millisecond. Same-ms timestamps are not zero CPU cost.

The server reads the final13B and EOF1ms after shutdown; client delivery of
the combined27B follows36–37ms later, and the logical relay completes another
1ms later. These are the actual observed reads, not content inferred solely
from the13-byte length. The sink's final `OK` is conditional on natural EOF
([tcp_sink.py](../lab/tcp_sink.py)); this capture shows prompt service at the
instrumented terminal boundaries once the last data makes FIN ready.
It therefore does not reproduce the prior six-second post-full-target tail.

## Larger nonterminal holds retained by this capture

Server reply reads are89 positive reads plus one EOF; client delivery has88
positive events because the final14B+13B coalesce. Both sequences cover
exactly[0,1208), without holes or overlap. Server response_claimed_end and
queued-byte observations agree with cumulative read ranges, and all queued
values at those reads are zero. The successful client result counts
424019128B=424017920 request+1208 reply bytes. This is reply-range/accounting
consistency, not an Original/copy/native completeness trace.

The largest client reply-delivery gap is exactly bounded by C75@42294, ending
at F999, and C76@50291, delivering[999,1013):7.997s at millisecond resolution,
consistent with the probe's7.996908s gap. That exact next reply is read by the
server at S75@43136. Thus842ms of the current-frontier gap precede the read,
and7155ms follow it. This is a genuine already-produced-reply service delay,
not eight seconds of missing forward target payload or delayed sink production.

Other long read-to-delivery intervals show why an individual byte's lifetime
must not be confused with its current-frontier gap:

| Exact response bytes | Server relay read | Client successful delivery | Read→delivery |
| --- | --- | --- | ---: |
|[999,1013) |S75@43136 |C76@50291 |7.155s |
|[1013,1027) |S76@43336 |C79@55020 |11.684s |
|[1027,1041) |S77@44489 |C80@57560 |13.071s |
|[1041,1055) |S78@44756 |C81@60442 |15.686s |

The last interval is the largest individual produced-reply delay, not a15.686s
single current-F stall. This observer does not locate its response publication,
native writer, client decode, mailbox or actor substage. In service samples
L37–L44 contained within the F999 hold, aggregate client TCP Recv-Q grows
244407→651927B while bytes_received grows778905B, implying371385B of socket
consumption. These are all incoming protocol bytes, not those exact14 reply
bytes; continued consumption does not identify the winning response carrier.

The largest sampled **incomplete-target** plateau is also much longer than
the terminal handoff. S is client-consumed source, T server target-socket
acceptance, Rs server-read replies and Rc client-delivered replies:

| Service boundary | Collector time | S / T bytes | Rs / Rc bytes |
| --- | ---: | --- | --- |
|L40 |39.282820s |402483839 /391950079 |1055 /999 |
|L48 |47.283681s |424017920 /391950079 |1055 /1013 |
|L55 |54.284427s |424017920 /391950079 |1055 /1055 |
|L56 |55.284540s |424017920 /392632039 |1097 /1097 |
|L57, final sample |56.284637s |424017920 /414438327 |1153 /1139 |

T391950079 and Rs1055 are constant from server Unix1788861545465 to
1788861560464, a14.999s sampled lower bound. Target completion occurs after
the final sample, at the exact terminal/data chain above. The source EOF
snapshot proves C391950079/U32067841 at one actual instant inside the plateau;
S alone is not C, and no missing continuous claim history is fabricated.
Another sampled T324656607 plateau spans server1788861536467→1788861540465,
3.998s around the configured UDP outage. QoS T254002335 is flat for2.001s.
These phases are retained without assigning an exact prefix owner or native
cause that the terminal-only hooks cannot observe.

## Sampled cost and timing limits

| Observation | Client | Server |
| --- | ---: | ---: |
| Peak RSS KiB |593188 |151552 |
| Final sampled RSS KiB |561104 |141260 |
| Peak observed `ps %CPU` |84.9 |32.7 |
| Final observed `ps %CPU` |76.2 |15.5 |

Final service sample is at collector56.284637s, before exact completion.
Directional router class deltas are upload598629081B (867989→599497070)
and return24246693B (39756→24286449). Each uses one HTB class, not parent+
child double counting; neither necessarily includes every final packet. They
include framing, control, copies and retransmission, not repair-only wire cost.
RSS is sampled resident memory; `ps %CPU` is a process-lifetime average at each
observation, not interval utilization or actor/claim CPU cost. Diagnostic cost,
different useful work and independent random realization prevent a throughput
or normalized-cost comparison with the preceding ordinary cell.

Collector elapsed, probe-relative time and each endpoint's Unix timestamps
have different anchors; some management values span a collector delay. Plateau
bounds use actual endpoint timestamps. They do not attribute sampling delay as
the cause of application gaps. Final H3_NO_ERROR close warning follows the
successful relay/terminal sequence and is not evidence of an earlier failure.

## Disposition

Terminal non-reproduction: no basis for a terminal-policy fix. Source EOF still
owned unclaimed Data; FIN receipt correctly waited for missing data; shutdown
and final response service were prompt once ready. The1.258s FIN write→decode
interval remains unattributed. The strongest retained service failure is the
earlier exact produced-reply delay and concurrent sampled target plateau,
with their missing intermediate stages explicitly bounded. No new run,
controller tuning, speculative correction or performance acceptance follows.
