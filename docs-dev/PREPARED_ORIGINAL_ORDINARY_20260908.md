# Prepared request ordinary comparison — promotion stopped

2026-09-08 13:21 +08:00. Category: actual ordinary evidence and next causal
question. No practical acceptance, public performance update or release.
Read PERFORMANCE_METHOD_AND_LESSONS and CURRENT_CLOSURE_PLAN.

## Identity and unchanged conditions

Control1436ff4: `./.tmp/reflection/bin/request-prefix-20260908/mptunnel`.
Candidate9720e4b: `./.tmp/reflection/bin/prepared-original-20260908/mptunnel`.
Both endpoints use their cell's ordinary optimized binary, no diagnostics.
Candidate build3m29s. Functional533GREEN/audit preceded this pair; later
unreachable-dispatcher/test-consumer cleanup is not in this frozen binary.

Existing `./.tmp/reflection/run.py mixed combined up`, environment:
REFLECTION_ROUTED=1, MIRROR_IMPAIRMENT=1, MANAGEMENT=1 (all REFLECTION_-prefixed);
FIFO=0, RETURN_RATE=500mbit; NO_JITTER/NO_LOSS/NO_QOS/NO_BLACKHOLE/DIAG/
NATIVE_TRACE=0. Endpoint binary overrides unset; REFLECTION_BINARY is the
respective `/workspace/.tmp/reflection/bin/.../mptunnel` container path.
Tags `prepared-original-{control,candidate}-0908`. No profile/harness edits.

Single500Mbps routed/mirrored link; upload70/20ms delay/jitter, return30/5ms.
Five-second loss epochs upload[3,8,5,6,10,3,5,8]%mean6; return[1,2,.5,3,2,.5,1,2]%.
Upload10Mbps15–25s; UDP blackhole30–33s;40s offered load. Existing probe
`--timeout 50` is an allowance AFTER the load deadline, giving90s total; the
runner's85s settlement guard is unchanged. Independent random realizations
are not packet-identical. Builds and labs did not overlap.

All five files per cell are retained under
`./.tmp/reflection/results/mixed-combined-up-prepared-original-{control,candidate}-0908/`
and in the accompanying raw archive, including all139 service samples.

## Completion and contrary phases

| Probe result | Control | Candidate |
| --- | ---: | ---: |
| Complete |Yes |No, settlement guard |
| Confirmed / locally accepted bytes |262668288 / same |106114466 /197984256 |
| Elapsed seconds |52.186854 |85.671962 |
| Exact complete Mbps |40.266 |Unavailable |
| First confirmation seconds |.609051 |.246881 |
| Maximum completed confirmation gap seconds |5.557333 |1.464514, excludes final silence |
| First local write seconds |.261327 |.113258 |
| Maximum completed write gap seconds |7.322724 |1.286327 |

The candidate's9.909Mbps scalar is partial-confirmation lower-bound evidence,
not complete goodput. Its final ConnectionResetError follows runner teardown;
it is not evidence of an independent network reset. Products/probes are stopped.

S is consumed local source, NOT claimed wire horizon C; T is ordered target
socket writes; Rs/Rc are server-read/client-delivered reply bytes. Snapshots do
not expose exact missing Original/response ranges.

| Candidate sample seconds |S |T |Rs |Rc |
| --- | ---: | ---: | ---: | ---: |
|4.000472 |125852802 |69885058 |174 |148 |
|10.001129 |177298562 |142105730 |449 |267 |
|16.001807 |178514210 |178383138 |715 |267 |
|40.084688 |179104034 |178383138 |715 |267 |
|85.090909 |180939042 |178383138 |715 |267 |

Control T at10s is4863999B: early forward progress improves substantially in
the candidate. This does not waive Rc's75.001s sampled hold (client management
Unix1788844346555–1788844421556). During10–16s, T and Rs still advance, proving
that interval is not forward starvation alone. T subsequently holds for69.000s
(server Unix1788844352557–1788844421557). S−T is only131072B at16s, growing to
2555904B at85s; it is NOT a persistent full64MiB undelivered window. S is flat
at179199106B during44–61s, then resumes slowly. The control has substantial
stalls too, but settles; no favourable third run or promotion follows.

## Native receive-service discriminator

Stable client TCP ports42866/42872/42864 have Recv-Q0 at3s; at10s their Recv-Q
is404937/713469/722386B and at85s409736/697306/707497B. Across16–85s, summed
`bytes_received − Recv-Q` advances only111979B. Substantial return traffic has
already reached the client kernel but its consumption is very slow.

Server→client42866 confirms backpressure:

| Sample seconds |Send-Q=notsent bytes |Native ACKed bytes |Cumulative rwnd-limited ms |
| --- | ---: | ---: | ---: |
|16 |280792 |1006616 |9644 |
|40 |282416 |1006616 |33720 |
|61 |284040 |1006616 |54728 |
|85 |264592 |1027912 |78620 |

These bytes mix control/data and do not identify the missing reply DSN. Server
QUIC instance1 also reports25103B native flight at all selected10–85s samples;
that does not locate the response frontier. Client actor source reads continue
in portions of the hold; its pending-local-write-only loop cannot explain the
ENTIRE span because that loop performs no source Read. Do not infer that all
local writes, ACK processing, or other receive service were prompt.

## Cost and full confirmation series

| Sampled cost |Control |Candidate |
| --- | ---: | ---: |
|Upload class bytes |385951645 |251008534 |
|Return class bytes |14600062 |14483460 |
|Upload packets / drops |314311 /4825 |201896 /4047 |
|Return packets / drops |75900 /969 |66140 /877 |
|Peak client/server RSS KiB |596940 /144684 |519200 /113412 |
|Client lifetime CPU% peak/final |60.5 /54.8 |127 /103 |
|Server lifetime CPU% peak/final |25.7 /10.3 |40.7 /5.2 |

Class1:10 deltas: router eth1 upload, eth0 return; not sums of qdisc levels.
Windows0–52.196930s versus0–85.090909s. Class bytes include native retransmits,
framing/control and Product copies, not repair-only traffic. Stable PIDs; CPU
is ps lifetime-average, NOT interval CPU. Unequal work and censoring forbid
efficiency claims. Candidate persistent CPU use is a competing local-service
clue, not proof of a particular actor or notification-loop defect.

Control untrimmed confirmation Mbps, indices0–52:

```text
0–9:   1.049,3.144,3.493,4.992,4.194,5.243,4.194,4.194,4.098,4.290
10–19: 3.263,3.982,5.339,5.788,5.222,4.194,3.050,538.677,0.291,0
20–29: 0.288,0,0,0,0.761,8.677,1.809,148.994,66.969,100.471
30–39: 0,0,177.830,90.370,0,22.877,0,143.131,0,23.209
40–49: 0,0,0,0,0,149.518,0,158.675,73.157,1.573
50–52: 60.057,230.731,33.557
```

Candidate raw series is[] because ACK accounting is invalid on the incomplete
transfer; do not reconstruct bins from unrelated counters. The control's
538.677Mbps bin is buffered confirmation release, not physical >500Mbps service.

## Next bounded question

Locate the actual client receive/decode/handoff wait holding already-arrived
return bytes. Rc=267 is the delivered reply prefix, NOT an observed mux frontier;
a pending local batch can put the mux ahead of Rc. Identify the exact missing
range before attributing it to that backlog.
Competing causes include costly synchronous service, writer/reader interlock,
blocked forwarding, and claim-notification recurrence without useful admission
change. Source review excludes neither all of these nor the exact missing frame
residing elsewhere. No queue shrink, timer/controller change or assumed response
parity fix is justified. Prove a real reachable mechanism before correction;
reuse existing aggregate/input traces only if existing evidence cannot answer it.
