# Mixed response placement and receive service

2026-09-08. Runtime `d999fea`; diagnosis and prospective model controls only.
**MPP is not performance-accepted. No runtime correction or seconds-saving
claim follows from this report.**

## Scope, artifacts and byte domains

One frozen `receive-owner-service-20260908` diagnostic binary reused the
[reply-stage observer and three owner timers](RECEIVE_OWNER_SERVICE_TRACE_20260908.patch)
for the unchanged combined mixed upload and download. No build overlapped a
capture. The harsh shared-cut/loss/QoS/outage profile is a diagnostic stress
case, not the final performance environment or a100Mbps objective. These are
not ordinary comparator/candidate performance results; the ordinary twelve-cell
[redesign baseline](REDESIGN_BASELINE_20260908.md) remains separate.

Raw directories are
`./.tmp/reflection/results/mixed-combined-up-receive-owner-service-0908/` and
`./.tmp/reflection/results/mixed-combined-down-receive-owner-service-0908/`.
[Raw archive](MIXED_RESPONSE_PLACEMENT_SERVICE_20260908.raw.tar.gz) contains their
ten result files and six build/run/test logs, rooted at `./.tmp/reflection/`.
The [structural RED patch](MIXED_SERVICE_STRUCTURAL_RED_20260908.patch) preserves
the proposed ownership controls separately. Both observer and test patches
were reversed; intentional REDs are not left in the runtime/CI source.

C/S references below are one-based physical client/server log lines, **not
the diagnostic seq field**. Times are Unix milliseconds. Throughout the DOWN
section, abbreviated times add the common base1788871800000; UP uses full
Unix timestamps. Product DSN ranges are half-open payload intervals. Native
write success means local protected/TCP or H3 acceptance, not transmission or
peer ACK. QUIC encodes smaller records; use range coverage, not equality
between a65,536B publication and its12,000B decoded pieces. Native H3 IDs are
connection-local. Wire path IDs, runtime indexes and physical instances have
distinct namespaces. Mux F is the client Product receive frontier, not the
server target socket or the probe's consumed HTTP-body count.

## Download: full outcome, including failed interactive service

The completed40-second bulk-interactive observation has `status=loss`, runner
exit0. Bulk HTTP200 transfers357,680,274 body bytes in40.018022620s
(71.503838Mbps); first body0.630275336s, largest read gap3.589461376s between
20.593686052 and24.183147428s. There is one deliberately duration-limited,
partial request of the8GiB body, zero completed full bodies. `bulk_status=ok`
does not mean the whole object completed. The forty raw one-second Mbps bins,
including all zero and burst intervals, are:

```text
0:  0.466,9.563,25.690,13.631,11.534,8.389,7.340,4.194,367.002,3.670
10: 4.194,5.243,582.385,148.137,162.670,91.750,0,30.601,0,0
20: 11.771,0,0,0,14.540,58.676,196.416,199.512,228.286,83.339
30: 8.389,4.194,5.767,4.475,3.958,5.082,11.792,8.151,488.734,51.807
```

Persistent64B echo, scheduled at500ms with the unchanged3s timeout, has74
actual attempts:29 success, one timed-out I/O failure (index29), then44
`unavailable_after_disconnect` outcomes. Attempt29 spans15.190330437 to
18.193475066s; the probe disconnects then. Request/response payload is
1,920/1,856B. Successful-only p50/p95/max latency is239.283843/788.362692/
833.831718ms and maximum successful-response gap1.094672639s; these figures
must not hide the subsequent45 failures. All74 attempt timestamps remain in
probe.json. Success latencies for indexes0–28, in order (ms), are:

```text
141.089665,239.283843,833.831718,534.098783,436.550072,469.172223,
292.425187,301.659485,290.696394,121.193898,274.342320,312.019563,
526.835507,117.375521,373.000060,788.362692,143.393150,250.508474,
112.192688,96.955928,115.790200,138.221165,60.541282,269.587778,
126.899442,83.723543,103.936774,122.584004,112.563901
```

DOWN client/server PID is296892/302769, session16375315291330820283. Bulk is
stream1, echo stream0. Ordinary QUIC uses wire0, client/server physical4,
client runtime0/attachment2, H3 stream4 for bulk and12 for echo; repair uses
H3 stream8. TCP wire0/1/2 maps to server physical1/2/3 and client
runtime2/0/1, physical1/3/2, bulk attachment1/0/3 respectively.

Final logs contain335,090 client and28,819 server lines. There are6,844
gap-free bulk Original publications covering[0,421680148); final bulk mux F
is357782018, C335074 at t41898. These are different stages from the probe's
357680274 HTTP-body bytes; this duration-limited cancellation is not a final
all-source ACK/consumption settlement test.

### Longest individual holds do not select pre-native placement

| Stream / F / full hold | Original publication → positive local write | First winning decode → mux/local delivery | Meaning |
| --- | --- | --- | --- |
|Bulk F186028922,3.589s; C181986@22467→C183286@26056 |QUIC[186028922,186094458), S10640@14479→S10726/S10736@14485 |C182794@26041→C183286/C183290@26056 |Only6ms before native acceptance;3.574s of the hold precedes decode,15ms follows it. |
|Bulk F184557594,3.097s; C180017@19333→C180932@22430 |Covering QUIC Original[184521594,184587130), S10570@14472→S10670/S10684@14482/14483 |C180442@22411→C180932/C180938@22430 |11ms publication→write;19ms decode→local. |
|Echo F1856,5.594s; C160187@16676→C180438@22270 |QUIC[1856,1920), S16935@17095→S16936/S16937@17096 |C180430@22270→C180438/C180441@22270 |No multi-second pre-writer or postdecode wait. Late delivery follows the probe's timeout; it is not a successful echo attempt. |

For F186028922, the TCP repair S16949@25843 stages/writes/flushes
S16954–16956@25845 but decodes C268646@31477, after the QUIC winner. For the
echo, TCP repair S16943@17363 flushes S16946@17364 and decodes C268644@31474;
its route has no remaining attachment. Neither losing-copy residence is the
winning user's delay. The above predecode intervals alone do not distinguish
native ordered-byte withholding from all earlier reader/task service.

All74,634 completed DOWN owner records give24,878 records per cut:

| Cut | Maximum acquisition / whole section (ms) | Acquisition / section line |
| --- | ---: | --- |
|Batch bound |0.498 /2.072 |C90590 /C276565 |
|Receive feedback |0.041 /6.615 |C13435 /C77004 |
|Write-poll setup |0.041 /3.140 |C10356 /C39976 |

Section time includes acquisition, work and descheduling; it is not exclusive
CPU and is not additive with acquisition. These three cuts do not account for
an individual multi-second DOWN hold.

### Material sequence: unstarted TCP prefix beside real QUIC suffix service

At **t29040**, all128 TCP Originals in the exact contiguous interval
`[283493924,291208666)`—**7,714,742B**—are published but not yet writer-started.
The real Original publications run S19336–S19511 at t28890–29020. Matching
every publication to its exact TCP wire/range command proves that the earliest
write begin is S20671@29710, and the latest is t38867. No count is inferred
from queue-size gauges or Native bytes-in-flight.

| TCP wire path | Original records | Exact bytes |
| --- | ---: | ---: |
|0 |8 |524,288 |
|2 |86 |5,197,686 |
|1 |34 |1,992,768 |
|Total |128 |7,714,742 |

An equal-sized later QUIC Original is an actual, not hypothetical, alternative
service opportunity: `[291220666,291286202)` publishes S19769@29038,21ms
after TCP Original[290898522,290964058) at S19505@29017. Its write begins
S19786@29039 and succeeds S19798@29040. Six decoded pieces
C263026/C263028/C263120/C263122/C263150/C263247 cover all65,536B at
t31271–31275; mux pieces C263921–C263946 arrive at t31305–31306 while F
remains283493924. This proves real equal-sized Original authority and native
service on QUIC; it does not prove any extra spare physical capacity.

More broadly,978 later QUIC Originals cover the gap-free suffix
`[291208666,348329286)` =57,120,620B and have positive writes between
S19797@29040 and S23460@31268. These are Original payload bytes, not copy
bytes or wire bytes. At the first such success, the entire128-frame lower
TCP interval was still unstarted. Thus a same-volume earlier-prefix placement
question exists without inventing a rate or enlarging native admission.

| Actual ordered event | Client evidence |
| --- | --- |
|Reach F283493924 with no reordering |C263911@31304 |
|Later QUIC65,536B is present but cannot advance F |C263921–C263946@31305–31306 |
|Peak buffered reordered suffix60,835,978B, F289236058 |C327726@39113 |
|Last5,536B of the TCP-assigned interval arrives |C329173@40790, incoming[291203130,291208666) |
|That arrival advances F291203130→351061540 |Same C329173; reordered bytes fall to200,048 |

It takes **9.486s** to traverse the7,714,742B lower interval after F first
reaches its start. This is a slow sequence with continuing prefix progress,
not one9.486-second no-progress gap. It exposes a material ordered dependency
on private earlier assignments while tens of megabytes of later data arrive.

The slowest representative TCP Original[290898522,290964058) waits **9.850s**
from S19505@29017 to command-stage/write/flush S24782–S24784@38867. It loses:
QUIC repair publication S25045@40202 writes S25049/S25052@40202 and wins
C328751@40764; the TCP Original decodes C334310@41328. That9.850s is not an
observed user gap or a promised gain.

A separate instantaneous check joins each of14,859 bulk frontier advances
to the Original covering its previous F. The overlap of its individual hold
with that Original's publication→write-begin interval is at most **1ms** at
the logged millisecond resolution
(F58400 and F124803698). Most private-queue residence occurs before that
particular range becomes F. This rejects attributing the longest hold to a
currently unstarted head; it does **not** erase the earlier placement decision
that created the slow multi-range sequence. One isolated64KiB reassignment
cannot clear millions of other lower TCP-owned bytes.

## Upload: material receive residence, owner-cut hypothesis rejected

The same frozen observer completes exactly397,344,768 locally accepted and
target-confirmed bytes in59.830669s (53.129Mbps). First confirmation/write is
0.512636/0.129914s; maximum completed confirmation/write gap6.403523/5.075253s;
no probe errors. This duration-upload workload has no separate persistent
echo series. All sixty raw one-second confirmation Mbps bins are:

```text
0:  1.145,7.765,11.126,6.291,542.755,48.759,218.060,73.541,56.099,4.194
10: 31.982,64.487,15.204,6.291,281.402,0,0.665,0,1.336,0
20: 0,0,0,264.146,82.217,9.297,28.548,39.846,91.226,55.959
30: 72.448,0,131.596,48.663,105.906,0,168.296,0,0.096,0
40: 0,0,0,0,0.096,0,0,0,0,0
50: 145.272,11.726,0,0,0,242.982,110.957,9.201,23.593,165.586
```

These are buffered confirmation bins, not wire-rate measurements. Client/server
PID is295499/301387, session10071478086351525457, stream0. QUIC wire0 uses
ordinary H3 stream4, client physical1/runtime0/attachment3; TCP wire0/1/2 maps
to client physical2/4/3. These identities must not be carried over from DOWN.

All915 completed owner records (305 per cut) bound acquisition/whole-section
times at13.040/13.042ms batch-bound,13.393/13.405ms receive-feedback, and
0.486/0.489ms write-poll setup. Section elapsed includes acquisition,
descheduling and guarded work; do not add nested measurements. Early exits
can censor records. These cuts cannot explain an individual multi-second
hold in this capture.

The largest actual reply-frontier hold is F1032: C1967@1788871616432 to
C1983@1788871622835, about6.403s. Winning QUIC Original[1032,1046) is read
S913@1788871611825, published S915 in the same millisecond, and locally written
S916–S917@1788871611825–1788871611826. Client decode C1971@1788871621894
precedes route C1973–C1974@1788871622251–1788871622253, shared send
C1977–C1978@1788871622688, dequeue C1981@1788871622835, mux C1983 and
delivery C1986@1788871622835. Thus5.462s of the actual hold precedes decode
and0.941s follows it. The three owner acquisitions here are0/301/0us.
Whole Original write→decode age10.068s is not the actual6.403s frontier hold.

C1971 reports1,786 preceding non-data frames with6,504,789us cumulative
reader `send().await` elapsed, maximum individual284,551us, since previous
QUIC response mailbox completion C1944@1788871615382. Accounting for that
earlier window start, at least approximately5.453s of the sequential waits
overlaps the selected frontier's predecode hold. This is local reader-handoff
wait overlap, not exclusive CPU or proof that native response bytes were
already available throughout. The capture records no predecessor types or
handler scopes: ACK/credit/requalification and native proof/control are
possible; the consumer also services outgoing commands. It does not identify
ACK handling as dominant or promise that all overlapping seconds are removable.

Another already-decoded useful prefix is TCP client physical4[962,976):
decode C1731@1788871607590, route C1746@1788871608422, shared send
C1752–C1753@1788871608556–1788871608565, dequeue C1764/mux C1766 at
1788871608701, delivery C1769@1788871608702. Its1.112s postdecode residence
is within the actual F962 hold of2.053s. Adjacent[976,990)/[990,1004) have
1.114/1.116s postdecode residence but actual frontier holds only3/2ms; these
overlapping waits are not additive critical delays. Across101 winners,
mux→successful local write is at most13ms.

Contrary QoS case F710 lasts5.291s: winning[710,724) writes
S565@1788871591691, decodes C1239@1788871595742 and is delivered
C1268@1788871595743. Postdecode residence is1ms; ten preceding non-data frames
total only4us send-await. Not every long hold is local receive service.

UP response accounting closes:119 gap-free Originals cover[0,1619), with189
copies; all308 positive local writes total4,226B. There are305 client decoded
transactions/4,185B and101 useful mux advances/deliveries totaling1,619B.
The three unobserved decoded transactions are losing QUIC Originals
[1578,1592),[1592,1606),[1606,1619), locally accepted S1515@1788871631061,
S1525@1788871631345 and S1536@1788871631348. TCP copies deliver their41B at
C3225@1788871631391, C3247@1788871631561 and C3258@1788871631678. These are
losing/censored transaction limits, not missing unique data.

## Model proof and conditional benefit disposition

The actual protected-writer structural pair first proves B's current Original
admission, write and peer receipt using default-derived capacity and legal
Backup-A/Regular-B preference. Only then does the prospective late-placement
assertion fail: never-consumed A already owns131,072B, expected0. The opposite
actually writes/receives A first and retains its ownership correctly. Existing
test enrollment stands in for OPEN; protected writer/peer service is actual.
This is a **prospective ownership-contract RED**, not a current RFC violation
or measured time improvement. The independent receive-owner pair also reaches
its intended dependency RED, but current owner timings reject promoting that
structural dependency as the multi-second cause in these captures.

The conditional late-native Original-claim benefit is to use an already
admitting writer for the lowest still-unbound prefix instead of permanently
placing future payload in a different writer's private queue. DOWN supplies
actual earlier equal-sized QUIC service and a material9.486s ordered sequence.
It does not establish a fixed numerical gain: all intervening ranges,
W/P/E/source conservation, exact chosen Native/attachment fences, ordinary
quantum/batching and feedback changes must compose. No promised9.486s saving,
no extra QUIC capacity, no static protocol preference and no revival of the
old hard BDP/ETA window, whole-ready-set invalidation or metadata wake churn.
Already-started native work remains irrevocable; native loss/ordered backlog
and shared bottlenecks are not removed by late placement.

UP useful receive residence remains material and unresolved, while its three
owner-acquisition cuts are rejected as the multi-second explanation. Preserve
that independent boundary while proving DOWN placement. Neither mechanism's
model/control result proves that the other is fixed. The next attributable
candidate must be compared against its forecast with ordinary timing and
healthy/high-BDP mixed service, not promoted from these diagnostic Mbps.
