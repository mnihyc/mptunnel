# Exact QUIC response handoff diagnostic

2026-09-08 +08:00. One completed diagnostic, not practical acceptance or a
new queue/congestion-policy decision. Exact winning response intervals show
substantial local input-service time. The largest whole stall still has an
unresolved earlier boundary; do not attribute an aggregate to the wrong window.

Raw: [RESPONSE_QUIC_HANDOFF_20260908.raw.tar.gz](RESPONSE_QUIC_HANDOFF_20260908.raw.tar.gz).
Observer: [RESPONSE_QUIC_HANDOFF_TRACE_20260908.patch](RESPONSE_QUIC_HANDOFF_TRACE_20260908.patch).
Cell: `./.tmp/reflection/results/mixed-combined-up-response-quic-handoff-diag-0908/`.
The nine-file lab-only overlay was frozen and reversed before the run; runtime
remainsf64b904, without the TCP observer, sampler or policy changes. The existing
500Mbps asymmetric changing-loss/QoS/outage profile is unchanged, as documented
in [the ordinary comparison](RESPONSE_RETAINED_ORDINARY_20260908.md).

The probe completes exactly260,571,136B in42.258328s, reporting49.329Mbps,
first confirmation0.490189s and maximum confirmation gap9.433234s. Neither
the diagnostic average nor a comparison to another random capture is acceptance.
Line references below are the cell's original endpoint logs; timestamps are
Unix milliseconds, not the endpoints' independently initialized monotonic time.

## Complete ordinary response coverage

All82 observed ordinary QUIC Data writes belong to session8706632590771399456,
Product stream0, server wirePath0/native instance4/native request stream4.
Client ordinary decode identifies runtimePath0/native instance4, and its actor
route identifies request stream4. Matching numbers here do not establish a
general cross-endpoint identity equivalence. Client attachment is1.

All82 writes have complete decode-to-mux joins:78 advance F, four are redundant.
Four route through the write interlock,78 through the ordinary actor. Server
dispatch-to-write-begin is at most2ms (offset618, server228->229); write begin
to local completion at most1ms. Local H3 completion is not wire/peer delivery.

Independent complete-chain maxima in milliseconds:

| Stage | Maximum |
| --- | ---: |
| Decoded response to reader mailbox completion | 4 |
| Reader mailbox completion to actor routing | 495 |
| Actor route operation | 4 |
| Actor route completion to attachment forwarding | 571 |
| Shared fan-in admission | 4 |
| Shared fan-in residence | 183 |
| Shared dequeue to mux application | 1 |

These maxima need not belong to one range. The largest complete decode-to-mux
chain is1.246s for winning `[782,796)`, client2776/2777/2890/2892/3029/3031/
3070/3071, at Unix1788819202443/2447/2928/2931/3502/3506/3689/3689.

## Tight local-time witness: winning [768,782)

The server dispatches and completes its local write at1788819191764
(server272--274). Client progression is:

| Boundary | Client line | Unix ms |
| --- | ---: | ---: |
| Previous ordinary response [754,768) mailbox completion | 2703 | 1788819200797 |
| F becomes768 | 2749 | 1788819201805 |
| [768,782) decode | 2773 | 1788819202424 |
| Reader mailbox completion | 2774 | 1788819202427 |
| Actor route begin / end | 2883 /2885 | 1788819202910 /1788819202914 |
| Attachment shared-send begin / complete | 3022 /3024 | 1788819203485 /1788819203488 |
| Shared dequeue / mux F768->782 | 3063 /3064 | 1788819203661 |

Between the previous response's reader-mailbox completion and this decode,
474 nondata-frame sends consume1,622,826us of a1,627ms interval; the largest
individual send operation is28,374us. Reader and actor mailbox free-slot
snapshots are0; shared fan-in has131 used slots at admission,129 after dequeue.

The actual F768 plateau is1,856ms. At most1,008ms of the measured preceding
send time can precede that plateau, so at least614.826ms overlaps it. The
separate post-decode interval is1,237ms. Consequently approximately1,851.826ms
of the1,856ms plateau is accounted inside local handoff/service intervals,
allowing millisecond timestamp quantization. This is an interval-overlap bound,
not an estimate made by adding unrelated stage maxima.

The counters measure wall time around completed `send(...).await` operations,
including executor scheduling/service. They are not exclusively time proved
Pending on channel capacity, CPU measurements, or native packet arrival times.
The reader was not performing its next native read inside those send operations;
the earliest time the next response bytes became natively available is unknown.

## Independent winning control [796,810)

Server local write completes at1788819197492 (server312). The prior ordinary
reader-mailbox completion is1788819202447 (client2777); decode is1788819203978
(3153). Its369 preceding nondata sends consume1,518,015us of1,531ms. Decode,
reader mailbox, actor begin/end, shared begin/complete and mux occur at
1788819203978/3981/4319/4320/4515/4517/4564/4564 (client3153/3155/3305/3307/
3432/3434/3466/3467).

Actual F796 holds from1788819203689 to1788819204564:875ms. At least276.015ms
of preceding-send time overlaps that plateau;586ms follows decode. About862ms
of875ms is therefore localized to local service intervals by the same bound.
This is another real winner, not a late repair duplicate.

## Largest stall: aggregate scope prevents stronger attribution

F754 holds9,433ms: client2260 at1788819192372 to2749 at1788819201805. Its
winning Original `[754,768)` dispatches at1788819190895 (server264), with write
begin/end1788819190896 (265/266). Decode is1788819200793 (client2702),9,897ms
after local write. Its later decode-to-mux interval is directly measured1,012ms.

The decoder reports5,757 preceding nondata sends totaling11,707,975us, maximum
18,685us. But that counter begins at the previous **ordinary QUIC response**
`[657,670)` mailbox completion1788819178921 (client1944), not the latest TCP
frontier or a QUIC repair. Its window is21,872ms, including11,974ms before the
new Original was dispatched (11,975ms before local write completion). The
whole aggregate could fit before publication;
it cannot alone prove how much of the9,897ms write-to-decode interval was local
waiting. Do not call the complete9.433s stall solved or attributed from this sum.

## Fast controls and domain limits

Winning `[114,126)` decodes at1788819167248 (client1244), reaches mux at7249
(1251), and advances F. Its92 preceding nondata sends total5us, maximum1us;
reader free slots127/126 and shared queue4/5 contrast with the saturated later
case. Winning interlocked `[657,670)` decodes/routes/forwards at1788819178921
(1943--1948), reaches mux8924 (1951), and has24 preceding sends totaling1us.
The same pipeline can therefore deliver promptly; no universal delay is claimed.

The F670 QUIC winner is outside the ordinary reader observation domain:
Original TCP dispatch server252 at1788819181239, QUIC repair dispatch257 at
1788819182055, winning mux client2032 at1788819182090. Missing ordinary decode
for this repair is expected, not a lost observation or decoder defect. Other
long gaps containing delayed new response production must likewise remain
distinct from delay after that response's publication.

## Next bounded question

The local work source and cost must now be identified: why do successive
nondata handoffs consume milliseconds under this load, and which recipient
work or executor service keeps the pipeline occupied? Inspect existing relay
per-turn preparation, ACK handlers and recovery cost instrumentation before
selecting any new observer. The counters do not identify every nondata frame's
kind and do not authorize ACK coalescing, priority changes, larger queues or a
congestion adjustment. Preserve the fast controls, exact effect ordering,
ownership and ordinary timing gates. No new runtime fix follows this report.

## Follow-up: client cost discriminator, September8

The single frozen/reversed cost-observer capture is
`./.tmp/reflection/results/mixed-combined-up-response-client-cost-diag-0908/`.
It completes exactly450,166,784B in44.532321s, with5.083127s maximum
confirmation gap. Its80.870Mbps is diagnostic context, not an ordinary
comparison or acceptance. Per-second aggregate scopes cover whole actor
preparation and per-kind handlers, with nested ACK/recovery costs; individual
ACK printing and individual performance samples are off.

All86 ordinary QUIC decoded ranges have one matching QUIC mux application and
advance F. The observed identity is session1736103927169766931/stream0,
server Path0/native instance1/request stream4, client runtime0/native instance1/
attachment1. This is this capture's mapping, not a cross-endpoint identity rule.
Maximum winning decode-to-mux time is300ms, smaller than in the preceding
capture but still part of a concrete local-service stall:

| Winning range | Actual F hold, Unix ms | Preceding ordinary reader completion -> decode | Measured preceding nondata send awaits | Decode -> mux | Local interval lower bound |
| --- | --- | --- | --- | --- | --- |
| `[54,65)` |1788820310229 ->311186;957ms |1788820310009 ->310933;924ms |1118 sends;919771us |253ms |699.771+253=952.771ms |
| `[65,76)` |1788820311186 ->312326;1140ms |1788820310934 ->312026;1092ms |1338 sends;1088887us |300ms |836.887+300=1136.887ms |

The abbreviated endpoints retain the preceding timestamp's high digits. The
lower bounds subtract all time before the actual F hold from the preceding-
send aggregate, then add the disjoint postdecode interval. They do not assume
uniformly distributed cost. Millisecond timestamp quantization still applies.
For `[65,76)`, client.log144 is previous mailbox completion;187/188 are
decode/mailbox completion;189/190 actor routing;193/194 shared admission;
197/198 dequeue/mux. Original server write completion33 at1788820310072
precedes the entire hold. The corresponding `[54,65)` records are client108,
143/144,149/150,151/152,153/154. Reader and actor queues are sampled full.

Surrounding aggregate windows show substantial synchronous preparation and
ACK work, not one expensive response-data handler:

| Approximate window end, Unix ms | Whole preparation | Retained-frontier helper, nested | Input ACK | Dispatch |
| --- | --- | --- | --- | --- |
|1788820310773 |557466us |264280us |330054us |61036us |
|1788820311774 |587782us |220209us |263948us |87925us |
|1788820312778 |643220us |195720us |125393us |149764us |

Exact rows are client.log126/129/134/124,170/173/178/168 and302/305/310/300.
These are approximately one-second completion buckets, not per-F CPU samples;
do not allocate them uniformly across a shorter stall. Whole preparation,
Input ACK and Dispatch are distinct outer actor regions; retained-frontier
cost is already inside preparation. Preparation includes possible topology/
disconnection awaits and Dispatch includes cooperative yield. Across the full
capture, retained-frontier totals3.140200s inside7.009682s preparation. That
3.140200s is the entire helper's measured budget, not promised savings from
removing its full-horizon query. This identifies meaningful work to examine
for exact-equivalent reduction, without proving it is the only cause.

The largest5.083s F646 hold is a contrary control, not attributed to this cost:
F646 begins at1788820324854 (client1100), while its new Original is only
dispatched/written at1788820328015 (server281--283). It decodes at329936
(client1200) and reaches mux329937 (1225). Seven preceding nondata sends total
only3us, reader capacity is available, and decode-to-mux takes1ms. Preparation
is small in the overlapping aggregate windows. This case contains3.161s before
publication and a separate1.921s write-to-decode interval; neither is explained
by the early local busy intervals. No whole-tunnel closure or universal speed
benefit follows from reducing the measured retained-owner computation.

## Ordinary follow-up: ranked-prefix query scope, September 8

Raw: [FRONTIER_SCOPE_ORDINARY_20260908.raw.tar.gz](FRONTIER_SCOPE_ORDINARY_20260908.raw.tar.gz),
containing `mixed-combined-up-frontier-scope-{control,candidate}-0908` under
`./.tmp/reflection/results/`. This is one new ordinary comparison, not a repeat
of the response-recovery pair seeking a better number. Both endpoints use
their cell's binary with diagnostics off: frozen response-retained 953a54f
control versus only the equivalent one-query deletion, 445011f, frozen at
`./.tmp/reflection/bin/frontier-scope-20260908/mptunnel`. No observer, sampler,
initial-owner intervention, queue policy, profile change or build overlap.

The 321 affected checks and prefix-restriction oracle establish equivalent
bounded work: the two retained-helper fixtures now each visit 1372 model items
instead of 2744/7514. They do not establish end-to-end timing improvement.
The ordinary pair completes exactly, but maximum confirmation and local-write
gaps worsen. **Promotion stops; no third ordinary run or wider acceptance.**

| Probe result | Control | Candidate |
| --- | ---: | ---: |
| Complete / exact ACK accounting | true / true | true / true |
| Confirmed = locally accepted = final bytes | 214,695,936 | 420,610,048 |
| Complete / failed streams | 1 / 0 | 1 / 0 |
| Whole elapsed time (s) | 47.194034 | 47.163930 |
| Whole confirmed goodput (Mbps) | 36.394 | 71.344 |
| First confirmation (s) | 0.485959 | 0.465351 |
| Maximum confirmation gap (s) | 4.260012 | 4.627694 |
| First local write (s) | 0.149918 | 0.104370 |
| Maximum local-write gap (s) | 1.897546 | 6.914621 |
| Time beyond planned 40 s load boundary (s) | 7.194034 | 7.163930 |

Both probes exit 0 with valid exact `target_sink_ack` accounting and no errors.
The candidate completes substantially more bytes in nearly equal elapsed time,
but that does not waive either adverse gap. Unseeded packet-loss realizations
and unequal work prevent a causal speedup/regression estimate from this pair.
Time beyond 40 s is not exact drain from the last successful source write;
that timestamp and exact maximum-gap endpoints are not retained in the summary.
There is no concurrent interactive/echo-latency or quiet reclamation series.

### Every raw confirmation bin

Indexi is [i,i+1)s on each probe's own clock. These are all48 positive-ACK
observation bins, including recorded zeros, without the producer's three-bin
trimming at each end. Cumulative ACK deltas are assigned when the client probe
observes them, not when the server writes the target or packets cross the
router. Values above 500 Mbps can therefore represent buffered confirmation
release and are not a physical-link speed claim.

| One-second bin | Control Mbps | Candidate Mbps |
| --- | ---: | ---: |
| 0 | 3.144 | 1.689 |
| 1 | 4.486 | 4.719 |
| 2 | 10.193 | 53.885 |
| 3 | 7.436 | 516.379 |
| 4 | 6.816 | 30.453 |
| 5 | 5.671 | 22.020 |
| 6 | 5.243 | 3.379 |
| 7 | 4.719 | 6.795 |
| 8 | 3.146 | 3.457 |
| 9 | 3.242 | 2.717 |
| 10 | 3.050 | 5.243 |
| 11 | 2.097 | 3.574 |
| 12 | 2.193 | 3.242 |
| 13 | 3.050 | 1.573 |
| 14 | 3.146 | 553.412 |
| 15 | 2.193 | 6.912 |
| 16 | 3.574 | 25.786 |
| 17 | 2.621 | 1.049 |
| 18 | 3.766 | 0.000 |
| 19 | 3.574 | 0.000 |
| 20 | 3.670 | 0.716 |
| 21 | 4.194 | 0.000 |
| 22 | 10.486 | 30.313 |
| 23 | 527.389 | 6.816 |
| 24 | 0.665 | 8.197 |
| 25 | 4.332 | 75.261 |
| 26 | 23.315 | 179.210 |
| 27 | 0.000 | 106.667 |
| 28 | 0.000 | 184.645 |
| 29 | 79.928 | 190.272 |
| 30 | 41.707 | 179.447 |
| 31 | 0.000 | 0.000 |
| 32 | 0.000 | 16.990 |
| 33 | 0.000 | 0.000 |
| 34 | 118.297 | 0.000 |
| 35 | 0.000 | 0.000 |
| 36 | 0.000 | 0.000 |
| 37 | 0.000 | 51.884 |
| 38 | 104.429 | 290.027 |
| 39 | 35.268 | 42.327 |
| 40 | 0.000 | 59.341 |
| 41 | 42.423 | 48.567 |
| 42 | 21.400 | 78.879 |
| 43 | 176.161 | 79.123 |
| 44 | 0.000 | 171.774 |
| 45 | 0.000 | 266.907 |
| 46 | 287.834 | 8.056 |
| 47 | 152.710 | 43.177 |

### Forward versus return stage

S/T/Rs/Rc retain the provenance defined in
[the previous ordinary report](RESPONSE_RETAINED_ORDINARY_20260908.md):
client local-source reads / server ordered target-socket writes / server
target-reply reads / client local reply writes. They are independent observed
I/O counters, not exact native transmission or probe confirmation bins.
Sample lines below are original `service.jsonl` lines; runner elapsed precedes
sequential collection, and each endpoint's cached Unix timestamp is separate.

| Cell | Line | Elapsed (s) | S bytes | T bytes | Rs bytes | Rc bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| control | 4 | 3.000323 | 69,467,895 | 2,359,031 | 125 | 125 |
| control | 23 | 22.002310 | 78,511,863 | 11,402,999 | 1011 | 1011 |
| control | 24 | 23.002412 | 145,710,263 | 78,601,399 | 1037 | 1037 |
| control | 31 | 30.003123 | 162,463,479 | 137,595,799 | 1184 | 1115 |
| control | 36 | 35.112150 | 180,390,807 | 160,390,327 | 1212 | 1142 |
| control | 39 | 38.112455 | 190,447,351 | 183,184,855 | 1282 | 1142 |
| control | 47 | 46.113369 | 214,695,936 | 213,998,775 | 1562 | 1212 |
| control | 48 | 47.113480 | 214,695,936 | 214,695,936 | 1603 | 1352 |
| candidate | 4 | 3.000368 | 76,939,211 | 9,830,347 | 101 | 101 |
| candidate | 5 | 4.000466 | 139,198,411 | 74,710,987 | 152 | 152 |
| candidate | 15 | 14.001601 | 149,994,315 | 83,082,059 | 620 | 620 |
| candidate | 16 | 15.001721 | 218,783,115 | 152,419,147 | 688 | 688 |
| candidate | 19 | 18.002021 | 222,894,347 | 155,785,483 | 758 | 758 |
| candidate | 21 | 20.002226 | 222,894,347 | 155,785,483 | 758 | 758 |
| candidate | 31 | 30.003336 | 339,298,283 | 272,189,419 | 1052 | 1052 |
| candidate | 32 | 31.127860 | 345,196,523 | 278,087,659 | 1066 | 1066 |
| candidate | 34 | 33.128065 | 345,211,123 | 278,102,259 | 1080 | 1080 |
| candidate | 38 | 37.128495 | 345,211,123 | 278,102,259 | 1080 | 1080 |
| candidate | 39 | 38.128593 | 355,902,891 | 288,794,027 | 1108 | 1108 |
| candidate | 41 | 40.128815 | 396,869,355 | 335,193,515 | 1206 | 1178 |
| candidate | 45 | 44.129229 | 420,610,048 | 409,225,195 | 1416 | 1276 |
| candidate | 47 | 46.129480 | 420,610,048 | 414,688,683 | 1500 | 1500 |
| candidate | 48 | 47.129549 | 420,610,048 | 415,308,043 | 1556 | 1556 |

Early service differs materially: the candidate's first large target burst
is sampled at 3–4 s; the control's at 22–23 s. In those early slow intervals
Rs and Rc mostly track each other while S consumes far ahead of T. This is
not evidence that every gap is held return work, or proof that the scope
optimization alone caused the earlier forward release.

The candidate's adverse long episode is a forward-service plateau:

- Lines 34–38, 33.128065–37.128495 s, keep S=345,211,123 and
  T=278,102,259 exactly flat, separated by 64 MiB. Rs=Rc=1080 throughout.
  Client Unix times are 1788821426835–1788821430836; server
  1788821426836–1788821430836. No new target-reply input is held in the
  measured server-Rs/client-Rc interval here.
- Over the longer 31.127860–37.128495 s interval, S and T each advance only
  14,600 B, at the 33.128065 s sample. The raw series' sole four-bin zero run,
  bins 33–36, places its maximum confirmation silence in this episode.
  The 6.914621 s local-write maximum is consistent with the extended source
  stall, but its exact endpoints cannot be recovered from source-read samples.
- The UDP block is first sampled at 30.003336 s and cleared at 33.128065 s;
  the plateau persists for another approximately 4 s. Target progress is
  visible again at 38.128593 s. Temporal association does not by itself
  attribute the delay to a native controller or a particular repair.

The control has a different late geometry. Its lines 36–39,
35.112150–38.112455 s (both endpoint Unix 1788821368441–1788821371441),
keep Rc=1142 while Rs grows 1212→1282 and T grows 160,390,327→183,184,855 B.
Thus its bins 35–37 confirmation silence contains held return work despite
22,794,528 B of forward target progress. Its earlier outage-associated T
plateau is 30.003123–33.111929 s at 137,595,799 B. These mechanisms must not
be combined into one average or declared identical across cells.

After 40 s the candidate also has some return lag, but not the same long flat
Rc episode: at 44.129229 s, T=409,225,195 and Rs/Rc=1416/1276; by 46.129480 s,
Rs=Rc=1500 while T is 414,688,683. Its last snapshot leaves 5,302,005 target
bytes unsettled; final probe equality records their later completion.
The control instead has all payload target-written in its last snapshot but
Rc/Rs=1352/1603. Neither last sample is the complete terminal state.

### Native context for the candidate's worst pause

During lines 34–38, the stable client-reported path counters are:

| Path / native instance | Native epoch | Native ACKed bytes, start→end | Product OriginalData debt | Reported queue bytes, start→end |
| --- | --- | ---: | ---: | ---: |
| TCP1 / 4 | 11500194338610892311 | 32,746,051→34,211,379 | 0 | 3,957,419→2,582,171 |
| TCP2 / 2 | 2776913170470870378 | 13,479,878→13,479,878 | 0 | 0→0 |
| TCP0 / 3 | 7311020705210105742 | 14,731,484→17,810,200 | 0 | 5,637,753→2,684,959 |
| QUIC0 / 1 | 378159093625881351 | 271,021,151→272,224,751 | 67,108,864 | unavailable |

QUIC's ACK counter actually stays 271,021,151 from 31.127860 through 36.128384 s,
then advances 1,203,600 B by 37.128495 s while T is still flat. TCP native ACK
progress continues, so this is not a total carrier-set service freeze.
The retained OriginalData debt is on QUIC, while TCP queues still contain
megabytes despite zero TCP OriginalData debt. Those queues can contain already
settled work, repairs or control/framing; the ordinary files cannot identify
their exact composition or which range blocks ordered target delivery.
Product debt, native ACKed bytes, native queue and S−T are different byte domains.

All path identities/native epochs are stable from line 2 onward. The control's
first snapshot has an unresolved second TCP-instance placeholder; the next
snapshot resolves TCP2/native4. Both then retain three TCP paths plus QUIC.
No setup trace establishes exact first-owner readiness or ordering. There are
no management errors; client logs are empty and each server has only its
post-sampling H3_NO_ERROR teardown warning.

### Sampled costs and limits

Router class 1:10 deltas use eth1 upload and eth0 return, without summing nested
qdiscs. Counters are monotone. The 48-sample intervals are control
0.000052–47.113480 s and candidate 0.000053–47.129549 s; neither spans all
startup/terminal wire work. Values are a whole-class wire-work proxy, not
repair-only overhead.

| Sampled cost | Control | Candidate |
| --- | ---: | ---: |
| Upload class bytes, first→last | 634,471→280,999,438 | 658,669→572,916,055 |
| Upload byte delta | 280,364,967 | 572,257,386 |
| Upload packet / drop delta | 234,035 / 2,707 | 463,134 / 7,915 |
| Return class bytes, first→last | 31,898→9,042,929 | 34,240→22,047,799 |
| Return byte delta | 9,011,031 | 22,013,559 |
| Return packet / drop delta | 52,333 / 547 | 124,930 / 1,660 |
| Client PID | 252799 | 253929 |
| Client RSS first / peak / last (KiB) | 51,472 / 1,159,376 / 349,140 | 54,892 / 464,976 / 406,508 |
| Client peak-RSS elapsed (s) | 46.113369 | 45.129364 |
| Client maximum observed / last ps %CPU | 60.3 / 58.6 | 70.7 / 41.9 |
| Server PID | 258927 | 260050 |
| Server RSS first / peak / last (KiB) | 30,740 / 134,740 / 134,740 | 30,632 / 131,468 / 131,468 |
| Server peak-RSS elapsed (s) | 47.113480 | 47.129549 |
| Server maximum observed / last ps %CPU | 29.6 / 9.2 | 33.8 / 16.6 |

RSS is sampled resident KiB, not a continuous peak or quiet post-load baseline.
`ps pcpu` is a process-lifetime average, not interval CPU, CPU seconds or
proof of which helper consumed time. The smaller candidate client RSS peak
and lower last CPU observation are useful context, not normalized efficiency
claims: it completes nearly twice the payload, spends different time in each
state, and has worse gaps. Wire counters mix framing, control, native retries
and Product copies; exact repair count/cost is not observed.

The single useful next causal question is the forward plateau after UDP
reopening: which exact byte range prevents T advancing beyond 278,102,259,
and is it waiting for native admission/receipt, Product ordering or target
socket service? The snapshots rule out reusing the prior server-Rs/client-Rc
held-return explanation here, but do not identify that forward range or its
recovery service. Do not infer a controller/queue fix, or rerun ordinary cells seeking
a favourable average. The demonstrated computation reduction remains separate
from unsatisfied practical timing and global acceptance.
