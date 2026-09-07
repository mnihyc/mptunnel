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
frontier or a QUIC repair. Its window is21,872ms, including11,975ms before the
new Original was published. The whole aggregate could fit before publication;
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
