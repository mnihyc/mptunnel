# Forward service after ACK-support reduction — 2026-09-08

**Disposition: exact boundary evidence, not performance acceptance.** This
diagnostic completes436797440B in48.135091s with6.095822s maximum
confirmation gap. Its largest actual mux hold is4.723145s and resolves
through a TCP copy; two large QoS holds resolve through QUIC repairs.
The three winners reach mux within0–14ms of authenticated decode. A
different2.169175s hold spends about2.076s before the missing Original is
claimed. Predecode, unavailable assignment and shared-cut queuing remain
distinct; no Native-controller defect or new correction follows.

## Capture, clocks and artifacts

The [ordinary ACK-support pair](ACK_SUPPORT_SERVICE_20260908.md) remains
mixed/adverse. Its sampled forward holds selected the question: which exact
Original/copy owns the blocking byte, and is it unclaimed, unwritten,
predecode or decoded but awaiting ordered service? This capture uses unchanged
runtime d999fea plus the independently reviewed11-file
[POST_ACK_FORWARD_TRACE overlay](POST_ACK_FORWARD_TRACE_20260908.patch).
The warning-free optimized build took3m32s and was frozen as
`./.tmp/reflection/bin/post-ack-forward-20260908/mptunnel`.
Every hook was reversed before the one unchanged-profile run; no build/lab
overlap or general reinjection-attempt flood was added.

Raw directory:
`./.tmp/reflection/results/mixed-combined-up-post-ack-forward-0908/`.
[POST_ACK_FORWARD_SERVICE_20260908.raw.tar.gz](POST_ACK_FORWARD_SERVICE_20260908.raw.tar.gz)
contains exactly seven files: the five result files and
`post-ack-forward-0908-build.log` / `post-ack-forward-0908-run.log`.
The observer patch is separately linked, not an eighth archive member.
Archive integrity, member list and byte-for-byte source comparisons pass.
There are51497 client /43024 server log lines and49 service samples.

The [fixed profile](ADVISORY_OWNER_SERVICE_20260908.md#fixed-ordinary-profile-and-order)
remains routed/mirrored500Mbps; upload70/20ms and return30/5ms delay/jitter;
five-second upload losses[3,8,5,6,10,3,5,8]% and return
[1,2,.5,3,2,.5,1,2]%; upload10Mbps at15–25s, UDP outage30–33s,
40s offered load,85s runner guard and90s total probe boundary.
Different random realizations are not packet-identical controls.

C/S denote one-based client/server log lines. Exact-join table times are
Unix milliseconds minus1788865000000. Client PID282179 and server PID288171
have separate monotonic origins and must not be subtracted across processes.
Logical stream0 uses exact session string17777903812046591390, not an
inexact floating-point session identifier.

| Underlay | Client runtime / physical / attachment | Wire PathId / server physical | Native request stream |
| --- | --- | --- | --- |
| QUIC |0 /1 /2 |0 /1 |ordinary4; repair8 |
| TCP |0 /2 /0 |1 /2 |protected TCP writer |
| TCP |1 /4 /1 |2 /4 |protected TCP writer |
| TCP |2 /3 /3 |0 /3 |protected TCP writer |

These memberships stay active in all49 management snapshots. Matching
numbers are not a global namespace: exact commit/transaction wire mapping
and the session establish these joins. QUIC writer hooks lack runtime
physical identity; this sole observed QUIC connection and its ordinary4/
repair8 streams bound their interpretation. Repair ordinals are local to
one receive half, not global request or packet identities.

Product commit is accepted ownership, not a native write. Positive QUIC
write completion and TCP flush completion are local acceptance, not packet
transmission or peer receipt. `request_reader` is authenticated complete
MPP decode before the existing handoff. Server F is mux release, not target
socket-write completion. A stall record identifies incoming frame geometry
and actual F before/after, not an invented ingress path. For the selected
winners below the earlier decoded covering candidates are unambiguous;
this must not be generalized to every duplicated frame.

## Exact completion and all confirmation bins

| Probe field | Result |
| --- | ---: |
| Local accepted = target-confirmed bytes |436797440 |
| Complete / failed streams |1 /0 |
| Elapsed seconds / exact Mbps |48.135091 /72.595 |
| First confirmation / maximum closed gap seconds |.401135 /6.095822 |
| First local write / maximum gap seconds |.102595 /5.560319 |
| Elapsed beyond40s offered-load boundary |8.135091s |
| Runner elapsed seconds / exit |49.205940 /0 |
| Probe exit / errors / stderr |0 /none /empty |
| Accounting |Exact valid target-sink ACK; not lower bound |

Neither probe nor runner is censored by the observation guard. The final
sample is nevertheless not a post-settlement measurement. This diagnostic
does not improve or replace the ordinary disposition.

All49 raw one-second confirmation bins follow in Mbps, with zero-based
starting indices and a partial last bin. Buffered598.212/503.360Mbps
confirmation arrival does not establish physical service above500Mbps.
The trimmed70.244Mbps mean is not substituted for full history.

```text
 0: 3.669, 5.654, 8.913, 8.913, 598.212, 60.914, 32.41, 68.157, 19.354, 503.36
10: 9.962, 137.984, 38.273, 9.865, 24.117, 1.145, 0, 0, 0, 0
20: 0, 141.089, 0, 0, 0, 2.237, 264.163, 276.282, 27.699, 8.293
30: 0, 0, 230.608, 0, 16.393, 1.049, 0, 132.788, 0, 0
40: 183.436, 43.708, 177.035, 0.953, 2.097, 0, 160.671, 218.564, 76.413
```

## Full ownership, write, decode and ACK reconciliation

All13590 Original commits form precisely[0,436797440), without holes or
overlapping Original ownership. Their byte totals are394832962 QUIC and
41964478 TCP. An additional1508 copy commits represent34288004 copied
bytes, not unique throughput. All15098 transactions have one exact write
begin and positive local completion in the correct order, with no duplicate
transaction key or unmatched commit. There are no logged write-error
completions or begin-only transactions in this capture.

| Channel | Successful local transactions / bytes | Decoded Data records / bytes | Coverage result |
| --- | ---: | ---: | --- |
| QUIC ordinary4 |12769 /394832962 |36200 /394832962 |Exact byte-order sweep |
| QUIC repair8 |523 /13093548 |1329 /13093548 |Exact byte-order sweep |
| TCP all three instances |1806 /63158934 |1271 /58149974 |Every Original decoded;535 losing copies remain undecoded |

The QUIC comparison preserves splitting/coalescing and copy multiplicity
by matching byte order, not equal record counts. TCP joins use exact
physical instance and interval, with corresponding wire-path mapping.
All821 TCP Originals decode. The535 missing TCP copy decodes total5008960B;
all have successful local flush records and final positive Product ACK
covers their byte ranges. They can remain native work after useful logical
completion. Their missing decode does not establish corruption, cancellation,
a lifetime leak, or extra useful payload.

There are6202 applied **client request ACK** rows releasing436797440B.
Every row has nondecreasing F, with F/largest_end<=claimed_end. Another80
server-side response ACK rows are a different direction and must not be
added to this request accounting. Final C51496@126300 has F=C436797440 and
zero retained bytes; C51497@126304 records logical success, sender queue/
retained bytes zero, reply frontier1209 and total payload436798649
=request436797440+reply1209.

Sparse ACK fields do not expose the entire dictionary. `first_range` and
cumulative F prove their covered bytes; the unlogged remainder is unknown,
not negative ACK evidence. No missing dictionary or confirmation bin is
reconstructed from management counters.

## Four distinct actual held-frontier chains

The101 `server_receive_delivery_stall` records are ranked by their measured
delivery gap, not by duplicate copy age. The exact duration is in the server
clock; subtracting it from millisecond Unix timestamps gives only an
approximately aligned start. None of these mux durations is the probe's
maximum confirmation gap.

| Held F / duration | Original commit → local completion | Winning publication / local completion | Winning authenticated decode → mux |
| --- | --- | --- | --- |
|311994142 /4.723145s, largest; outage/recovery |Q[311946142,312011678), C20145@104798 →C20169@104799 |TCP runtime1/physical4 copy[311994142,312011678), C32969@112449 →C32974@112450 |S31245@112696 →S31247@112697;1ms |
|208218020 /4.427207s, QoS |TCP runtime1/physical4[208218020,208283556), C12879@87372 →C12881@87373 |Q repair[208218020,208232620), C17538/C17540@93652 |S22758@98052 →S22765@98052; same ms |
|209266596 /3.976569s, QoS/release boundary |TCP runtime1/physical4[209266596,209332132), C12921@87377 →C15755@89883 |Q repair[209266596,209332132), C17828/C17840@95027 |S23000@103367 →S23331@103381;14ms |
|379448222 /2.169175s, later source/claim gap |No Original until Q[379448222,379460222), C35432/C35434@120551 |Original wins; no covering copy |S37552/S37553@120644; same ms |

For the largest F311994142 hold, the Q Original is one65536B assignment
whose first48000B precede the missing suffix. The winning17536B TCP
stale-path copy takes246ms local-flush→decode and1ms decode→mux.
An earlier14600B TCP runtime2/physical3 copy C32805–07@111453 only
decodes S36674@117889, after the winner; its6.436s predecode age does not
determine useful recovery. The Q Original's first covering decoded chunk
is S31392@113276,8477ms after its local write and579ms after mux already
advanced. Client C33178@112725 then positively acknowledges through312077214.
Thus neither a missing covering repair nor a multi-second winning mux delay
explains this exact episode.

For F208218020, the winning Q copy takes4400ms local acceptance→decode;
only its first12000B advance the selected mux event. Its remaining2600B
decode S22762 in the same millisecond. Original TCP only decodes
S22766@99404,1352ms after the first Q release; that Original then releases
a later hole plus buffered suffix at S22768, with F208232620→208676772.
The additional TCP copies locally written at94097/94382 decode still later
at111003/109738 and do not win. Client ACK C18174@99426 positively covers
the range; server hindsight alone does not prove the
sender knew the earlier Q delivery at its exact time.

For F209266596, the TCP Original itself spends2506ms between write-begin
C12922@87377 and flush C15755@89883, then13544ms until decode
S23332@103427. The winning Q copy is already locally accepted about4377ms
before this F becomes current; its8340ms local-write→decode age is not
the3.976569s current-F hold. Q decode precedes TCP Original decode by60ms,
and C18253@103420 positively covers the range. The14ms
decode→mux interval is local elapsed service, not proof of one handler's CPU.

### Source/claim counterexample, not another all-Native hold

For F379448222, the previous Original ending there is C33211@113209.
No new Original follows until C35432@120551. The hold becomes current
approximately118475, so about2076ms of its2169.175ms duration precedes
the missing assignment; local acceptance→decode/mux then takes93ms.

Client service L42 at Unix1788865119176 already has source S429265412;
L43 at1788865120175 has full436797440. Last preclaim ACK C35431@120549
still reports claimed_end379448222. Thus real consumed-source bytes await
claim during this hold: S is not merely inferred from a socket queue.
C35429–31 show continuing ACK progress while C remains fixed. These facts
select a claim-service boundary, not its refusing predicate, writer readiness,
P/E capacity or an unjustified recovery timer. No7-second Native delay can be
inferred from the longer interval since C33211.

## Repair reader: long read residence, short route residence

All1346 read ordinals are coherent:1345 complete read/route pairs and one
final begin-only ordinal1346 at S37546@120361. Completed kinds are1329
STREAM_DATA and16 STREAM_REQUALIFY_DATA. Every completed pair preserves
ordinal/kind/range and read-before-route order. Total measured route await
is46367us; maximum3871us at S26540/ordinal969 is the only route above1ms.
The final outstanding read is not an invented long completion or payload wait.

| Read ordinal / returned range | Read begin → complete | Local acceptance of containing copy | Relation to selected winner |
| --- | --- | --- | --- |
|371 /[191365438,191377438) |S22717@93434 →S22741@98051;4617561us |C17030@93510, after read starts |Precedes winning F208218020 ordinal375 |
|377 /[208232620,208244620) |S22764@98052 →S22955@103365;5313498us, largest completed read |C17833@95027, before read starts |Precedes winning F209266596 ordinal388 |
|1279 /[345587940,345599940) |S31033@108269 →S31280@113248;4978657us |C22915@107313, before read starts |Suffix work; not the winning F311994142 copy |

Winner ordinal375 has only16us read /23us route await; its4.400s
acceptance→decode delay is almost entirely while earlier ordinal371's
read is pending. Of ordinal371's own measured read, about76ms precedes
local copy completion. Winner ordinal388 has12us read /18us route await,
after the5.313498s ordinal377 read. These distinguish prior ordered-reader
residence from a long per-frame registry route. They do not measure packet
availability, timer choice or exclusive CPU.

Ordinal377's returned bytes later duplicate bytes already delivered by
the Original TCP at S22766@99404 and positively covered by C18174@99426.
That is receiver/sender knowledge acquired after the copy's local acceptance
C17833@95027, not evidence that its earlier publication was knowingly useless.
Product ACK release also does not retract a previously accepted native copy.

The QoS interval has a genuine shared-cut competing explanation: sampled
upload router HTB class backlog falls6151007→5032179→3851599→2662659→1285679B
around16–20s under the configured10Mbps class. That queue is not a measured
per-range location or proof of sole causality, but it prevents labeling the
4.4s predecode delay a Native defect solely from its duration. Routing,
native ordered availability and task scheduling still have separate limits.

## Interpretation and stop

For these three largest winners, rapid decode→mux service excludes that
local boundary as the multi-second owner; positive local writes exclude
missing publication. Repair route awaits do not reproduce multi-second
registry blocking. Neither observation proves transport/controller error,
and the late unclaimed-Original case explicitly defeats treating every
delivery-gap record as a native service delay.

This is one diagnostic realization, not a rerun that establishes practical
improvement over the adverse ordinary pair. All useful bytes complete, but
multi-second confirmation/write gaps remain. No controller tuning, policy
change, new threshold, ideality claim or performance promotion follows.
Sampled S/T/reply phases and resource costs below are contextual boundaries,
not substitutes for the exact range evidence above.

## Selected service phases and cost boundaries

S is client locally consumed upload (`reliable.io.to_peer_bytes`); T is
server target-socket-written upload (`from_peer_bytes`). Rs is reply read
from the target on the server (`to_peer_bytes`); Rc is reply delivered to
the client's local socket (`from_peer_bytes`). S is not claimed C, T is not
mux F, and Rc is not an exact receiver-mux frontier. All are byte counters.
L denotes the one-based service row; times below retain each side's own
Unix milliseconds minus 1788865000000, not a falsely simultaneous runner clock.

| Service row / phase | Client / server timestamp | S | T | Rs / Rc |
| --- | --- | ---: | ---: | ---: |
| L2, early | 79175 / 79175 | 67764224 | 655360 | 32 / 32 |
| L11, about 10s | 88175 / 88176 | 230883646 | 163774782 | 482 / 482 |
| L17→20, QoS plateau | 94176→97175 / 94176→97176 | 275326884 | 208218020 | 776 / 776 |
| L23→26, second plateau | 100176→103175 / 100175→103175 | 276375460 | 209266596 | 804 / 804 |
| L31→35, outage/recovery | 108175→112176 / 108176→112175 | 355311268→379103006 | 311994142 | 902 / 888→902 |
| L37→38, return lag | 114175→115175 / 114175→115175 | 384470830→390458606 | 328925838→357211812 | 958→986 / 944 |
| L39→41, return lag | 116176→118176 / 116175→118176 | 397786638→420805732 | 357318884→370426084 | 1000→1014 / 972 |
| L42→43, later source/claim hold | 119176→120175 / 119176→120176 | 429265412→436797440 | 379448222 | 1042 / 986→1014 |
| L46, late forward frontier | 123176 / 123175 | 436797440 | 379841438 | 1084 / 1084 |
| L49, last sample | 126175 / 126176 | 436797440 | 433066500 | 1182 / 1182 |

The two QoS T/Rs plateaus each span 3.000s of server snapshots and have
exactly 64MiB S−T. The outage/recovery T/Rs plateau spans 3.999s while source
consumption continues. L34 is deliberately not treated as simultaneous:
its client timestamp 112176 is 1.001s later than server 111175 and repeats in
L35. The later Rc plateaus coexist with increasing T/Rs, so not every zero
confirmation bin means absent forward target service. Conversely the QoS
T plateaus are real target-write holds, not solely delayed reply delivery.

| Sampled resource or directional class delta | Client / upload | Server / return |
| --- | ---: | ---: |
| Peak / last process RSS, KiB | 520968 / 331768 | 156016 / 156016 |
| Maximum / last reported ps %CPU | 73.2 / 57.2 | 41.5 / 20.1 |
| Router HTB class bytes, last minus first | 594941215 | 21754944 |
| Router HTB class packets / drops, last minus first | 482084 / 7499 | 106509 / 1339 |
| Peak aggregate TCP Recv-Q / Send-Q, bytes | 331185 / 10094996 | 0 / 184772 |

Upload is router eth1; return is eth0. These class counters include protocol,
feedback, copies and native retransmission, not repair-only overhead. They
stop at the last sample and are not exact whole-transfer amplification.
Do not add parent HTB and child netem counters for the same traffic. The
upload netem backlog peaks at 11605808B in L2; return peaks at 63470B in L6.
Separately sampled class/child values can differ: at L20 upload class backlog
is 2662659B and netem backlog 2664173B. TCP Recv-Q is kernel socket occupancy,
not exact Product reply debt; zero server Recv-Q does not exclude a QUIC
ordered-stream gap. No TCP-to-QUIC queue equivalence is implied.

ps %CPU is a process-lifetime average, not interval-exclusive CPU or handler
cost. No aggregate CPU-scope observer is enabled here. Client RSS peaks in
L46–47; server peaks in L49. T is still 3730940B short of final in L49, so
the last RSS values are not post-load settled ownership or reclamation.

## Selected next boundary: late ordinary QUIC read service

The restored-capacity Original [379841438,379853438) is distinct from the
earlier unclaimed-head case. Product C35519@120566 assigns its 12000B to
QUIC runtime 0 / physical 1 / attachment 2. Ordinary H3 stream 4 write begins
C35567@120572 and completes C35615@120575. The exact authenticated decode
is S37652@124171 on wire 0 / server physical 1 / H3 ordinary 4; mux S37773
follows at 124175, advancing precisely that range with reorder bytes zero.

Thus local completion→decode is 3.596s and decode→mux is 4ms. The actual
current-F hold is 1.270544s, not the whole 3.600s local-write→mux residence.
The earlier local acceptance rules out missing publication for this range,
but does not say whether ordered bytes were available inside Native.
Unlike the QoS repair episode, nearby L41–43 upload class backlog is only
about 92–101KB under restored 500Mbps. This contextual contrast is not proof
of an empty native queue or a QUIC defect. The ledger's next sparse observer
distinguishes late ordered-byte availability from late local polling/return;
this capture alone cannot choose between them. No runtime correction or
performance promotion follows from this selected diagnostic boundary.
