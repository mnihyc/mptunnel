# Native ordered-read service — 2026-09-08

**Disposition: late processed input is observed; a Native defect is not proved.**
The diagnostic completes 391184384 exact bytes in 49.043824s, with maximum
confirmation/write gaps of 3.594738/6.955278s. A winning QUIC repair spends
3.021829s before its missing native head byte is validated/stored, then
72us before the first ordered Chunk returns. The largest actual mux hold
instead resolves through TCP. Physical loss/queuing, service tradeoffs and
implementation defects remain distinct classifications; this is neither
performance acceptance nor evidence that all preceding placement was optimal.

## Capture and exact identity

Runtime is unchanged d999fea plus the temporary 19-file
[NATIVE_READ_SERVICE_TRACE overlay](NATIVE_READ_SERVICE_TRACE_20260908.patch).
The warning-free optimized diagnostic build took 3m57s and was frozen at
`./.tmp/reflection/bin/native-read-service-20260908/mptunnel`. Every source
and Cargo observer change was reversed before the one unchanged-profile run;
no build/lab overlap or runtime policy correction was added.

Raw directory:
`./.tmp/reflection/results/mixed-combined-up-native-read-service-0908/`.
The [raw archive](NATIVE_READ_SERVICE_20260908.raw.tar.gz) contains exactly
seven files: five result files plus native-read-service-0908 build/run logs.
Its 1227628 compressed bytes preserve 24912498 uncompressed bytes; gzip,
ordered member list and byte-for-byte comparison of all members pass.
The observer patch is separately linked. There are 27920 client log lines,
40052 server lines, and 50 service samples.

The [fixed profile](ADVISORY_OWNER_SERVICE_20260908.md#fixed-ordinary-profile-and-order)
is unchanged: mirrored single 500Mbps, upload 70/20ms and return 30/5ms
delay/jitter, five-second changing loss, upload 10Mbps at 15–25s, UDP
blackhole at 30–33s, 40s offered load, 85s runner guard and 90s total probe
boundary. This random realization is not a packet-identical ordinary control.

C/S refer to one-based raw log lines, **not** their seq values. Table times
are Unix milliseconds minus **1788866600000**. Client PID 283330 and server
PID 289312 have separate Product clock origins. Native seq/native_mono_us
are another observer domain; only their own differences measure native
episodes. Unix milliseconds align producer boundaries with millisecond
precision, not exact microsecond cross-process timing.

| Carrier | Client runtime / physical / attachment | Wire PathId / server physical |
| --- | --- | --- |
| QUIC | 0 / 4 / 2 | 0 / 4 |
| TCP | 0 / 1 / 0 | 1 / 1 |
| TCP | 1 / 3 / 3 | 2 / 3 |
| TCP | 2 / 2 / 1 | 0 / 2 |

Session is the exact string **1134306674588161274**, logical stream 0.
Server Product-domain binds S8@19746 and S17@19867 associate ordinary H3
stream 4 and repair stream 8 with native connection **23097797370624**.
This is a process/lifetime-scoped identity, not a cross-endpoint number.
QUIC writer rows still lack this connection ID; the single observed
connection plus exact Product range/membership joins bounds that association.

Native offsets include H3 framing and are **not Product DSNs**. A pending
episode closes on its first nonempty native Chunk, not necessarily a whole
MPP frame. Already-ready reads are unlogged; absence of an episode proves
neither healthy service nor network-only delay.

## Exact probe and full confirmation history

| Measurement | Result |
| --- | ---: |
| Local accepted = target-confirmed bytes | 391184384 |
| Complete / complete streams / failed streams | true / 1 / 0 |
| Probe elapsed seconds / exact Mbps | 49.043824 / 63.810 |
| First confirmation / maximum closed gap, seconds | .445767 / 3.594738 |
| First local write / maximum write gap, seconds | .140827 / 6.955278 |
| Elapsed beyond 40s offered-load boundary | 9.043824s |
| Runner elapsed seconds / exit | 50.160383 / 0 |
| Probe exit / errors | 0 / none |
| ACK accounting | exact target-sink ACK, valid, not a lower bound |

Neither completion is guard-censored. All 50 raw one-second confirmation
bins follow in Mbps; indices are zero-based and the last bin is partial.
Values above 500Mbps reflect buffered confirmation arrival, not physical
link capacity. The trimmed 58.399Mbps mean does not replace this history.

```text
 0: 1.669, 5.552, 7.051, 12.583, 552.483, 78.643, 32.078, 17.206, 529.531, 28
10: 11.963, 11.106, 15.633, 1.69, 5.126, 0, 13.727, 0, 0, 0.524
20: 0, 0.952, 0, 17.922, 0, 519.473, 4.719, 0, 0, 0
30: 0.234, 0.234, 16.79, 0.35, 0.794, 0, 71.251, 0, 461.993, 0
40: 14.061, 4.719, 15.204, 73.009, 2.621, 6.816, 48.138, 18.306, 395.454, 131.872
```

## Complete Product accounting

All 6359 Original commits form precisely [0,391184384), gap-free and without
overlapping Original ownership. They comprise 5743 QUIC Originals carrying
355932216B and 616 TCP Originals carrying 35252168B. Another 834 copies
carry 35474055B, not unique throughput. All 7193 commits have exactly one
matching write-begin and positive local completion, in order; transaction
keys are unique and no write-error completion is logged.

| Native channel | Successful writes / payload bytes | Decoded Data records / payload bytes |
| --- | ---: | ---: |
| QUIC ordinary H3=4 | 5743 / 355932216 | 32640 / 355932216 |
| QUIC repair H3=8 | 322 / 11115007 | 1097 / 11115007 |
| Three TCP instances | 1128 / 59611216 | 1092 / 58181424 |

Both QUIC channels reconcile by exact byte-order sweeps, preserving split/
coalesced frames and repeated copy multiplicity. TCP joins use the exact
physical instance and interval. **36 TCP Originals**, totaling 1429792B,
have no corresponding decode before capture end (23 on physical 2, 13 on
physical 3); these are not mislabeled losing copies. Every one was locally
flushed, and all-decoder interval union covers the complete logical payload.
Other-path copies therefore provide their useful coverage. Finite missing
loser decodes do not establish corruption, cancellation or a lifetime leak.

There are 6340 applied client request ACK rows, releasing exactly 391184384B.
The additional 91 server response ACK rows belong to the reverse direction.
Final C27917@68572 has F=C=391184384 and zero retained cache. C27918@68573
reports logical success, zero sender queue/cache, response frontier 1401,
and payload total 391185785=request391184384+reply1401. A losing TCP copy
still flushes at C27919–20@68574, illustrating why native work and logical
completion have different lifetimes.

Product commit is accepted ownership, not packet transmission. Native
write/flush completion is local acceptance, not peer receipt. Authenticated
decode precedes Product routing/mux; mux F is not completed target writing.
Sparse ACK fields expose cumulative F and the first range, not the whole
dictionary; no hidden positive ranges or negative omissions are invented.

## Largest actual mux holds and their winners

119 mux delivery-stall records are ranked by actual current-frontier hold,
not age of a losing duplicate. Their durations use the server monotonic
clock; inferred starts from Unix milliseconds are only approximately aligned.

| Held F / measured duration | Original local completion | Actual winning decode → mux |
| --- | --- | --- |
| 233453673 / 3.448691s, largest | TCP physical1, C15943@40886, Original [233453673,233519209) | TCP physical2 copy [233453673,233468273), S29892/S29893@49660 |
| 165558377 / 3.023067s, QoS | TCP physical3, C10847@27953, Original [165558377,165623913) | QUIC repair first12k [165558377,165570377), S21789@39228 → S21837@39229 |
| 302778089 / 1.903557s, later | TCP physical1, C20661@54062, Original [302778089,302843625) | That Original S31164/S31165@59813, releasing through302909161 |
| 232858313 / 1.870568s, QoS boundary | QUIC Original [232798313,232863849), C15848@40881 | Its last5536B, S22998/S23000@44575 |
| 167917673 / 1.847312s | TCP physical3, C10963@27960 | QUIC repair S22637@42690 → S22924@42698 |
| 232994921 / 1.633122s | TCP physical1, C15847@40881 | QUIC repair S24356@46203 → S24653@46212 |

For the largest hold, the winning TCP physical2 copy is published
C20264@49073 and flushed C20266 in the same millisecond: 587ms before
decode/mux. Earlier copy physical3 writes C20054@48701 but decodes
S29960@51871, after the winner. QUIC repair writes C20229@48901 but first
decodes S30157@57711, also losing. Original TCP arrives S30019@54311.
Optimizing the oldest losing residence is not evidence of useful recovery.

The third-ranked hold is an Original-wins counterexample: TCP physical1
takes 5751ms local completion→decode, but its current-F hold is only
1.903557s. TCP copies locally flushed at C21920@59089 and C21993@59387
decode later at S32960@65334 and S31671@62987. No native-QUIC episode can
attribute this TCP winner's preceding delay.

### Winning repair: late processed input, prompt return

The F165558377 Q copy is accepted at C15465/C15474@35323. Repair read
ordinal230 starts S21685@36206, immediately after the prior repair ends
at this F. Native repair episode159 starts S21686 at native offset1718267.
Its first validated ingestion covering that offset is S21786@39228:
**3021829us after Pending**. The first Chunk returns S21787 after **72us**,
covering native [1718267,1719416), not the full12k Product frame.

Read completion S21788 and authenticated decode S21789 follow at 39228;
the exact Product range is [165558377,165570377). Route await S21790 is
35us and mux S21837 follows at 39229. Thus this winning read's multi-second
part precedes processed head availability, not postavailability Chunk
service or its registry route. The full local-write→decode interval is
3905ms; it is not identical to the 3023067us current-F hold.

This is processed receipt after validation/storage, not NIC/socket arrival,
and it cannot identify packet loss, sender scheduling, native retransmission
or the physical shared cut as the sole cause. Nor does a late head stamp
prove all bytes needed for the entire Product frame were absent beforehand.

### Winning ordinary suffix: the same boundary, different geometry

The 65536B Q Original [232798313,232863849) commits and writes at 40881.
Its first 60000B decode S22444–48@42554. Native ordinary episode 240 then
waits at offset 209984260 from S22449@42554 to validated ingestion
S22996@44574: **2019941us**. Chunk return S22997 follows after **426us**,
covering only native [209984260,209984853). The remaining Product 5536B
decode S22998 and advance mux S23000 at 44575. That gives 3694ms total
local-write→decode versus only 1.870568s of current-F hold.

The tight same-reader bracket identifies the missing-input boundary needed
before this suffix decoded, without equating its native offset to a DSN.
It does not assign every millisecond between write and full-frame decode
to one native read or a particular congestion-control action.

## All native episodes, including contrary and censored cases

1665 native-read event rows comprise two Product-domain binds and 1663
native rows. Native sequence 0..1662 is gap-free and unique. There are 556
Pending episodes: 553 complete Pending→head_available→chunk_return chains,
plus two begin-only control streams (H3=2/r29 at S1 and H3=0/r342 at S2)
and repair episode211 S36225@67284→reinit S40016@68650, which is censored.

The 553 completed episodes divide into 343 ordinary and 210 repair.
Every chain retains its exact offset/start/availability anchors; ingestion
covers r and the nonempty returned Chunk starts at r. Native elapsed versus
mono differences agree within 1us of flooring. Process/lifetime/connection/
H3/episode keys prevent ordinal or timestamp-domain conflation.

| Closed-episode processed-availability→first-Chunk elapsed | Microseconds |
| --- | ---: |
| Median / p95 / p99 | 88 / 440 / 2205 |
| Maximum, repair8 episode46 | 5457 |
| Maximum ordinary4 episode120 | 3361 |
| Count above1ms / above10ms | 13 / 0 |
| Sum across553 episodes | 93824 |

The maximum repair example is S10429@25535 Pending→S10865@25656 available
(121743us), then S10885@25662 return (5457us). The ordinary maximum is
S13729@26917→S13731@26983 (66303us), then S13732@26987 (3361us).
These sums span concurrent streams, not exclusive CPU. They sample only
actual Pending episodes; unlogged already-buffered reads, parser service,
mailbox waits and paused/canceled futures are outside the asserted bound.

The longest Pending→head episode is ordinary4/277: S29901@49772,
r274336949, to S30344@59339 after 9566956us, then S30345 return after 85us.
The next Product Original [300577137,300591737) is not published/written
until C20283–85@50267, 495ms after the episode starts. The preceding
Product 2600B still decodes S29902 after that Pending was logged. This
demonstrates why native Pending is not an exact per-Product-frame start
or proof of continuously available sender payload.

More importantly, this Q Original loses: TCP physical2 copy writes
C20529@53717 and decodes S30007@54026; mux has passed it by
S30079@55955. Q only decodes S30347@59339. The longest native episode is
therefore not a 9.567s useful current-frontier hold. Repair8/179 similarly
waits 7362572us until S30089@56255, then 72us to return S30090. The first
following Product decode is predecessor [300389929,300401929), not the losing
F233453673 repair. Native-stream order must be retained when interpreting
that older copy's 8.810s local-write→decode age.

## Sampled phases and bounded cost context

S=client locally consumed source, T=server target-written upload,
Rs=server target-read reply, Rc=client locally delivered reply. These are
not respectively C, mux F or an exact ACK-content dictionary. Per-side
generated timestamps—not one runner elapsed value—bound sampled holds.

| Rows / phase | Server Unix milliseconds minus1788866600000 | S | T | Rs / Rc |
| --- | --- | ---: | ---: | ---: |
| L18→20, QoS | 36487→38488 | 232667241 | 165558377 | 730 / 730 |
| L25→26, ordinary suffix | 43488→44487 | 299967177 | 232858313 | 772 / 772 |
| L28→31, largest forward hold | 46488→49487 | 300562537 | 233453673 | 800 / 800 |
| L36→37, return lag | 54487→55488 | 302994833→310524009 | 235885969→243415145 | 968→1010 / 954 |
| L40→41, later forward | 58487→59487 | 369886953 | 302778089 | 1122 / 1122 |
| L50, last sample | 68487 | 391184384 | 391184384 | 1401 / 1360 |

The first and largest sampled forward holds have exactly 64MiB S−T;
the separate Rc hold coexists with actual increasing target service.
At L50 target writing is complete but 41 reply bytes remain undelivered,
so final sampled cost is not after complete logical settlement.

Client peak/last RSS is 270492/255380KiB; server 170216/148416KiB.
Maximum/last ps %CPU is 77.1/40.5% client and 46.1/20.2% server. These are
process-lifetime average CPU samples, not interval cost or handler-exclusive
CPU. RSS is resident process memory, not an exact Product-owner allocation.

Router HTB class first→last deltas are 505036982 upload bytes/408616 packets/
7012 drops and 17399613 return bytes/90156 packets/1346 drops (eth1/eth0).
They include protocol, feedback, copies and native retransmissions, stop
before full settlement, and are not repair-only amplification. Parent/child
qdisc counters must not be added. These are the first two router arrays;
the later root-qdisc query is independently sampled. Upload class backlog is 2787612B at L18,
2354621B at L20 under 10Mbps, 2485806B at L26 after 500Mbps restoration,
and 81864B at L31. Aggregate Q/C neither locates an individual blocking byte
nor proves an earlier assignment optimal; it supplies physical context only.

## Classification and stop

For the tightly joined winning Q reads, the observed missing native head
becomes available late and returns promptly. This rejects a multi-second
postavailability first-Chunk delay for those episodes. It does **not** prove
a controller bug, all-native causation, or an ideal/unavoidable delay bound.
Already-created physical queues, stochastic loss, transport service and
throughput/latency placement tradeoffs remain possible.

The user's competing explanation is retained: unideal performance need not
be a code defect. Exact matched baseline comparisons and their limitations
remain root-owned; this diagnostic's 63.810Mbps is not a benchmark win over
another random realization. No runtime fix, threshold, controller tuning,
new queue policy or practical promotion follows from this capture alone.
