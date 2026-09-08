# QoS forward-prefix diagnostic

Recorded:2026-09-08 16:13 +08:00. Category: completed bounded causal capture.
No runtime correction, release or performance acceptance follows from this
report. The ordinary e476308 pair remains mixed/adverse in
[ADVISORY_OWNER_SERVICE_20260908](ADVISORY_OWNER_SERVICE_20260908.md).

## Capture and measurement boundary

One unchanged e476308 diagnostic, with the temporary
[QOS_FORWARD_PREFIX_TRACE overlay](QOS_FORWARD_PREFIX_TRACE_20260908.patch),
built warning-free in3m34s. All hooks were archived and reversed before running
the frozen `./.tmp/reflection/bin/qos-forward-prefix-20260908/mptunnel`.
Raw evidence is
`./.tmp/reflection/results/mixed-combined-up-qos-forward-prefix-0908/`:
`probe.json`, `probe.err`, `client.log`, `server.log`, `service.jsonl`.
The complete capture and build/run logs are preserved in
[QOS_FORWARD_PREFIX_20260908.raw.tar.gz](QOS_FORWARD_PREFIX_20260908.raw.tar.gz).
The logs contain116157/36839 client/server lines and66 service samples.

The profile remains routed/mirrored single500Mbps, upload70/20ms and
return30/5ms delay/jitter. Five-second upload loss epochs are
[3,8,5,6,10,3,5,8]% and return[1,2,.5,3,2,.5,1,2]%; upload is10Mbps during
15–25s, with UDP blackhole30–33s,40s offered load and the existing85s runner
guard/90s probe boundary. No setting was changed for this capture. The observed
UDP blackhole is present in service rows31–33 and absent again in row34.

Product commit means accepted exact ownership, not a write. TCP
`flush_complete` and QUIC `write_complete result=ok` mean local protected/native
acceptance, not transmission or peer ACK. `request_reader` means complete MPP
decode; server stall events report the incoming range and actual mux frontier
before/after. They do not carry an invented mux path identity. No aggregate
cost overlay or new per-attempt scheduler instrumentation was added. Existing
recovery events remain in the capture, including37685 `reinjection` rows;
these are not37685 accepted copies.

## Completed transfer and exact accounting control

| Probe field | Result |
| --- | ---: |
| Accepted / target-confirmed / completed bytes |374669312 /374669312 /374669312 |
| Elapsed seconds |65.783216 |
| Exact goodput Mbps |45.564 |
| First confirmation / maximum confirmation gap seconds |.628708 /5.592993 |
| First write / maximum write gap seconds |.261841 /3.933834 |
| Complete / failed streams; process exit |1 /0;0 |
| Accounting |Exact target-sink ACK; valid; not a lower bound |
| Probe errors |None |

All18779 Original Product commits form exactly[0,374669312), with no holes
or overlapping Original ownership. An additional5230 repair commits represent
65529144 copied bytes, not unique throughput. All24009 commits have one matching
write begin and one positive local completion; no mismatch or write-error
completion is logged. TCP joins use exact physical instance and range; QUIC
uses the sole observed connection's ordinary H3 request4 versus repair H3
request8. Encoding splits/coalescing require byte-coverage joins at decode.

The6409 applied ACK rows release exactly374669312 bytes. Every row satisfies
F_after>=F_before and F_after/largest_end<=claimed_end. Final ACK C116156 at
Unix1788854827572 has claimed_end=F=374669312 and zero retained bytes.
This tests log/accounting consistency, not fluent delivery.

QUIC Original commits/decoded bytes both total307016028; its repair
commits/decoded bytes both total18637200. TCP accepted writes total114545228B,
while completed-capture TCP decode totals102874268B. Losing TCP work can remain
undecoded when the logical transfer completes and teardown starts. Missing
decode for such work is not by itself corruption or an accounting defect.

All66 raw one-second confirmation bins are preserved below in Mbps. Labels
are zero-based starting indices, not timestamps; the final bin is partial.
Do not replace the zero spans with the probe's trimmed interval average or
interpret a buffered confirmation burst as physical link capacity.

```text
 0: 1.669, 10.388, 14.68, 408.945, 32.078, 45.997, 135.695, 0, 53.714, 43.708
10: 69.73, 23.593, 100.331, 49.807, 256.185, 138.508, 76.974, 0, 0, 0
20: 0, 26.835, 3.286, 0, 0, 10.345, 3.242, 56.195, 36.014, 0.722
30: 0.187, 101.454, 65.356, 0.407, 0.096, 146.609, 175.732, 214.529, 12.436, 0.288
40: 2.333, 1.957, 4.002, 4.29, 5.479, 4.054, 0.857, 2.237, 5.767, 9.681
50: 0, 0, 0, 0, 0, 164.907, 0, 0, 7.34, 69.826
60: 46.189, 9.769, 74.947, 0, 0, 267.984
```

## Three exact held-frontier chains

C/S are one-based client/server log lines. Short times are Unix milliseconds
minus1788854700000; process-relative clocks are not compared. Session ID is
the exact integer string6120868306693267468, stream0. Do not parse that session
ID through an inexact JavaScript Number.

The relevant TCP Original uses client runtime index1/physical3/attachment2,
wire PathId2. Server decoded PathId2/physical3 is its separately owned receive
identity, not proof that physical counters share a namespace. QUIC uses client
runtime0/physical4/attachment1, server PathId0/physical4; H3 IDs are
connection-local. Product commit identities and writer wire IDs establish the
join rather than coincidental numeric equality.

| Held F / measured gap | Original commit → local completion | Covering repair | Winning decode → mux |
| --- | --- | --- | --- |
|185133452 /3.432533s |TCP[185133452,185138988), C59919–21@77095 |QUIC repair C70089/C70093–94@82685; arrives too late to win |TCP S17370@81878→S17381@81879;1ms |
|187796428 /2.661961s |QUIC[187796428,187808428), C60654@77292→C60675/C60696@77296 |No covering repair commit/write in this capture |QUIC ordinary S17647@87134→S17679@87135;1ms |
|298838412 /4.752859s, largest |TCP[298838412,298843948), C96031–33@98979 |QUIC stale-path copy C104150@106907→C104155–56@106908 |QUIC repair S30044@116659→S30172@116664;5ms |

The first two are QoS-phase forward holds; the largest is a later drain-period
hold, not itself evidence of the configured15–25s shaper. Their incoming ranges
cover the actual F and advance it by5536/12000/5536 bytes respectively, while
other holes remain. The largest server mux gap differs from the probe's
maximum confirmation gap: these are different measurement boundaries.

For F185133452, Original write→decode is4783ms; only3432.533ms is its actual
current-frontier hold. The QUIC copy is published806ms after mux already
advanced, then decodes S17680@87137. It cannot explain recovery. However the
client applies the covering positive ACK only at C70112@82693,8ms after copy
publication. Server hindsight alone does not make that sender repair a false
loss inference. This valid incomplete ACK has frame-local frontier0 but
advances cumulative F185133452→185198988: the two frontier meanings differ.

For F187796428, Original write→decode is9838ms, not the2661.961ms current-F
hold. Its earliest covering ACK is C70616@87603, moving cumulative F to
188148108. The absence of a covering repair is observed; target eligibility,
owner timing and admission at the necessary instant were not fully captured.
It does not alone establish a missing-recovery algorithm defect.

For the largest F298838412 hold, its QUIC copy has already completed local
write approximately5003ms before this F becomes current. That copy's
write→decode is9751ms, followed by only5ms to mux. No TCP decoded interval
covering the head appears anywhere in the completed capture. The actual
winning QUIC repair's previous logged StreamData interval[298826412,298838412) decodes at
S29112@111909; the winner decodes4750ms later on the same H3 repair stream.
Client ACK C114116@116700 advances F298838412→298969484. This directly refutes
“no covering copy was accepted/sent during the hold” for this case.

The repair H3's1689 successful writes align by coverage with1740 split/coalesced
decoded records,18637200 payload bytes each. At the target's local write,
5490376 preceding accepted StreamData bytes had not yet decoded: target start
position10397200 minus decoded position4906824(S27905@106394). However, all
of those preceding bytes have decoded by S29112@111909; they cannot be called
still-undecoded data ahead during the subsequent4750ms pause. Meanwhile ordinary
H3 request4 decodes642 records/6970816B from S29216@112895 through
S29998@115551, contradicting connection-wide receive silence. Unlogged
requalification records may interleave on repair H3; this is a StreamData-only
sequence, not proof of adjacent encoded records or of the pause's cause.

Positive ACK-only lower bounds further constrain stale-copy interpretation.
At target admission/write106907/106908, C103850@106827 has cumulativeF291700524;
155072B of that fixed5490376B preceding-copy cohort lie below F. At gap start
111909, C106789@111903 has F295239468, covering3074656B of the cohort. Copy
multiplicity is retained. Disjoint ACK ranges are incompletely logged, so the
remaining5335304/2415720B are unknown, not proved unACKed. This does not show
the preceding copies were already ACKed at their own earlier admission. In
any case the whole cohort had decoded before the final4750ms silence.

## What remains ambiguous

These three winners arrive promptly at mux after decode. They select a
predecode boundary rather than a long winning-frame mux delay, but do not
prove network/QoS as the sole cause. Ordinary QUIC and TCP readers may wait
on their bounded input channels before decoding their next frame. The QUIC
repair reader instead decodes one frame, validates its exact stream, then
awaits registry/Product routing before the next decode. The preceding route
await, native ordered-byte availability/loss/flow scheduling and task service
are not separated by this overlay. No native-arrival or route-completion
timestamp was captured for the late winning repair.

Local H3 acceptance also releases the repair writer's queue charge, not
Quinn's outstanding native bytes. Product ACK may release exact DSN copy debt
while a losing native copy is still pending. Thus neither a zero adapter queue
nor continued Native ACK activity alone identifies the blocking range or
proves absence of earlier accepted native work. These are interpretation
limits, not a newly proven backlog or congestion-control defect.

## Sampled resource observations and disposition

| Process | Peak / final RSS KiB | Highest sampled / final reported CPU % |
| --- | ---: | ---: |
|Client |821756 /784844 |117 /64.6 |
|Server |125824 /125824 |47.1 /13.3 |

Service row59 at58.214879s contains client peak RSS; row17 at16.005691s its
highest reported CPU. Server's highest reported CPU is row8 at7.000909s;
row66 at65.215647s supplies both final process observations. `ps %CPU` is its
reported process-average utilization, not exclusive instantaneous handler
CPU. Unequal diagnostic work/duration and observer cost forbid interpreting
these values as a causal ordinary regression or a post-cleanup leak test.

This run completes but retains multi-second forward and confirmation gaps.
It supplies exact accounting and competing winner chains, not near-perfect
performance. No controller, threshold, resource or recovery change is justified
solely by these observations; a model-level counterexample is still required.
The ordinary pair, distinct return holds, unchanged global acceptance gates
and absence of release promotion remain intact.

## Continuation: repair read/route discriminator, 2026-09-08

The second capture uses unchanged e476308 and the same profile above. The
[QOS_REPAIR_READ_TRACE overlay](QOS_REPAIR_READ_TRACE_20260908.patch) reuses
the first hooks and adds only repair-reader read/route boundaries, including
StreamRequalifyData. Its warning-free build took3m32s; all11 source-file hooks
were reversed before running the separately frozen
`./.tmp/reflection/bin/qos-repair-read-20260908/mptunnel`. Evidence is
`./.tmp/reflection/results/mixed-combined-up-qos-repair-read-0908/`, preserved in
[QOS_REPAIR_READ_20260908.raw.tar.gz](QOS_REPAIR_READ_20260908.raw.tar.gz).
There are98192/49347 client/server log lines. This continuation does not
retroactively identify the cause of the first capture's4750ms repair pause.

The probe completes exactly485359616 accepted/target-confirmed bytes in
52.139505s,74.471Mbps, with one complete stream, zero failed streams/errors and
exit0. First confirmation/write are.544772/.269165s; maximum confirmation/write
gaps are7.128953/4.925427s. Runner duration53.1968s is a different boundary.
All53 raw one-second confirmation bins follow; indices are zero-based and the
last bin is partial. The561.941Mbps confirmation burst is not proof of a
physical500Mbps link exceeding its configured service rate.

```text
 0: 2.097, 6.291, 9.437, 10.057, 561.941, 62.507, 82.817, 64.391, 452.557, 49.304
10: 73.38, 23.069, 26.118, 48.235, 65.536, 0, 0, 0, 0, 0
20: 368.147, 64.915, 0, 28.429, 0, 27.787, 141.537, 43.516, 327.893, 19.09
30: 80.364, 80.281, .075, .096, 3.478, 4.386, 13.823, 137.267, 347.626, 2.097
40: 1.004, 10.184, 106.06, 0, 0, 0, 0, 0, 0, 180.383
50: 242.909, .446, 113.346
```

Exact accounting still holds. The13438 Original commits form precisely
[0,485359616), with matching write-begin and positive completion for each.
An additional2620 repair commits account for41384032 copied bytes. QUIC
ordinary12684 successful write ranges align in byte order with40832 decoded
ranges/444484143B; repair391 writes align with1396 decoded StreamData
ranges/14355944B, including encoder splitting/coalescing.

Do not generalize this to all16058 Product commits completing native writes:
16055 writer begins and16054 positive completions are observed. Four losing
TCP physical2 repair transactions remain incomplete in the trace:
[461631705,461643705) has commit C96437 and begin C96470 but no completion;
[461643705,461644489),[461691705,461697241),[461697241,461706305) have commits
C96439/C96478/C96480 but no begin. Applied ACKs C96476/C96482 cover these
ranges. This is not missing unique data; cancellation versus outstanding
writer service is not established by these hooks. All7913 applied ACKs release
exactly485359616B and preserve monotonic F<=C. Final C98190 at
Unix1788855777660 has C=F=485359616 and zero retained bytes.

For the following joins C/S again denote one-based log lines, but short times
now subtract1788855700000 from Unix milliseconds. The exact session string is
5050659630538301190, stream0. Client QUIC is runtime0/physical1/attachment2,
ordinary H3 request4 and repair8. The relevant Original TCP is client
runtime1/physical2/attachment1, wire PathId2; server identities remain separately
owned. Do not reuse the first capture's identity mapping.

| Actual delivery gap | Publication and winning path | Read/route/mux boundary |
| --- | --- | --- |
|F238223201 /5.138138s, QoS phase |TCP Original C47255@36318, local flush C47262@36319; winning QUIC copy C57953/C57956@40636 |Repair ordinal235 begins S22871@40440, decodes S23028/S23029@45535, route S23030=27us, mux S23032@45536 |
|F471135545 /6.295860s, largest |F reached S47121@70186; Original only C94200–02@74162; winning QUIC copy C96800/C96803@76229 |Repair ordinal1371 decodes S48028/S48029@76481, read16us, route S48030=18us, mux S48036@76482 |

The QoS winner spends4899ms from successful local write to decode, followed
by approximately1ms to mux. Its5095096us read await starts196ms before that
write; the route await is not its multi-second delay. TCP Original decodes
later at S23040@45592. During the accepted-copy wait, ordinary QUIC decodes149
records/1626400B (S22876@40856 through S23026@42647), excluding connection-wide
decode silence. Ordered native availability/loss and task service remain
unsplit: read-await time is not physical-wire waiting alone.

The largest delivery-gap record has a different explanation boundary. When
F471135545 is reached at70186, reordered bytes are zero; its missing Original
is not claimed until74162, approximately3976ms later.
Source S is already485359616 at clientUnix1788855768480 and remains unchanged;
C is471135545 from C89457@68064 until C94200@74162. Thus14224071 consumed-source
bytes await claim. Applied ACKs continue, from C91838@70172 F423163193 to
C94190@74152 F435960841, still with C471135545. This identifies a claim-service
boundary, not its refusing predicate. Its winning repair takes
only approximately253ms from local completion to mux. Preceding repair
ordinal1369 appears to wait7094009us (S46134@69387 to S48020@76481), but the
eventual [471070009,471082009) copy is not published until C96794@76228 and
locally completed C96797@76229. Approximately6841ms of that read precedes
publication, not accepted native service. That earlier range had already
arrived by TCP at S47108@70185; its covering ACK reaches the client only at
C96798@76229. Neither delayed sender knowledge nor a read waiting for future
work proves spurious loss inference or a seven-second native repair backlog.

The new observer is internally consistent:1408 read-begins,1407 successful
read-completes and1407 successful route-completes, with matching ordinal,
kind/range metadata and phase order. Completed kinds are1396 StreamData and11
StreamRequalifyData. Final ordinal1408 at S49249@77624 is begin-only, not a
fabricated completion. Total measured route awaits are41466us; maximum3494us
at S25593@51610, ordinal683, is the only route above1ms. Thus this capture
contains no multi-second repair registry-route await. Its longest read,
7535273us at ordinal1223 (S28184@52317 to S33819@59853), returns
StreamRequalifyData rather than the winning payload; it cannot be assigned
wholesale to an ordered-data stall.

The discriminator closes this capture's route-await alternative, not every
native/read/task alternative or the earlier capture's uncaptured boundary.
It also exposes why delivery-gap records require source-publication joins.
Completion with multi-second gaps remains non-acceptance; no runtime change,
threshold adjustment or release promotion follows from this continuation.
