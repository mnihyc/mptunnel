# Prepared request claim-service diagnostic

Recorded:2026-09-08. Category: completed bounded causal capture on runtime
e476308. No runtime correction, performance promotion or release acceptance.
The preceding ordinary result remains mixed/adverse in
[ADVISORY_OWNER_SERVICE_20260908](ADVISORY_OWNER_SERVICE_20260908.md).

## Capture and measurement boundary

One warning-free optimized diagnostic build took3m33s. The temporary16-file
[PREPARED_CLAIM_SERVICE_TRACE overlay](PREPARED_CLAIM_SERVICE_TRACE_20260908.patch)
was archived and fully reversed before running both endpoints from
`./.tmp/reflection/bin/prepared-claim-service-20260908/mptunnel`.
Raw directory:
`./.tmp/reflection/results/mixed-combined-up-prepared-claim-service-0908/`.
The probe, client/server logs and full service snapshots are preserved in
[PREPARED_CLAIM_SERVICE_20260908.raw.tar.gz](PREPARED_CLAIM_SERVICE_20260908.raw.tar.gz).

The [fixed profile](ADVISORY_OWNER_SERVICE_20260908.md#fixed-ordinary-profile-and-order)
is unchanged: routed/mirrored500Mbps, upload70/20ms and return30/5ms
delay/jitter; five-second upload loss epochs[3,8,5,6,10,3,5,8]% and return
[1,2,.5,3,2,.5,1,2]%; upload10Mbps during15–25s; UDP outage30–33s;
40s offered load,85s runner guard and90s total probe boundary. No concurrent
build or second diagnostic trial. Random packet realizations are not identical.

The question follows the previous capture's positive source but delayed claim:
which actual prepared-claim predicate prevents the lowest source from advancing?
Exact publication/write/decode events were reused; bounded claim, planning and
notice counters distinguish refusal, readiness and wake alternatives. Counts
are cumulative through their emitted samples, not promised final totals.
Sampled state is not reconstructed between emissions. Planner counts are
branch visits, not claim totals; a notice publication attempt is distinct from
queue acceptance. Physical-key counters can merge attachment generations.

Product commit is exact accepted ownership. TCP `flush_complete` and QUIC
`write_complete result=ok` are local acceptance, not wire delivery or peer ACK.
`request_reader` is authenticated MPP decode. A server delivery-stall event
names the actual mux frontier and incoming covering range, not a separately
invented winning-path field. Decode joins below use interval coverage.
Source I/O S, claimed offset C, ordered target writes T, server-read reply Rs,
client-delivered reply Rc and Product ACK frontier F remain distinct counters.

## Completed probe and resource context

| Probe field | Result |
| --- | ---: |
| Local accepted / target confirmed / completed bytes |484507648 /484507648 /484507648 |
| Elapsed seconds / exact goodput Mbps |44.176499 /87.740 |
| First confirmation / maximum confirmation gap seconds |.481934 /4.478852 |
| First write / maximum write gap seconds |.142521 /8.647851 |
| Complete / failed streams; probe exit |1 /0;0 |
| Runner outcome |Completed, exit0 |
| Accounting |Exact target-sink ACK; valid; not a lower bound |
| Probe errors |None |

The probe uses one duration-upload stream,40s load and50s post-load completion
timeout. All45 raw one-second confirmation bins follow, in Mbps; labels are
zero-based starting indices, and the final bin is partial. Buffered ACK release
can exceed500Mbps within one confirmation bin; it is not measured physical
link capacity. The trimmed39-bin average74.981Mbps is not substituted for the
whole transfer or its zero spans.

```text
 0: 1.048, 21.086, 300.181, 0, 131.5, 48.522, 54.57, 382.73, 162.298, 170.101
10: 49.924, 27.146, 39.846, 2.097, 620.011, 0, 0, 0, 0, 124.293
20: 8.389, 0, 0, 14.444, 1.049, 42.8, 311.471, 115.937, 0.818, 0.234
30: 0.35, 0.249, 0.122, 0.393, 0.371, 0.213, 0.35, 1.268, 541.256, 1.768
40: 46.254, 23.476, 545.784, 34.603, 49.107
```

| Sampled process observation | Client | Server |
| --- | ---: | ---: |
| Peak RSS KiB |346776 |174192 |
| Last RSS KiB |346776 |157852 |
| Peak observed `ps %CPU` |122.0 |47.7 |
| Last observed `ps %CPU` |56.5 |23.0 |

RSS values are sampled resident memory, not exact allocation peaks. `ps %CPU`
is a process-lifetime average at each sample, not interval CPU utilization or
request/claim CPU time; values above100% can reflect multiple cores. Diagnostic
formatting/output affects cost. These observations and copied Product bytes
below are not normalized ordinary-build costs or repair-only wire accounting.

## Exact Original, copy and ACK accounting

All10428 Original commitments form exactly[0,484507648), without holes or
overlapping Original ownership. Every Original has a matching writer begin,
positive local completion and server decode coverage. Additional1188 copy
commitments total45020616B; these are duplicate Product work, not useful bytes.

| Native domain | Positive local writes | Payload bytes | Server decoded records |
| --- | ---: | ---: | ---: |
| QUIC ordinary H3=4 |9785 |450346765 |41352 |
| QUIC repair H3=8 |354 |13564002 |1346 |

Both QUIC decoded payload sequences match their local write sequences exactly,
including splitting across decoded MPP records. H3 records and Product command
counts are different units. All643 TCP Original records are also decoded.
Across all carriers there are11616 commitments,11613 writer begins and11612
positive completions. TCP has1274 decoded records/54881953B.

The6367 applied client ACK rows release exactly484507648B. Final ACK C73511
atUnix1788857705823 has C=F=484507648, complete=true and zero retained bytes.
This is accounting consistency, not proof of fluent intermediate service.

Four losing TCP stale-copy commitments, total261720B, have no positive local
completion. All use client runtime1/physical3/attachment3:

| Client commitment line | Exact range | Writer observation |
| --- | --- | --- |
|C71905 |[455803678,455869214) |Begin C71934 only |
|C71906 |[455869214,455934538) |No begin |
|C71918 |[456131146,456196470) |No begin |
|C71935 |[465917392,465982928) |No begin |

These are logfile line numbers, not potentially interleaved diagnostic sequence
numbers. Overall203 TCP copy commitments/10735544B lack a decoded counterpart,
including those four. The remaining199 have positive local completion.
No Original is missing. C72563 atUnix1788857703509 advances cumulative F to
467228112 and positively covers all four exceptional ranges; final ACK covers
the entire source. Earlier sparse ACK logs do not expose every range, so their
largest-end field alone cannot prove when each copy became redundant.
Incomplete losing-copy transactions do not establish corruption, cancellation
or a particular native failure; transfer completion can leave duplicate work.

## Largest current-frontier holds

C/S denote one-based client/server logfile lines. Short times in this section
are Unix milliseconds minus1788857600000, not process-relative clocks.
Logical stream0 belongs to session15024924616572160796. Client TCP runtime
indices0/1/2 map to wire PathIds1/2/0, physical instances2/3/1 and attachment
IDs0/3/1. QUIC runtime0 is physical4/attachment2, wire PathId0, ordinary H3=4
and repair H3=8. Server physical identities are separately owned; joins use
session, underlay/wire identity and exact ranges, not bare numeric equality.

There are135 server delivery-stall events. The two largest are QoS-phase
already-written QUIC Originals, not unclaimed blocking prefixes:

| Held F / actual gap | Original range; commit → positive write | Winning decode → mux |
| --- | --- | --- |
|261515956 /4.408347s |Q[261467956,261533492); C49922@75970→C49926@75972 |Q ordinary S23457@80985→S23573@80991 |
|269707956 /3.514626s |Q[269659956,269725492); C50285@76037→C50303@76038 |Q ordinary S24212@85509→S24218@85509 |

The arriving winning records are respectively[261515956,261527956) and
[269707956,269719956). Their Original local-write→decode intervals are5013ms
and9471ms, not the shorter durations for which those bytes were the current
frontier. A TCP runtime2 tail copy of the first range is committed/written
C53293/C53296@77956, but decodes S23878@81064, after mux release. No covering
copy commitment is logged for the second head. These observations locate a
post-local-acceptance/predecode boundary; they do not distinguish native ordered
delivery from task service before decode.

Contrary winners matter. F134348747 holds1.088939s: TCP2 Original local write
C25672@67964 is followed by a winning QUIC repair C31094/C31099@69419/69420,
decoded S13427@69537 and applied S13429@69538. Conversely F328858420 holds
.897295s: its TCP2 Original was written C53356@81019; QUIC repair writes
C58827@91997, but TCP1 repair C58849@92155 wins at S35902/S35903@92792.
That QUIC copy decodes later at S35936@95147. Neither carrier is uniformly the
prompt alternate. Late F471356880 holds only.566944s: Q Original claim/write
C72624/C72639@103659→S42924/S42925 decode/mux@103927. It does not reproduce
the earlier capture's multi-second unclaimed current-head interval.

## Claim and notice discriminator

The longest gap between Original commitments holds C329149108 for4.987s:
C53369@81020→C53538@86007. Prepared U is already172608B at C53379@81028
and reaches7548640B at C53537@86006; source queue front4488 is4.979s old at
the latter observation. Yet the QUIC ordinary writer already owns a208608B
batch[328416212,328624820), whose four write begins C53285–53288@76733
complete C53522–53525@86006:9.273s of awaited local write spans the C hold.
This is not a Ready QUIC writer waiting solely for a lost notice.

QUIC enqueue sample C53309@81015 has count18917; its next claim entry
C53526@86006 has that same count. Between samples@82016 and@85534, TCP
physical1/2/3 claim-entry counters advance107/126/120: C53400→C53487,
C53406→C53485 and C53397→C53490. These353 further entries are inside the
C hold. Thus no lost-notice or critical-queue-front cause
is supported for this largest C interval; an occupied QUIC writer and continued
TCP attempts must not be relabelled an absent wake. The elapsed write includes
awaited native backpressure and scheduling, not measured CPU or packet flight.

A narrower late sampled refusal selects a different source boundary. At
C72286@102445 and C72529@103448, C remains471356880, QUIC is Ready, and U is
393216/1507328B. Cumulative F is404641232/405755344 and Data front5262 remains
present. Planning rows C72285/C72528 show Regular-tier stale eligibility true
but input `can_enqueue=false`; fresh TCP writers are not Ready in the relevant
samples C72281/C72355/C72399. Whole-frame allowance is not observed until
C72623, after cumulative F catches C.

The source audit identifies the bounded composition:5d660f3b introduced a
legacy stale preference based on attached active/scorable nonstale outputs;
9720e4b added finite Ready plus fresh/stale tiers while reusing the prefiltered
input. A first mask that ignores Ready cannot be undone by the second tier's
AND. Its useful intention is to avoid feeding stale outputs while retaining
sole-survivor service. The capture demonstrates this refusal boundary and the
source review identifies the overlapping policy masks. It does **not** prove
that every downstream whole-frame W/P/E, qualification or Native check would
have allowed the late claim, nor that this gate caused the largest server holds.

## Disposition

Completed diagnostic and exact accounting; no practical acceptance. The
largest forward stalls remain already-written/predecode, while the narrower
late Ready/stale refusal justifies the predeclared actual-producer counterexample
at that boundary. A correction requires executed RED, opposite controls and
independent review before ordinary evaluation. No timer, congestion gain,
critical-queue policy or protocol preference changes follow from this report.
