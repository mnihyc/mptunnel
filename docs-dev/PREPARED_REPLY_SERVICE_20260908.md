# Prepared request candidate: exact reply service

Recorded: 2026-09-08 14:18 +08:00. Category: bounded diagnostic evidence,
not performance acceptance or a new runtime correction.

## Capture and scope

Source is `8e8e99a` (`b3dfef1` plus the test-only import cleanup) and the temporary
PREPARED_REPLY_SERVICE_TRACE_20260908.patch. The executable was frozen at
`./.tmp/reflection/bin/prepared-reply-service-20260908/mptunnel`; hooks were
archived and reversed before capture. Raw logs are under
`./.tmp/reflection/results/mixed-combined-up-prepared-reply-service-0908/`.
`C` and `S` below mean one-based lines in client.log and server.log.

The unchanged diagnostic profile completes exactly 440598528 B in 45.810536s;
runner exits 0. Maximum confirmation gap is 3.459360s. This is not an ordinary
performance comparison: it does not reproduce the prior ordinary31s target
hold or establish that observation improved the Product. No promotion follows.

All 100 response frames that advance client mux F join to their exact sender
publication, local native write, and client decode. They comprise 8 TCP Originals,
7 TCP repairs, 84 QUIC Originals and 1 QUIC repair. No server publication repeats
the same path/stream/range key. For these winners, publication to native write
completion is at most2ms. These counts exclude slow losing copies.

TCP identities in this capture must be translated, not equated:

| Wire PathId | Client runtime index | Client physical instance | Attachment |
| --- | ---: | ---: | ---: |
|0 |2 |1 |1 |
|1 |0 |2 |0 |
|2 |1 |4 |3 |

QUIC is runtime0/physical3/attachment2; its ordinary H3 request ID is 4 and
repair request ID is 8. Server and client physical-instance numbers are independent
namespaces even where their numeric values happen to match here. TCP earliest
decode identifies wire path/session/port; routing supplies runtime/physical
identity. Session 13516197427470576723, logical stream 0, is used throughout.

## Largest actual frontier hold: already decoded winning repair

F953 holds from C2009 at Unix1788848028250 until C2102 at1788848031710:
**3.460s**. The winner is TCP wire2/runtime1/physical4/attachment3 repair
[953,967). Every timestamp in the following table is Unix milliseconds minus
1788848000000; this is a common wall-clock domain, not process-relative time.

| Boundary | Time | Evidence |
| --- | ---: | --- |
| Prior ordered delivery reaches F953 |28250 |C2009 |
| QUIC Original[953,967) publication/write completion |28804 |S564–566 |
| Winning TCP repair publication/stage/write/flush |29456 |S578–581 |
| Winning repair authenticated decode |29484 |C2040 |
| Native interlocked route attempt, mailbox full/pending |31316 |C2089–2090 |
| Attachment forwarding to shared input begins/completes |31649/31651 |C2095–2096 |
| Shared input dequeue |31709 |C2101 |
| Mux F953→967 |31710 |C2102 |

The winning copy's write-to-decode is 28ms; **decode-to-mux is 2.226s**.
It spends 1.832s before the native actor attempts routing, then 333ms before
attachment forwarding begins, and 61ms from that begin to mux application.
The latter subdivisions identify boundaries, not exclusive CPU or exact
channel-Pending durations. The route event explicitly reports
mailbox_full_pending, and the shared queue reports131 used slots.

Two sibling TCP copies decode earlier (C2038–2039 at29077/29289) but only
reach mux after the winner (C2104/C2106). QUIC Original[953,967) decodes at
34387 (C2231), reaches mux at34391 (C2268), and loses with F already995.
Its5.583s write-to-decode is not the winning delay to optimize. Conversely,
the2.226s bound above follows the actual winner and therefore directly proves
local return-service delay, independent of those losing maxima.

The next-largest winning decode-to-mux delay is 212ms: QUIC Original[295,309),
C594@5667→C608@5879, with route C600@5762 and attachment C605@5854. The following
winner[309,323) takes 209ms (C609@5946→C658@6155). Other winners are at most 130ms
after decode. This capture does not show a universal multi-second local hold.

## Contrary long gaps: service is not explained by one client cost total

Times use the same base1788848000000.

| Held F / full gap | Initial Original publication | Winning write completion → decode → mux | Interpretation |
| --- | --- | --- | --- |
|729 /3091ms, C1425@13891→C1503@16982 |S462@16350 |QUIC repair S476@16952→C1498@16982→C1503@16982 |2459ms precedes any Original publication; winning repair has 0ms postdecode delay |
|743 /2954ms, C1503@16982→C1602@19936 |S477@19114 |TCP Original S480@19115→C1596@19936→C1602@19936 |2132ms prepublication, 821ms write-to-decode, 0ms postdecode |
|785 /2626ms, C1616@20505→C1706@23131 |S489@23093 |TCP Original S492@23093→C1700@23130→C1706@23131 |2588ms prepublication, 37ms write-to-decode, 1ms postdecode |
|841 /2624ms, C1759@24720→C1913@27344 |S505@24962 |TCP Original S508@24963→C1907@27308→C1913@27344 |2345ms write-to-decode, 36ms postdecode; the winner releases F841→911 |

The only winning QUIC repair is[729,743); server repair write and client
repair decode/route hooks close that separate H3 domain. Late QUIC repairs of
[743,757),[785,799),[841,855) lose after TCP already advances F. A capture that
observed only the ordinary QUIC reader would miss the winning[729,743) chain.

## Causal question and limits

The exact F953 chain selects client local service as a real bottleneck in that
episode. The next bounded question is which work or wait occupies the native
reader/router and Product input owner between29484 and31710, and specifically
whether repeated request recovery dispatch performs redundant work while those
reply bytes wait. Aggregate completion buckets may support that question but
must not be assigned wholesale to a narrower interval or called its full cause.
Root's aligned cost evidence belongs below this section.

No queue-size, ACK policy, congestion gain, path preference or timeout change is
justified by these joins alone. Prepublication gaps select a different stage;
write-to-decode includes native/network/reader service and is not purely wire
delay. Local write completion is native acceptance, not peer arrival. A full
interlocked mailbox result is not successful admission; attachment receipt
bounds its later progress. The capture has no exact TCP reader-mailbox completion
event between authenticated decode and native routing. QUIC preceding nondata
send-await aggregates contain only completed waits and include scheduling time.
No Product-lock or new same-stream logical await cycle was proved by the source
audit; that does not exclude bounded backpressure or expensive synchronous work.

## Aligned costs and contrary activity

Build3m33s, warning-free. Only3909 diagnostic log lines were emitted; no
per-ACK/reinjection flood or per-attempt perf samples. The existing aggregate
observer counts actual claim calls after weak registration/owner upgrades.
It reports8427successful claims/440598528B/1.434545s,14520blocked/4.492534s,
300Busy/.030524s and2561Empty/.304965s. Claimed bytes are not wire bytes.

Approximate phases below group aggregate completion timestamps relative to
first client management Unix1788847999014. Crossing buckets are not prorated.
Times are elapsed, not CPU; claim timing includes locks/native sampling,
whereas the five lexical preparation scopes exclude their initial Product-lock
acquisition. Rows and nested regions must not be summed as exclusive CPU cost.

| Phase | Claim calls / successful | Claim elapsed s | Preparation s | ACK handler s | Dispatch handler s |
| --- | ---: | ---: | ---: | ---: | ---: |
|Before15s |10838 /4870 |2.067744 |1.665086 |1.374198 |.032908 |
|15–25s |2110 /1099 |.415904 |.051087 |.018770 |.002342 |
|25–30s |1013 /205 |.759026 |.096445 |.070909 |1.369815 |
|30–33s |866 /45 |.880847 |.102265 |.118934 |2.436335 |
|33–40s |3003 /138 |.785253 |.127496 |.038410 |.565817 |
|40s onward |7978 /2070 |1.353794 |.349208 |.288838 |.009002 |

Dispatch total4.416219s contains recovery-dispatch4.354264s. Separate dispatch
collection.332585s contains recovery-collection.295766s. ACK handler1.910059s
contains Product ACK1.711001s and flight release1.652844s. Preparation totals
2.391587s; nested admission/recovery helpers are already included.

| Bucket completion Unix ms | Dispatch elapsed s | Nested recovery elapsed s |
| --- | ---: | ---: |
|1788848029069 |.717618 |.710893 |
|1788848030070 |.846876 |.846837 |
|1788848031071 |.871841 |.869930 |

These buckets overlap the actual F953 held-input interval and sampled Rc953
hold8029014–8031015. They locate substantial synchronous recovery work, not an
exclusive allocation of their whole duration to that reply. A different tail
bucket ending8041216 has4380blocked claims BUT also1859successful claims/
67189000B: it does not demonstrate unchanged-state notification recurrence.

Completed TCP reader queue sends total.163840s/max45.380ms; completed local
writes total.003847s/max.393ms. These are contrary evidence to treating final
local writes as this capture's long-delay owner, not retrospective measurements
of the preceding ordinary freezes. Peak RSS client/server355100/156304KiB;
final lifetime CPU47.1%/17.5%, peaks83.9%/38.7%. Sampled upload/return class
deltas571181339/22414638B include control, framing, retries and copies. Unequal
diagnostic work prevents normalized efficiency comparisons.

All46 raw confirmation Mbps bins, without trimming:

```text
0–9:   1.572,7.748,17.302,191.365,477.058,40.939,392.027,50.471,3.767,170.414
10–19: 36.679,108.003,98.566,75.069,20.019,0,0,398.908,0,0
20–29: 6.816,145.163,0,0,.140,41.440,0,0,76.001,43.419
30–39: 0,0,45.613,48.759,272.147,3.104,74.044,0,12.988,4.194
40–45: .309,4.794,80.569,0,293.536,281.844
```

## Observer audit and next source discriminator

The suspicious diagnostic-only TCP byte/item increments occur only on branches
that immediately break, before another budget/coalescing decision. Continuing
branches use identical counters in both builds. QUIC budgets and relay yields
are unconditional. No active budget difference is established in this bounded
audit. One pre-existing feature-only Product diagnostic read does acquire and
release the shared lock, even when its log event is filtered. Release can wake
a writer that encountered Busy, not a model-Blocked waiter or create admission.
That and ordinary observer timing effects prevent a feature-neutral speedup
claim; this capture does not attribute the45.8s versus85.4s difference.

Next inspect the actual recovery query work during no-target/held-service
dispatch. Its origin preserves globally ordered recovery and independent
eligible targets; retain those properties. Prove an actual repeated equivalent
query counterexample before proposing scoped work sharing. If current differing
Native/ownership evidence makes each query necessary, reject that optimization;
do not add a rate/iteration cap, delay incoming feedback, or hide slow phases.
No additional ordinary repeat or runtime correction has been made.

Full logs, all46 service snapshots, probe outputs and build/run logs are retained
in [the raw archive](PREPARED_REPLY_SERVICE_20260908.raw.tar.gz); the exact observer
patch is retained separately. All hooks are reversed, products/probes stopped,
the user's seven-line edit preserved, and no push/release has occurred.
