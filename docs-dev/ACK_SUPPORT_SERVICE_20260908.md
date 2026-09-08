# ACK-support service — ordinary pair, 2026-09-08

**Disposition: work reduction proved; ordinary result mixed/adverse, no
performance promotion.** Both cells complete exactly. Candidate d999fea
finishes1.223678s earlier but transfers79953920 fewer bytes; maximum
confirmation gap worsens3.060424→5.497708s and maximum local-write gap
4.396938→5.665049s. First confirmation and first write also worsen.
The candidate retains substantial forward target plateaus and separate
return-reply residence. This pair does not assign them to the modified
function or prove a general efficiency benefit.

## Bounded mechanism and evidence

The [reply-residence diagnostic](REPLY_RESIDENCE_20260908.md) located
1.961027s of local reader nondata-send await inside a winning reply's
predecode interval. It did not identify ACK-only work or a specific handler
as the entire cause. Earlier [COPY_DEBT_SERVICE evidence](COPY_DEBT_SERVICE_20260908.md)
measured4786 request flight-release calls taking7.808784s within8.160194s
ACK-handler elapsed; contained local-backpressure windows included246899us
and285275us flight release. These nested elapsed values are not additive
CPU seconds or a current-run attribution.

Origin fd32e60 used a whole-flight snapshot/rebuild to preserve exact
pre-release copy multiplicity; f4206d0 moved ownership and765683b corrected
byte-exact partial-copy attribution. The retained invariant does not require
rebuilding unrelated suffix. For normalized ACK ranges with H=max(end),
every released byte u<H; a flight starting at/above H cannot cover u or
affect its ambiguity. Candidate extracts starts<H in existing key/vector
order, keeps crossing flights intact, and leaves unrelated keys alone.
Crossing survivors rekeyed at H precede the old H bucket as before.
The all-keys-eligible predicate retains the old whole-map drain.

Runtime d999fea changes one production function,39 added/4 removed lines:
no credit, ACK semantics, qualification, copy debt, clock, native congestion,
controller, batching or threshold change. The H-boundary bucket can still
move during a merge; full-horizon ACKs still process all eligible records.
The counter excludes map comparisons and boundary-bucket movement, so this
is a precise workset reduction, not a constant-time or universal CPU claim.

| Checkpoint | Exact result |
| --- | --- |
| Test-only RED0683286 |1 control passes; intended work assertion fails66 versus3 after byte/proof/debt/metadata assertions; .00s |
| RED build |Warning-free1m12s |
| GREEN d999fea |546 focused checks pass1.28s; the two fragmentation cases now process2 records each |
| GREEN build |Warning-free1m08s; independent whole-function review passes |
| Ordinary candidate build |Warning-free3m29s; no diagnostic feature/events |

The real producer fixture preserves131072B Original plus14600B copy while
changing only suffix fragmentation. The separate boundary control verifies
receipt clipping, exact replacement identities, survivor ordering, immutable
assignment/deadline metadata, replay and full-horizon settlement. These are
ledger semantics, not a claim of writer admission or practical latency.

Existing [prepared-claim raw evidence](PREPARED_CLAIM_SERVICE_20260908.raw.tar.gz)
supports a conservative work lower bound without another capture:
among6367 applied ACKs,6343 have committed records starting at/above the
maximum ACK end seen so far, hence not positively ACKed by any recorded
ACK. Those records sum4470499 avoidable workset visits, mean702.136 across
all6367 ACKs and maximum2520. Root and an independent replay agree.
This does not reconstruct omitted sparse ranges, measure saved CPU, or
attribute the current ordinary gap. Its existing archive is linked rather
than duplicated in this pair's archive.

## Fixed pair and artifacts

Order was fresh parent first, then candidate after RED/GREEN and ordinary
build. Parent b783cd6 uses
`./.tmp/reflection/bin/prepared-stale-20260908/mptunnel`;
candidate d999fea uses
`./.tmp/reflection/bin/ack-support-20260908/mptunnel`.
Both endpoints use their cell's binary. No build overlaps either lab cell,
and there is no favorable repeat, new baseline or profile change.

The [unchanged ordinary profile](ADVISORY_OWNER_SERVICE_20260908.md#fixed-ordinary-profile-and-order)
is routed/mirrored500Mbps, upload70/20ms and return30/5ms delay/jitter;
five-second upload loss epochs[3,8,5,6,10,3,5,8]% and return
[1,2,.5,3,2,.5,1,2]%; upload10Mbps at15–25s; UDP outage30–33s;
40s offered load,85s runner guard and90s total probe boundary.
Same settings do not make independently randomized packet histories equal.

Full result directories are
`./.tmp/reflection/results/mixed-combined-up-ack-support-{control,candidate}-0908/`.
[ACK_SUPPORT_SERVICE_20260908.raw.tar.gz](ACK_SUPPORT_SERVICE_20260908.raw.tar.gz)
preserves both five-file directories plus six logs:
`ack-support-red-0908.log`, `ack-support-green-build-0908.log`,
`ack-support-green-0908.log`, `ack-support-ordinary-build-0908.log`,
and both cell run logs. Sixteen files total; no observer overlay is active.
The parent has48 service samples; candidate47. Both client logs and the
parent server log are empty. The candidate's sole server line is an
H3_NO_ERROR close warning; successful exact probe/runner outcomes do not
make that line evidence of a failed transfer.

## Completion, first service, gaps and full raw bins

| Metric | Parent b783cd6 | Candidate d999fea |
| --- | ---: | ---: |
| Local accepted = target-confirmed bytes |473104384 |393150464 |
| Complete / failed streams |1 /0 |1 /0 |
| Probe elapsed seconds |47.992259 |46.768581 |
| Exact whole-transfer Mbps |78.863 |67.250 |
| First confirmation seconds |.368903 |.378394 |
| Maximum closed confirmation gap seconds |3.060424 |5.497708 |
| First local write seconds |.091815 |.115526 |
| Maximum local-write gap seconds |4.396938 |5.665049 |
| Elapsed beyond40s offered-load boundary |7.992259 |6.768581 |
| Runner elapsed seconds / exit |48.391215 /0 |47.152978 /0 |
| Probe exit / errors |0 /none |0 /none |

Both use exact valid target-sink ACK accounting, not a lower bound; stderr
is empty. Neither cell reaches the observation guard. No missing terminal
silence is hidden by a censored maximum. The bytes differ because this is
a duration-based offered workload with blocking writes, not fixed-work
completion. Post40s elapsed is not a clean drain starting after all source
bytes have already been consumed.

All raw one-second confirmation bins follow in Mbps. Indices are zero-based;
each final bin is partial. They measure arriving confirmations, not physical
wire capacity. Parent bins542.875/541.226 do not establish capacity above
the500Mbps link; buffered confirmation release can cross interval boundaries.
The trimmed averages77.465/56.873Mbps are not substituted for the full series.

Parent,48 bins:

```text
 0: 2.097, 9.961, 6.816, 5.339, 3.146, 542.875, 60.389, 197.945, 121.111, 83.79
10: 252.235, 83.406, 3.382, 541.226, 174.5, 68.918, 0, 43.612, 0, 4.815
20: 0, 0, 28.735, 0, 0, 38.797, 6.579, 34.079, 0, 125.209
30: 85.399, 85.215, 40.155, 71.731, 73.92, 52.525, 118.585, 7.96, 45.117, 0
40: 0, 105.618, 44.209, 51.284, 51.713, 44.131, 150.138, 318.176
```

Candidate,47 bins:

```text
 0: 1.049, 23.069, 313.908, 48.375, 99.378, 113.481, 25.883, 71.066, 48.899, 128.31
10: 31.79, 112.198, 205.425, 105.67, 421.52, 94.047, 0, 0.953, 0, 0
20: 0, 0, 10.818, 0, 9.105, 55.242, 70.255, 51.905, 333.491, 25.882
30: 0, 0, 0, 0, 0, 2.858, 33.362, 3.862, 22.833, 6.151
40: 22.685, 8.869, 0, 167.484, 199.333, 110.757, 165.292
```

## Stage history and exact sampled holds

S/T/Rs/Rc denote client source bytes consumed / server ordered target-socket
bytes written / server target-reply bytes read / client local reply bytes
written. S is not request claim C; T is not raw Product receipt; Rc is not
by definition mux F. Ordinary logs contain no exact missing-prefix,
publication/write/decode or individual confirmation-gap timestamps.

L is one-based service.jsonl line. Elapsed is the runner's sample time;
generated Unix clocks identify each endpoint snapshot. Server/client snapshot
times are not atomic and cached samples can repeat across runner rows.
A constant sampled span is a lower bound, not the exact start/end of a stall.

| Cell; L / elapsed seconds | S | T | Rs | Rc |
| --- | ---: | ---: | ---: | ---: |
| Parent;6 /5.000588 |70662249 |3553385 |211 |211 |
| Candidate;6 /5.000644 |131739360 |109546656 |234 |155 |
| Parent;8 /7.000803 |146562443 |79453579 |314 |314 |
| Candidate;8 /7.001182 |145752064 |143863520 |332 |194 |
| Parent;11 /10.001100 |196911627 |146538443 |467 |425 |
| Candidate;11 /10.001487 |177102272 |146878176 |360 |234 |
| Parent;16 /15.001731 |335761939 |271340051 |719 |705 |
| Candidate;16 /15.002007 |291040736 |230615648 |514 |486 |
| Parent;23 /22.002479 |346800887 |279692023 |817 |747 |
| Candidate;20 /19.002428 |298391872 |234499808 |556 |528 |
| Candidate;26 /25.003100 |301608672 |234499808 |556 |556 |
| Parent;32 /31.249243 |388324747 |370799063 |1013 |901 |
| Parent;36 /35.389806 |431866615 |370799063 |1013 |971 |
| Candidate;32 /31.118739 |367394816 |300285952 |738 |738 |
| Candidate;36 /35.151590 |367394816 |300285952 |738 |738 |
| Parent;40 /39.390240 |445162923 |384368587 |1069 |1069 |
| Parent;42 /41.390435 |454176427 |402572659 |1125 |1069 |
| Candidate;43 /42.152383 |385733824 |344189536 |976 |934 |
| Candidate;44 /43.152496 |393150464 |354924544 |1018 |934 |
| Candidate;45 /44.152598 |393150464 |372202848 |1074 |948 |
| Parent;48 /47.391056 |473104384 |459582933 |1349 |1293 |
| Candidate;47 /46.152814 |393150464 |380103264 |1102 |1102 |

The candidate is substantially ahead in early target service at5–7s despite
lower delivered reply counts, nearly level in target bytes at10s, and behind
at15s and thereafter. A favorable early stage is not a full-stream benefit.

**Candidate's longest target hold:** L20–L26, server Unix
1788863964962→1788863970961,5999ms: T234499808 and Rs556 stay flat.
S adds3216800B and Rc catches528→556. Thus this is a real target-service
plateau during QoS, with overlapping return delay; not six seconds of newly
produced replies waiting behind a local reader. Rc528 itself is flat for
4000ms at client L19–L23,1788863963959→3967959, while T adds2531904B
and Rs adds28B. The stages must remain separate.

**Candidate's outage/recovery hold:** L32–L36, server
1788863976960→1788863980961,4001ms, T300285952 and Rs738 flat.
Client S367394816/Rc738 are also flat for4000ms; S−T is exactly64MiB.
There is no positive Rs−Rc backlog at these snapshots. Five zero bins30–34
lie between positive bins29 and35 and require a long confirmation silence,
consistent with the reported5.497708s maximum. The probe does not retain
that maximum's exact event endpoints, so this is not an invented exact join.
The individual blocking forward range/owner is unavailable in ordinary data.

**Parent contrasts:** longest T hold is4000ms on L32–L36, server
1788863107188→1788863111188, T370799063/Rs1013 flat, but S adds43541868B
and Rc advances901→971. QoS T279692023 is flat3000ms on L23–L26,
server1788863098188→3101188, while Rc catches747→817.
Separately, L40–L42 Rc1069 is flat2000ms at client
1788863115188→3117188 while T adds18204072B and Rs adds56B.
This is observed already-read return residence, not zero forward service.

**Candidate's remaining return delay:** early L8–L13 (about7–12s) has
T143863520→152841952, Rs332→388, Rc194→290; a substantial reply deficit
persists while both sides make progress. Late L43–L44 Rc934 is flat1000ms
at client1788863987959→3988959 while T adds10735008B and Rs adds42B.
By L45 Rs1074−Rc948=126B. Existing reply delay therefore survives, but
these sampled counters cannot name a particular Native/queue/ACK-handler
cause or apportion its duration.

Full source S is first sampled at parent L46/45.390844s and candidate
L44/43.152496s. Last samples still lack13521451/13047200 target bytes.
Exact sink confirmation settles both later; neither final target timing nor
a clean terminal FIN chain can be reconstructed from these snapshots.

## Native context, consumption and resource costs

Parent session10201246180648611467 has QUIC physical1 and TCP wire
0/1/2 physical2/3/4. Candidate session15887632789799543620 has QUIC
physical3 and TCP wire0/1/2 physical4/2/1. Do not equate physical numbers
across runs or map socket ports by management list ordinal.

Candidate client TCP ports45990/45992/46014 remain established against
server7443. Parent ports41100/41114/41116 are a different stable set.
The aggregate socket-consumption calculation is delta bytes_received minus
delta Recv-Q across the same three sockets; it includes encrypted protocol
and control bytes, not only reply payload. ss and management are sampled
sequentially, not at one atomic timestamp.

| Cell / rows | Aggregate client Recv-Q bytes | Cumulative bytes_received | Implied consumed bytes |
| --- | ---: | ---: | ---: |
| Parent L40→L42, reply hold |210244→173823 |4409396→4601074 |228099 |
| Parent L32→L36, T hold |695179→168004 |3836261→3869362 |560276 |
| Candidate L8→L13, early reply lag |1045202→136208 |2829391→3004927 |1084530 |
| Candidate L20→L26, QoS T hold |0→0 |3478831→3581746 |102915 |
| Candidate L32→L36, all-stage hold |0→0 |4808822→4811060 |2238 |
| Candidate L43→L44, reply hold |258506→299177 |5400771→5577128 |135686 |

This excludes total TCP read-consumption silence in the listed intervals,
not a particular held reply. Candidate peak aggregate Recv-Q1076070B at
L7 exceeds parent's796210B at L30; their work and phases differ.
Native socket backlog remains evidence of residence, not proof of the
modified ACK function's current cost.

During candidate L20–L26, same-epoch QUIC native ACKed bytes rise
216084878→220260840 despite flat T. During L32–L36, that QUIC counter
stays278416564 and Product data-level debt stays62973632B; TCP physical2
also retains3598944B Product debt. Aggregate TCP native bytes_acked still
rise62891583→70203971 while client Send-Q falls21498344→17253446B.
These counters separate transport progress from ordered service but do not
identify which retained assignment owns the missing prefix. Some management
Native snapshots are cached; Linux ss is the separate socket observation.
Do not label the entire64MiB S−T gap native in-flight or infer Q preference.

| Sampled cost | Parent client / server | Candidate client / server |
| --- | ---: | ---: |
| Initial RSS KiB |99008 /30152 |99028 /30212 |
| Peak RSS KiB |762024 /137296 |640040 /116640 |
| Final sampled RSS KiB |758436 /129556 |624764 /107048 |
| Maximum observed ps %CPU |89.1 /32.8 |130 /44.5 |
| Final observed ps %CPU |89.1 /20.3 |84 /18.4 |
| Last sample elapsed seconds |47.391056 |46.152814 |

ps %CPU is process-lifetime-average utilization at an observation, not
per-second CPU, total CPU time, or a named handler cost. Candidate's lower
RSS accompanies less completed work and a shorter observation. The higher
early utilization readings and lower final readings cannot establish either
saved or regressed per-ACK CPU without corresponding operation measurements.

Router HTB class1:10, first→last sample; bytes are directional:

| Cell / direction | First | Last | Sampled delta |
| --- | ---: | ---: | ---: |
| Parent upload eth1 |647785 |603300353 |602652568 |
| Candidate upload eth1 |705144 |536047191 |535342047 |
| Parent return eth0 |31307 |27383934 |27352627 |
| Candidate return eth0 |37136 |26425473 |26388337 |

Do not sum parent HTB/netem counters, normalize these unequal-work/windows
as a causal cost gain, or call their delta repair-only overhead. The sample
ends before final settlement and includes protocol/retransmission traffic.
The runner's pre-existing HTB quantum warnings do not change the successful
probe classification or prove a runtime cause.

## Practical stop and one discriminating question

The semantic/work RED→GREEN is preserved, but this one fixed-order ordinary
pair is adverse on whole-transfer rate and first/maximum service gaps.
Independent random loss realizations, different work totals and unobserved
exact prefix histories prevent causal attribution of the difference to
d999fea. No promotion, favorable rerun or changed profile follows.

The strongest next question selected by existing snapshots is the actual
missing forward prefix during candidate T234499808's QoS hold (and the
distinct T300285952 outage/recovery hold): was its Original still prepared,
already locally written but not decoded, or decoded but not released in
order? Exact same-range ownership/write/receipt evidence would distinguish
those boundaries. Native ACK progress or aggregate Product debt alone cannot.
This report does not authorize another capture or propose a new controller;
root must declare any next action. The observed early/late reply lag remains
a competing service boundary, not proof that the previous local-reader
mechanism explains the longer forward holds.
