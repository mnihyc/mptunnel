# Native claimant advisory-owner service

Recorded:2026-09-08. Category: bounded mechanism RED/GREEN and completed ordinary
pair. Candidate timing is adverse/mixed; promotion stops. This document does not
establish practical acceptance, promotion or release.
Read PERFORMANCE_METHOD_AND_LESSONS and CURRENT_CLOSURE_PLAN.

## Origin, model and executed RED

The [preceding ordinary and exact winning-reply evidence](COPY_DEBT_SERVICE_20260908.md)
retains an11.042148s ordinary confirmation gap and distinct diagnostic local
service holds. Diagnostic F737 includes1203ms after decode; F821 includes at
least2.722s of preceding reader-send-awaited elapsed during its current-frontier
hold. Other gaps are chiefly prepublication/predecode. Neither those joins nor
whole-capture claim elapsed measure Product-mutex contention separately.

The source-reachable mechanism is narrower: two advisory `owner.lock()` calls
inside `claim_prepared_request_data` can park the native writer's executor
thread while the Product actor retains its mutex. They occur before and after
detached Native observation, outside the final Native authority fence. The
final fenced acquisition already uses a prearmed nonblocking try-lock.
Origin9720e4b addressed the lock cycle, but an acyclic lock graph does not prove
responsive native input service; even an eventual Empty result may first wait.

The proposed correction changes only those two acquisitions to freshly
prearmed existing try-lock/Busy outcomes. The existing receiver-owned deferred
wait returns control to native arbitration. At the second cut, retry current
state after contention rather than retain advisory authority. Fresh arming must
not reuse a wait already notified by this claimant's own earlier unlock.
Registration/source/Ready/Native revalidation, final fence, U→Original
conservation, cancellation and all admission policy remain unchanged. Busy is
not negative path evidence or permission to prefer Backup. No new timer,
queue, rate, coalescing, threshold or claim quantum is proposed.

RED checkpoint **584b748** uses the actual prepared producer with Product held
by another thread at each cut. After semantic held-state checks and bounded
thread cleanup, the uncontended control passes and both acquisition cases plus
cancellation reach their intended blocking assertions: **1pass/3fail**,1.00s.
The warning-free RED build took1m12s. Independent fixture/model review passes.
Runtime checkpoint **e476308** changes only the two advisory acquisitions;
independent source audits pass. The GREEN build also takes1m12s, and542 focused
checks pass1.28s. The final Native fence, persistent-idle readiness, cancellation
and source/flight conservation remain intact. The warning-free normal optimized
build completes in3m31s. Its ordinary candidate result is recorded below;
component GREEN does not establish practical acceptance.

## Fixed ordinary profile and order

Order was fixed before execution: fresh ordinary9ea25e2 parent first, candidate
only after RED/GREEN/audit. The parent ran during test-only fixture preparation,
without a concurrent build. Both endpoints use its frozen ordinary executable:
`./.tmp/reflection/bin/copy-debt-20260908/mptunnel`. No diagnostic overlay is
active. The older9ea25e2 ordinary realization is context, not the substituted
control, and no favourable third run is planned.

Existing runner: `./.tmp/reflection/run.py mixed combined up`. Declared unchanged
environment: REFLECTION_ROUTED=1, REFLECTION_MIRROR_IMPAIRMENT=1,
REFLECTION_MANAGEMENT=1, REFLECTION_FIFO=0, REFLECTION_RETURN_RATE=500mbit;
REFLECTION_NO_JITTER/NO_LOSS/NO_QOS/NO_BLACKHOLE=0, with each name prefixed
REFLECTION_; REFLECTION_DIAG=0 and REFLECTION_NATIVE_TRACE=0. Endpoint-specific
binary overrides are unset. REFLECTION_BINARY selects
`/workspace/.tmp/reflection/bin/copy-debt-20260908/mptunnel` in the containers;
parent tag is `advisory-owner-parent-0908`. No profile/harness change.

Single routed/mirrored500Mbps link, upload70/20ms delay/jitter and return30/5ms.
Five-second upload loss epochs[3,8,5,6,10,3,5,8]% have mean6%; return epochs
[1,2,.5,3,2,.5,1,2]%. Upload10Mbps at15–25s; UDP outage30–33s;40s offered load.
The existing probe `--timeout 50` allows50s after the load deadline,90s total;
the runner guard remains85s. Random packet realizations are not identical.

Parent raw directory:
`./.tmp/reflection/results/mixed-combined-up-advisory-owner-parent-0908/`.
It retains probe.json, probe.err, client.log, server.log and all45 service.jsonl
rows. Runner log: `./.tmp/reflection/advisory-owner-parent-0908-run.log`.
Candidate source is e476308, frozen at
`./.tmp/reflection/bin/advisory-owner-20260908/mptunnel`, used at both endpoints.
Both complete raw directories and RED/GREEN/build logs are preserved in
[ADVISORY_OWNER_SERVICE_20260908.raw.tar.gz](ADVISORY_OWNER_SERVICE_20260908.raw.tar.gz).

## Completed parent probe

| Observation | Ordinary parent9ea25e2 |
| --- | ---: |
| Runner / probe status |0 /complete,ok |
| Confirmed / locally accepted bytes |355532800 /355532800 |
| Exact ACK accounting |Yes |
| Probe elapsed seconds |44.964483 |
| Runner elapsed seconds |45.175620 |
| Confirmed Mbps |63.256 |
| First confirmation seconds |.848948 |
| Maximum confirmation gap seconds |5.052667 |
| First local write seconds |.580035 |
| Maximum local write gap seconds |1.356326 |
| Probe errors / failed streams |0 /0 |

This is normal completion, not the earlier85s guard/probe race. Client and
probe-error logs are empty. The server records one H3_NO_ERROR remote-close
warning at07:26:03.768UTC; it is not evidence explaining the earlier holds.
The existing HTB quantum warning remains in the runner log; no shaping
parameter was changed in response to it.

## Exact stage identities, phases and sampled holds

Session6610547043620839176 and management flow1 are stable. Client PID268969,
server275002. Local source127.0.0.1:54168 targets127.0.0.1:10023; server flow
source is10.238.46.10:45764. Stable client carrier ports45764/45758/45772
connect to10.238.47.20:7443. Active client management TCP wire PathId/instance
pairs are1/4,2/2,0/1; QUIC is0/3. An initial configured TCP placeholder has no
physical instance; it is not a demonstrated replacement. Do not equate wire
PathId with runtime index or infer exact missing-range ownership from these
ordinary management rows.

S=client reliable.io.to_peer_bytes, consumed source rather than claimed C.
T=server reliable.io.from_peer_bytes, ordered target-socket acceptance rather
than raw receiver F. Rs=server reliable.io.to_peer_bytes, replies read from
the target; Rc=client reliable.io.from_peer_bytes, replies written locally
rather than mux F. The counters are bytes. Each endpoint's generated_unix_ms
is its observation clock; within-row commands are sequential, not atomic.

| Nominal sample seconds |S |T |Rs |Rc |
| --- | ---: | ---: | ---: | ---: |
|3.000340 |104357047 |40054231 |100 |100 |
|7.000770 |143506039 |119180183 |325 |204 |
|11.001220 |148111095 |142081783 |535 |204 |
|14.001524 |152430935 |150375319 |619 |217 |
|23.002478 |217484183 |150375319 |619 |619 |
|25.002682 |225622647 |158513783 |633 |633 |
|31.174014 |269707127 |257616215 |857 |703 |
|34.174327 |293071191 |257616215 |857 |731 |
|40.175012 |340736151 |292112151 |1151 |1095 |
|44.175473 |355532800 |335672343 |1361 |1361 |

| Held counter / value | Endpoint-generated Unix-ms interval | Sampled span |
| --- | --- | ---: |
|Rc204 |client1788852325538–1788852329536 |3.998s |
|Rc217 |client1788852330536–1788852333536 |3.000s |
|T150375319 |server1788852332539–1788852341540 |9.001s |
|Rs619 |server1788852331539–1788852341540 |10.001s |
|T257616215 /Rs857 |server1788852349539–1788852352540 |3.001s |
|S225622647 |client1788852342536–1788852343537 |1.001s |

During Rc204's7–11s hold, T advances22901600B and Rs210B: forward target and
reply production continue while replies are held on return. During the later
14–23s target plateau, Rc progresses217→619. Thus the9.001s target hold is
not a9s confirmation hold. S−T grows2055616→67108864B over that interval;
at23,24 and25s it is exactly64MiB. These aggregate separations do not locate
one flight or establish its native queue ownership.

The nominal30/31s rows repeat client timestamp1788852349537; their equal S
adds zero measured hold duration, not1.17s. S reaches the full accepted total
at43.175357s; its subsequent1.001s sampled hold is source exhaustion.
Nominal completion−40=4.964483s is not an exact EOF/drain timestamp. Last
server T335672343 at1788852362539 precedes the later exact355532800B probe
settlement; management did not sample the final19860457B target release.

## Native receive consumption and sampled costs

Native consumption below is the sum of bytes_received−Recv-Q on the three
stable client sockets, not logical response or application-goodput bytes.

| Nominal sample seconds |Sum Recv-Q B |Sum native consumed B |
| --- | ---: | ---: |
|3 |0 |325578 |
|7 |1058284 |1105133 |
|9 |1392294 |1147206 |
|11 |1373769 |1207467 |
|14 |1318896 |1346044 |
|20 |352007 |2368441 |
|23 |0 |2761260 |
|25 |0 |2812624 |
|31 |639842 |3626582 |
|34 |244489 |4028393 |
|40 |233983 |4624033 |
|44 |0 |5296684 |

The early Rc204 hold includes102334B native consumption at7–11s. The target
plateau14–23s includes1415216B; later31–34s includes401811B. Receive backlog
clears at23s, reappears after25s, then clears finally at44s. Final sampled client
TCP Send-Q is1554766B before the later exact probe completion; native queue
contents cannot be equated with unique outstanding Product payload.

At9s, server→client45772 has328573B Send-Q/notsent and cumulative
rwnd_limited3288ms. At44s its Send-Q is90B and cumulative rwnd_limited10364ms.
These are native backpressure observations, not exact attribution of the
winning missing reply. They do not measure time acquiring Product.

| Sampled cost | Parent |
| --- | ---: |
| Upload class byte delta |468449998 |
| Upload packets / drops |384908 /6859 |
| Return class byte delta |23167456 |
| Return packets / drops |106648 /1467 |
| Peak client RSS KiB |983028 at42.175257s |
| Final client RSS KiB |940944 |
| Peak / final server RSS KiB |100336 at44.175473s |
| Client lifetime CPU% peak / final |115 at43.175357s /113 |
| Server lifetime CPU% peak / final |35.2 at7.000770s /17.2 |

Class1:10 deltas use router eth1 upload and eth0 return over sampled
.000047–44.175473s, not summed qdisc levels or final transfer accounting.
They include control, framing, native retries and Product copies, not repair-only
cost. RSS is KiB; CPU is ps lifetime-average percentage, not interval utilization,
exclusive handler work or mutex waiting. Lower receive queue or RSS with less
accepted work would not itself demonstrate efficiency. Candidate comparison
must preserve unequal-work/duration and random-realization limits.

## All45 raw confirmation bins

Untrimmed one-second Mbps by index0–44; the final bin can be partial. These
measure buffered confirmation observations, not instantaneous link service.

```text
0–9:   .096,6.718,8.913,345.794,71.731,61.630,80.977,0,0,0
10–19: 0,53.669,0,0,0,76.974,0,68.682,15.729,30.837
20–29: 56.003,0,322.961,10.678,0,103.617,0,77.070,96.661,0
30–39: 93.153,63.203,49.549,99.090,0,124.256,111.769,49.903,66.651,21.496
40–44: 58.986,0,156.954,301.628,158.884
```

## Predeclared candidate discriminator and stop conditions

The completed parent retains substantial but different forward/return holds.
Its63.256Mbps and5.052667s maximum confirmation gap differ from the preceding
9ea25e2 realization, without any runtime correction between them. This is
explicit evidence against treating one random run as representative speed.

With focused GREEN/audit complete, compare the single declared candidate's normal
completion/equality, first-body/write timing, all raw bins, maximum and terminal
gaps, phase-specific T/Rs/Rc progress, source feeding, stable-socket receive
consumption, settlement and absolute RSS/wire/CPU context. Expected benefit is
responsive native service while Product is occupied, not changed native capacity.
Return improvement bought by starving Original claims, worse initial T, longer
settlement or excessive retry/resource cost is an adverse outcome.

No existing ordinary field identifies the two advisory lock waits separately.
Shorter Rc holds with stronger native consumption support improved local service
but cannot assign every timing change to this mechanism. A prepublication gap
may remain outside it. A smaller native queue with fewer arrivals is not proof
of better service; higher throughput with worse gaps is not promotion. Preserve
current-frontier delay versus total duplicate residence, and source S versus
claimed C. An adverse/ambiguous candidate stops promotion and selects one
bounded causal question, not a favourable third trial or new policy stack.

## Executed ordinary candidate and pair disposition

The single declared e476308 candidate completes normally, runner0, after the
warning-free3m31s optimized build. No diagnostic overlay or concurrent build
is active. Profile, endpoint roles and parent-first order above are unchanged.
Raw directory:
`./.tmp/reflection/results/mixed-combined-up-advisory-owner-candidate-0908/`;
runner log:`./.tmp/reflection/advisory-owner-candidate-0908-run.log`.
All50 service rows and all50 raw confirmation bins are retained. Client and
probe-error logs are empty; server has one H3_NO_ERROR remote-close warning
at07:39:09.901UTC, after the intervals examined below. It does not explain them.

| Exact probe observation | Parent9ea25e2 | Candidate e476308 |
| --- | ---: | ---: |
| Runner / probe status |0 /complete,ok |0 /complete,ok |
| Confirmed = locally accepted bytes |355532800 |409796608 |
| Exact ACK accounting / errors |Yes /0 |Yes /0 |
| Probe elapsed seconds |44.964483 |49.163739 |
| Runner elapsed seconds |45.175620 |50.267362 |
| Confirmed Mbps |63.256 |66.683 |
| First confirmation seconds |.848948 |.713189 |
| Maximum confirmation gap seconds |5.052667 |6.197568 |
| First local write seconds |.580035 |.122947 |
| Maximum local write gap seconds |1.356326 |5.462440 |

Both transfers settle exactly without the observation guard. The candidate
accepts54263808B more work and takes4.199256s longer; its higher average and
earlier first service do not cancel the worse confirmation/write gaps.
Nominal completion minus the40s load deadline is4.964483→9.163739s, not an
exact measured EOF-to-drain interval. No practical promotion or full matrix
follows this adverse/mixed pair, and no third ordinary trial is authorized.

### Candidate exact stages and opposite phases

Session10960302112944762454, management flow1, client PID270028 and server
PID276057 remain stable. Local source127.0.0.1:52132 targets127.0.0.1:10023;
server flow source is10.238.46.10:47060. The three stable client TCP ports
47060/47072/47052 connect to10.238.47.20:7443. Client management TCP wire
PathId/instance pairs are1/1,2/4,0/3, and QUIC0/2. These are the reported
identity namespaces, not an exact blocking-range assignment or a proof that
runtime indices equal port order. S/T/Rs/Rc retain the byte definitions above.

| Nominal sample seconds |S |T |Rs |Rc |
| --- | ---: | ---: | ---: | ---: |
|3.000429 |114372161 |50445025 |119 |119 |
|7.000836 |165472705 |145418689 |323 |197 |
|10.003306 |183065889 |162338081 |407 |225 |
|11.003421 |189119201 |173468097 |421 |239 |
|14.003884 |213486593 |173468097 |421 |295 |
|15.004091 |240576961 |173608233 |435 |421 |
|16.004200 |240717097 |173608233 |435 |435 |
|20.004636 |240717097 |173608233 |435 |435 |
|21.004747 |244526657 |177417793 |449 |435 |
|22.004850 |244532785 |177423921 |463 |449 |
|25.005151 |279158473 |212049609 |505 |449 |
|30.005724 |334436513 |296531049 |645 |589 |
|32.148414 |360280041 |307434025 |701 |645 |
|34.265304 |374542889 |307434025 |701 |701 |
|40.265910 |391649441 |324540577 |813 |813 |
|42.266119 |397809825 |370772233 |869 |841 |
|43.266229 |402593953 |371296521 |911 |841 |
|44.266332 |409796608 |372064521 |925 |841 |
|45.266444 |409796608 |372064521 |925 |841 |
|49.267186 |409796608 |408635265 |1093 |1093 |

| Held counter / value | Endpoint-generated Unix-ms interval | Sampled span |
| --- | --- | ---: |
|T173468097 /Rs421 |server1788853110573–1788853113572 |2.999s |
|T173608233 /Rs435 |server1788853114572–1788853119572 |5.000s |
|S240717097 |client1788853115572–1788853119573 |4.001s |
|Rc435 |client1788853115572–1788853120572 |5.000s |
|Rc449 |client1788853121572–1788853124573 |3.001s |
|T307434025 /Rs701 |server1788853131573–1788853133573 |2.000s |
|Rc841 |client1788853141572–1788853144572 |3.000s |
|T372064521 /Rs925 |server1788853143572–1788853144573 |1.001s |

These are endpoint-clock lower bounds between equal samples, not exact event
gap endpoints. In particular, the raw probe gives the maximum gap duration,
not its exact start/end timestamps; five zero bins16–20 and the corresponding
stage hold locate a consistent QoS-phase interval without manufacturing an
exact join to the6.197568s maximum.

During16–20s, S and T are both flat and S−T=67108864B. Rs=Rc=435 throughout.
All three client TCP receive queues are zero. This interval is forward ordered
target starvation with a full64MiB aggregate source/target separation, not
evidence that replies already read by the server remain undelivered locally.
It does not identify claimed C, a missing Product range, one queue containing
64MiB, or prepublication versus native/receiver-ordering delay. The parent has
a longer9.001s sampled T plateau14–23s but continues releasing older replies;
its smaller maximum confirmation gap is therefore not proof of better forward
target continuity.

The candidate also retains genuine return holds. At22–25s, T advances34625688B
and Rs42B while Rc449 stays fixed. At42–45s, T advances1292288B and Rs56B while
Rc841 stays fixed. These are distinct from the16–20s forward hold, and need not
have the same cause. Conversely, during its32–34s target plateau, Rc645→701
catches up. Neither reply gaps nor target gaps alone describe the full service.

Candidate early T is higher at3s and10s (50445025/162338081B versus parent
40054231/140306775B). By40s candidate T324540577 also exceeds parent292112151B,
but this is different accepted work and different realized loss/service. It
does not establish a causal improvement from nonblocking advisory acquisition.
Nominal33/34s repeat client generated_unix_ms1788853133573: equal S there adds
zero observed hold, not1.117s. S reaches its final total at44.266332s; its later
5s sampled plateau is exhaustion, not another active source stall. Last T is
1161343B below the exact settled total. Sequential/cached management snapshots
do not replace final probe accounting even when nominal sample time is later
than the probe's separately measured elapsed duration.

### Candidate native consumption and absolute cost comparison

| Nominal candidate seconds |Sum client TCP Recv-Q B |Sum native consumed B |
| --- | ---: | ---: |
|3 |44331 |666304 |
|6 |962004 |1528909 |
|7 |948335 |1665054 |
|11 |426776 |2382673 |
|15 |0 |2964653 |
|16 |0 |2967017 |
|20 |0 |2970004 |
|22 |0 |2977512 |
|25 |0 |3010180 |
|30 |225925 |3466569 |
|34 |58745 |3946153 |
|40 |0 |4179939 |
|42 |465991 |4464279 |
|45 |19926 |4920318 |
|49 |0 |5333617 |

The same stable-socket bytes_received−Recv-Q proxy shows717619B consumed7–11s
versus parent102334B, with candidate Rc197→239 rather than a constant parent
Rc204. Peak Recv-Q falls1392294→962004B and first clearance moves23→15s. This
supports different local service in that phase, not a measured mutex-wait
reduction: arrivals, byte ordering and work differ. At16–20s only2987B more
native input is consumed, with zero queued input; no receive-backlog drain is
available to explain that forward hold. At22–25s,32668B is consumed despite
the Rc449 hold. Later42–45s,456039B is consumed while Rc841 remains fixed and
Recv-Q drains465991→19926B. Native consumption is not necessarily the winning
reply or even reply payload; control and other Product frames share sockets.

During16–20s, socket47060 Send-Q moves10046892→9945628B and notsent
9945628→9915808B; native bytes_acked advances101264B. Across all three client
TCP sockets, bytes_acked advances156932B. Management QUIC native_delivery
acked_bytes advances199454531→199986131B while reported data-level flight
23441416→23067392B; TCP1/1 data-level flight remains2737712B. Thus some native
and Product accounting progresses, but target service does not. These distinct
sampling clocks and aggregate fields do not reveal the exact head owner,
repair eligibility, available target capacity or queue contents.

At43/44s all three sampled server TCP Send-Q values are zero, while client
Recv-Q and the Rc841 hold persist. At49s client Recv-Q is zero but summed
client Send-Q remains6212384B. This may contain control/redundant native work;
it is not evidence of that many unique unconfirmed bytes after exact settlement.

| Sampled cost | Parent | Candidate |
| --- | ---: | ---: |
| Upload class byte delta |468449998 |564052439 |
| Upload packets / drops |384908 /6859 |455652 /7585 |
| Return class byte delta |23167456 |25094026 |
| Return packets / drops |106648 /1467 |125666 /1574 |
| Peak client RSS KiB |983028 |897892 |
| Final client RSS KiB |940944 |685028 |
| Peak server RSS KiB |100336 |119464 |
| Final server RSS KiB |100336 |117436 |
| Client lifetime CPU% peak / final |115 /113 |131 /93.3 |
| Server lifetime CPU% peak / final |35.2 /17.2 |41.5 /17.6 |

Candidate RSS peaks occur at45.266444s client and33.148537s server; CPU peaks
at7.000836s client and6.000739s server. Candidate class deltas span nominal
.000065–49.267186s, versus the parent's .000047–44.175473s. At nominal initial
candidate collection the sockets already contain3033470B Send-Q while the
cached management S remains zero, illustrating why these sequential samples
are not atomic start/final accounting. Costs retain the class direction,
framing/control/copy/native-retry, lifetime-CPU and unequal-work caveats above.
Lower client RSS/final CPU and higher server RSS/early CPU are mixed resource
observations, not a normalized efficiency or causal contention result.

### All50 candidate raw confirmation bins

Untrimmed one-second Mbps, including initial/final bins and every zero. Buffered
confirmation release is not a link-service rate or exact forward timing.

```text
0–9:   1.145,15.727,328.204,120.254,49.807,51.476,110.721,0,144.179,76.878
10–19: 42.039,65.248,43.612,67.205,182.733,89.565,0,0,0,0
20–29: 0,8.461,0,0,0,299.238,129.328,92.254,129.608,0
30–39: 117.345,137.536,69.781,84.934,2.386,0,97.098,28.028,0,2.429
40–49: 9.201,10.862,0,0,0,375.429,14.156,.428,235.611,45.467
```

### Boundary selected for the next causal question

The narrow mechanism RED/GREEN remains valid; practical composition remains
adverse/mixed. The most discriminating new ordinary boundary is the candidate's
15–20s ordered-target hold after returned bytes have caught up, not an assumed
persistent local reply backlog throughout its worst gap. The finite question is:
for the actual blocking request prefix in that interval, had its winning bytes
not yet been published/served natively, or had they reached the server but not
advanced ordered target delivery? Exact successful range/owner publication,
native handoff/receipt and server frontier evidence would distinguish those
cases; the current ordinary snapshots cannot. Prompt server receipt would
falsify a pre-receipt-only explanation; an already-read held reply would select
the return boundary instead, as the separate22–25s and42–45s intervals do.

This selection does not authorize a fresh capture, new policy or another
ordinary repeat. Reuse existing exact evidence first. No observed paired
aggregate identifies time blocked at either advisory acquisition, and these
random realizations cannot assign all improvements or regressions to the mutex
change. Earlier long local return holds and prepublication/native cases remain
opposite conditions; they are not waived by this candidate's higher Mbps.
