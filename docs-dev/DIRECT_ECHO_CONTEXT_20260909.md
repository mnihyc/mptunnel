# Direct echo beside mixed and QUIC-only load — 2026-09-09

Status: **completed context discriminator, not a runtime fix or acceptance**.
During actual mixed load the direct bypass echo reaches250.033/404.408/518.846ms
median/p95/maximum, compared with101.300/164.717/242.530ms beside QUIC-only.
Unloaded margins return to approximately100ms. MPP echoes follow the same broad
context:268.501/443.952/548.544ms mixed versus106.692/159.501/292.816ms QUIC-only.
This supports a material common path/host contribution, not an exact subtractable
delay component or proof that all remaining tunnel cost is external.

## Predeclared question and provenance

Existing winning-byte traces locate substantial response residence after native
handoff. The previous mixed/QUIC-only queue and RTT differences have similar
scale, but aggregate backlog cannot identify an individual delayed byte.
CURRENT_CLOSURE_PLAN therefore selects one direct-echo companion pair before
any further model or controller change. Elevated bypass latency under mixed load
supports common service; bypass remaining near its unloaded latency while MPP
stalls would instead favor tunnel-native/reader service. Route/class differences,
weak reproduction or inconsistent clocks would leave attribution unresolved.
No performance gain is forecast from this observation.

Both cells reuse the existing frozen
`./.tmp/reflection/bin/echo-membership-20260909/mptunnel`, with the intervention
flag **unset in both** and ordinary policy. The thin
`./.tmp/reflection/direct_echo_context.py` imports the existing
`interactive_tcp_worker`; it does not introduce a new echo implementation.
One persistent64B direct TCP request is scheduled every500ms, with the existing
3s timeout, for50s. The direct connection bypasses SOCKS/MPP and addresses the
same remote echo service across the shared cut. The companion supplies about
128B/s in each direction before native overhead; it is not another bulk flow.

The foreground runs the unchanged40s HTTP-backlog plus MPP-echo workload. Mixed
runs first, QUIC-only second. There is no build, runtime overlay, native-controller
change, priority adjustment or queue-size change in this transaction. Parent
owns execution and the next preregistered native-controller-count comparison;
this report selects no production policy.

Artifacts under `./.tmp/reflection/`:

- `direct-echo-{mixed,quic}-context-0909.jsonl`: complete companion start records
  and all200 actual direct attempts with their clock anchors.
- `results/{mixed,quic}-combined-down-direct-echo-context-0909/`: all five normal
  result files per cell, including every160 MPP attempts and80 body bins.
- `direct-echo-context-run-0909.log`: driver launch/completion timeline.
- `direct_echo_context.py`: exact thin reuse helper.

[Raw archive](DIRECT_ECHO_CONTEXT_20260909.raw.tar.gz) retains22 regular files:
the ten normal results, two companions, helper, cost JSON, driver log, existing
observer patch/build log, runner/shaper and three configurations. Size299484B;
gzip integrity, complete member listing and decompressed byte comparisons pass.
All raw attempt histories remain linked rather than repeated inline. Foreground runners exit0
after41.009316/41.004765s; both companions exit0. Existing HTB quantum warnings
remain. No ordinary-build performance or public baseline claim follows.

## Clock alignment and effective conditions

Driver launch is not foreground start: runner setup precedes the probe. The
foreground clock is aligned by its first `echo_request_read` Unix-ms event minus
the corresponding first probe attempt's start offset. Companion Unix/monotonic
anchors are retained independently. This yields:

| Companion-relative boundary s | Mixed | QUIC-only |
|---|---:|---:|
| Driver foreground launch | 3.875511 | 3.898903 |
| Estimated actual foreground start | 5.788033 | 5.877988 |
| Foreground40s end | 45.788033 | 45.877988 |
| Companion end | 50.000186 | 50.000173 |

First request-derived foreground Unix-ms origins are1788948993862.111 and
1788949044066.398. Checking all80 request events gives apparent origin spreads
−1.322…+1.491ms and−.778…+1.361ms relative to those anchors. This supports the
phase alignment but is not a proof that initial local handoff has zero delay.
Millisecond event quantization and independently sampled clock anchors remain;
do not turn these timestamps into sub-millisecond stage measurements.

All82 effective service rows verify500Mbps per direction,30ms DOWN/70ms UP,
zero jitter and configured loss, no blackhole, HTB burst/cburst65536 and netem
limit8192. Every class/qdisc drop delta is zero. There is no15/25s QoS transition.
The native socket dump reports three BBR TCP carriers under mixed and none under
QUIC-only; controller name `bbr` does not establish a numeric BBR version.
The companion's direct connection is not included in the carrier-port socket
dump. Multiple independent controllers sharing a cut are not equivalent to a
single raw bulk TCP baseline.

## Complete useful workload and timing

Both foregrounds receive one HTTP200 response from an8GiB object and deliberately
stop it at40s: zero complete and one duration-partial body, not completed8GiB.
Both `probe.err` files are empty. All200 direct and160 MPP attempts succeed;
none is removed from a percentile or converted into an assumed failure.

| Foreground outcome | Mixed | QUIC-only |
|---|---:|---:|
| Received body B /duration s | 1989302104 /40.000278 | 2122312086 /40.004816 |
| Whole useful goodput Mbps | 397.857658 | 424.411319 |
| First body s | .583128 | .515678 |
| Maximum body-read gap s | .273255 | .100524 |
| Gap start → end s | .583128→.856383 | .515678→.616202 |
| Body bytes before /after gap | 58192 /123728 | 12000 /48000 |
| MPP echo success /attempts | 80 /80 | 80 /80 |
| MPP echo p50 /p95 /max ms | 268.501 /443.952 /548.544 | 106.692 /159.501 /292.816 |
| MPP maximum completion-spacing s | .720160 | .674249 |
| Direct echo success /attempts, full50s | 100 /100 | 100 /100 |
| Direct full50s p50 /p95 /max ms | 211.073 /391.370 /518.846 | 101.153 /164.717 /429.421 |
| Direct full50s maximum completion-spacing s | .713630 | .829088 |

Each MPP echo transfers5120B per direction; each direct companion6400B. Completion
spacing includes both schedule and exchange time and is not another body-read
gap. The full50s direct summaries mix unloaded, setup and loaded periods; they
are not substituted for aligned loaded latency.

| Direct phase by attempt start | n per cell | Mixed p50 /p95 /max ms | QUIC-only p50 /p95 /max ms |
|---|---:|---|---|
| Quiet pre-launch | 8 | 100.355 /100.469 /100.469 | 100.308 /100.380 /100.380 |
| Runner setup before actual load | 4 | 100.229 /100.272 /100.272 | 100.417 /429.421 /429.421 |
| Actual foreground0–40s | 80 | 250.033 /404.408 /518.846 | 101.300 /164.717 /242.530 |
| Foreground5–40s | 70 | 252.946 /445.995 /518.846 | 101.352 /145.584 /198.403 |
| After foreground through companion end | 8 | 100.271 /100.302 /100.302 | 100.269 /100.328 /100.328 |

The QUIC-context429.421ms direct outlier (attempt9, companion4.505047→4.934467s)
occurs during runner setup before foreground start. It is retained in the full
history and setup category, not incorrectly attributed to sustained QUIC load
or discarded as noise. The report does not claim its precise cause.

| Foreground slice | Body Mbps mixed /QUIC | Direct p50/p95 ms mixed /QUIC | MPP p50/p95 ms mixed /QUIC |
|---|---|---|---|
| 0–5s | 285.919 /334.951 | 111.094/301.326 /101.130/242.530 | 258.151/424.893 /107.643/292.816 |
| 5–15s | 406.085 /441.339 | 250.033/340.213 /101.299/164.717 | 274.237/401.616 /106.476/149.761 |
| 15–25s | 423.586 /431.712 | 211.073/367.009 /101.153/145.584 | 235.031/367.348 /111.114/167.890 |
| 25–40s | 412.535 /438.200 | 293.941/518.632 /101.783/142.062 | 304.818/525.227 /105.144/149.050 |

Each slice includes10/20/20/30 attempts per echo series. Quantiles reproduce the
existing rounded `(n−1)p` convention with ties-to-even; phases use start time.
All raw body bins are retained, not the trimmed423.212/438.655Mbps fields.
Unmatched direct/MPP quantiles cannot be subtracted as exclusive stage delays.

## Exact echo winners and simultaneous slow interval

Each cell has80 unique Original claims covering[0,5120),80 source enqueues,
80 advancing mux applications and80 local deliveries totaling5120B. All winning
ranges join exact carrier/physical identity and logical extent to a positive
native handoff and authenticated decode. Every winning response is an Original.
Ordinary mixed naturally adds QUIC to this echo at client observer1.182s; there
is no forced-member marker. This realized membership differs from some earlier
mixed captures and must not be silently treated as identical.

| Winning carrier /handoff→decode ms | Mixed | QUIC-only |
|---|---|---|
| TCP winners, n /p50 /p95 /max | 21 /200 /339 /476 | none |
| QUIC winners, n /p50 /p95 /max | 59 /181 /361 /452 | 80 /32 /81 /162 |
| Source enqueue→Original claim maximum | 33ms | 33ms |
| Original claim→positive native handoff maximum | 2ms | 2ms |
| Advancing mux→local delivery maximum | 1ms | 1ms |

Mixed TCP winner identity is server wire1/physical3/attachment1, client
index0/physical3/attachment0. Mixed QUIC is server wire0/physical1/attachment2,
client index0/physical1/attachment1, native requeststream12. QUIC-only uses server
wire0/physical1/attachment1, client index0/physical1/attachment0, requeststream4.
Wire, local index and attachment namespaces are not interchangeable.

Mixed has85 decodes/mux applications:80 advance, five add no new bytes. There
are83 logged positive handoffs (24 TCP,59 QUIC), not85. Two nonadvancing QUIC
arrivals for[128,192) and[2816,2880) lack matching positive-write events in this
observer. All winning joins remain complete, but complete losing-copy/native
write conservation is **not** established; no new defect follows from this
coverage limit. QUIC-only has80 writes/decodes/mux applications without duplicates.

The largest mixed MPP echo, attempt61, spans foreground30.535761→31.084305s
(548.544ms). Its TCP Original[3904,3968) is handed off at1788949024469ms and
decoded1788949024945ms,476ms later. Direct attempt73 overlaps it at foreground
approximately30.760318→31.279164s (518.846ms); direct attempts71/72 also take
514.898/518.632ms. This is simultaneously slow bypass service, not solely an
aggregate median difference. Nearby30/31/32s samples show28.843/18.184/16.175MB
DOWN backlog and429.748/514.824/421.238ms server QUIC RTT. The samples do not
locate either request's exact queue position.

No aggregate perf counter rows are enabled in this frozen echo observer. Bulk
repair/Original totals from other captures must not be imported as this pair's
accounting. Handoff is native acceptance, not packet departure; direct echo also
includes its native TCP and shared target host service.

## Common native, queue and resource cost

Class windows span40.009101/40.004567s, distinct from foreground body time and
the50s companion window. All41 samples per role/cell retain one QUIC physical
instance1 and stable role-local native epoch/session.

| Sampled cost | Mixed | QUIC-only |
|---|---:|---:|
| DOWN /UP class byte deltas | 2398129824 /91793644 | 2251062563 /36081370 |
| DOWN /UP packet-counter deltas | 1767415 /810572 | 1506814 /336454 |
| DOWN backlog p50 /p95 /max B | 10515672 /21444214 /28842638 | 1888416 /4296744 /6019456 |
| UP backlog p50 /p95 /max B | 164013 /243820 /265465 | 71356 /84941 /88694 |
| Client QUIC RTT p50 /p95 /max ms | 249.014 /419.205 /516.892 | 101.584 /163.170 /206.860 |
| Server QUIC RTT p50 /p95 /max ms | 239.853 /421.238 /514.824 | 101.240 /162.624 /211.374 |
| Server QUIC flight p50 /p95 /max B | 7104636 /17106809 /21206295 | 5728140 /9870696 /14549040 |
| Client peak /final RSS KiB | 91096 /91096 | 36928 /36928 |
| Server peak /final RSS KiB | 348632 /348632 | 291124 /288812 |
| Client peak /final ps CPU % | 122 /122 | 99.4 /98.9 |
| Server peak /final ps CPU % | 201 /201 | 154 /154 |

HTB parent and netem child describe overlapping queue occupancy; do not add
them. Class traffic includes the companion, native acknowledgements, recovery
and protocol work, not exclusively useful body bytes. Packet counters are
offload-sensitive. Queues are sampled rather than continuous maxima; CPU is
process-lifetime multicore use, and RSS is not proof of leak freedom. None of
these aggregate values defines the exact delay of a particular echo.

Client Broken-pipe at10:17:13.864/10:18:04.074UTC coincides with foreground body
cancellation; server RemoteClosed follows13.938/04.146 in those minutes and
H3_NO_ERROR at10:17:14.893/10:18:05.121. All attempts remain successful. These
retained teardown messages are not new workload failures.

## Outcome and next boundary

The information forecast is supported: direct traffic which does not traverse
MPP develops a substantial loaded delay specifically beside mixed traffic,
returns to approximately100ms after load, and stays much closer to100ms beside
QUIC-only despite greater useful foreground throughput. This rules against an
exclusively MPP internal response-writer/reader explanation for all of the excess.
It does not prove all mixed overhead external, inevitable or immune to better
allocation, nor identify network FIFO versus shared host scheduling exactly.

Removing TCP changes controller count, data/control service and allocation
together. One context pair cannot label that competition a Product defect or
establish a universal optimum. Preserve the known recovery model and avoid
another scalar, queue-cap or protocol-preference change based on this result.
Parent's next bounded native-controller-count comparator is diagnostic, not a
new policy recommendation. No runtime change, public performance update or
release acceptance is made here.
