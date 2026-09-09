# Native TCP concurrency context

Date: 2026-09-09. Category: bounded diagnostic, not a Product correction or
performance acceptance. No MPP source, RFC, controller or limit was changed.

## Question and forecast

The [direct-echo companion](DIRECT_ECHO_CONTEXT_20260909.md) found that mixed
MPP load also delays independent traffic bypassing the tunnel: loaded direct
echo median was250.033ms, versus101.300ms beside QUIC-only. Earlier source and
winning-byte joins did not establish missing QUIC priority or a shared MPP
stream writer. The mixed case has three TCP controllers plus QUIC, whereas the
raw reference had one TCP body.

The next question was therefore whether independent native TCP concurrency can
reproduce a substantial common penalty without MPP. The predeclared comparison
was one then three simultaneous raw bodies, each with the same target/body,
one sparse direct echo,40s common clock and unchanged healthy500/500Mbps cut.
Higher latency under three progressing raw sockets would support native
competition as a material context; prompt three-socket service would weaken
count alone and retain MPP's wire work/assignment pattern as a necessary owner.
Neither outcome would establish all mixed delay, inevitability or a safe policy.
This was an information forecast, not a predicted speed improvement.

## Workload, sources and reproduction

The42-line `./.tmp/reflection/raw_controller_context.py` imports the existing
`bulk_worker` and `interactive_tcp_worker` from `./lab/mixed_workload_probe.py`.
It creates one or three body threads with independent result dictionaries and
ready events, one shared monotonic start/deadline and one shared echo-ready
event. The first successful echo releases all waiting body workers. No new
transport, socket setting or receive loop is implemented by the helper.

Both cells use direct HTTP `10.238.47.20:8080/large.bin`, declared8GiB,64KiB
reads and40s load with50s worker timeout. Direct echo is persistent TCP to
port10022,64B requests,500ms interval and3s response timeout. These are the
existing observation conventions, not Product thresholds. Monotonic and Unix
anchors, all body results and every echo attempt are retained. The observed
Python listener on8080 serves three simultaneously progressing HTTP bodies;
this excludes serialized HTTP body service in this pair without inferring the
listener's HTTP implementation from its process name.

The driver was executed inline, importing the existing `shape`, `command` and
`stop_products` helpers from `./.tmp/reflection/run.py`; there is no separate
driver source file. The driver log records stdout, not the tool-side shaping
stderr. It stopped owned MPP/Xray/H2 processes, ran counts1 then3,
initialized both routed directions and retained the same5s epochs0..7 with
healthy flags disabling loss, jitter, QoS and blackhole. It sampled class/qdisc
and process state each second, plus ESTABLISHED8080 sockets at both endpoints
and10022 echo sockets separately. The existing85s outer observation guard was
not reached. `run.py` and `shape.sh` were unchanged. No build or competing lab
ran during the cells.

Reproduction inputs and raw evidence are retained in
[the raw archive](RAW_CONTROLLER_CONTEXT_20260909.raw.tar.gz): the helper,
canonical worker/runner/shaper sources, driver transcript and both result sets.
The79,501-byte archive contains13 regular files. Gzip integrity, complete member
listing and decompressed comparison against every retained input pass.
The source result paths are:

- `./.tmp/reflection/raw-controller-context-run-0909.log`
- `./.tmp/reflection/results/raw-1-controller-context-0909/`
- `./.tmp/reflection/results/raw-3-controller-context-0909/`

Each result directory has `probe.jsonl`, empty `probe.err` and41
`service.jsonl` rows. Both driver exit statuses are0. The raw JSON retains all
160 echo attempts and160 per-body one-second bins; none is trimmed or omitted.

## Complete received service

All four bodies report HTTP200 and content length8589934592, exactly one
request, zero completed requests and one intentionally duration-censored
partial request. There is no early EOF/reopen, failed worker or missing body
counter. These are40s useful-service observations, not four completed8GiB
transfers. Both echo streams complete80/80 attempts,5120B each direction, with
no disconnect, mismatch, timeout or other error.

| Whole observation | One body | Three bodies |
|---|---:|---:|
| Exact received body bytes | 2254307216 | 2260280216 |
| Common elapsed s | 40.000582579 | 40.001099156 |
| Aggregate useful Mbps | 450.854877 | 452.043622 |
| Echo median / p95 / max ms | 103.221 / 125.942 / 294.997 | 214.376 / 235.202 / 305.235 |
| Largest successful-echo completion spacing s | 0.690361 | 0.702499 |

Aggregate goodput uses the sum of exact received bytes divided by the helper's
common elapsed time, not a sum of per-worker rates with different denominators.
The3-body distribution and each body's observed gap remain explicit:

| Cell / body | Received B | Own elapsed s | Mbps using common elapsed | First body s | Maximum read gap s |
|---|---:|---:|---:|---:|---:|
| One /0 | 2254307216 | 40.000431220 | 450.854877 | 0.404478 | 0.100232 |
| Three /0 | 778415840 | 40.000910324 | 155.678890 | 0.405639 | 0.100169 |
| Three /1 | 673215744 | 40.000314911 | 134.639449 | 0.408381 | 0.100272 |
| Three /2 | 808648632 | 40.000540971 | 161.725282 | 0.408892 | 0.138641 |

A per-flow gap is not an aggregate three-flow gap: another body can progress
while one waits. No exact subsecond aggregate read-gap claim is reconstructed
from these summaries.

### Timing across the complete observation

Every row includes ten aligned, untrimmed one-second aggregate body bins.
Echo attempts belong to the interval containing their start; all20 attempts
in every cell/interval succeed. Quantiles use the canonical nearest-order
statistic. Full-precision attempts and raw bins remain in the archive.

| Interval s | One-body Mbps | Three-body Mbps | One echo p50 / p95 / max ms | Three echo p50 / p95 / max ms |
|---|---:|---:|---:|---:|
| 0–10 | 417.794 | 426.702 | 103.746 / 290.429 / 294.997 | 214.814 / 302.564 / 305.235 |
| 10–20 | 460.904 | 464.046 | 103.173 / 117.982 / 119.013 | 214.125 / 233.608 / 238.925 |
| 20–30 | 452.068 | 459.229 | 102.496 / 125.942 / 125.986 | 214.475 / 232.599 / 242.736 |
| 30–40 | 472.633 | 458.201 | 103.572 / 117.822 / 126.371 | 216.356 / 233.410 / 235.202 |

Startup spikes remain included. The median separation persists in every
chronology interval; it is not solely a whole-run average or startup effect.

Canonical bins are rounded to0.001Mbps and limited to indices0..39. A recv
started before the deadline can complete just after40s, adding exact body
bytes outside that bin range. Reconstructed bin bytes are57841B and57341B below
the exact one/three-body totals, approximately0.0026%; the archive preserves
both. Do not force the rounded bins to conserve exact whole bytes or silently
replace the common elapsed denominator with40.

## Actual sockets, profiles and cost

Both endpoints have exactly one or three stable8080 tuples in40 samples.
The server has none in the initial sample; the client has none in the final
sample. Sequential endpoint collection therefore has different boundary
coverage. Every one of39 adjacent live sample
intervals shows positive progress on every socket: server `bytes_acked` and
client `bytes_received`. All show `bbr`, but `ss` does not establish its numeric
version. Echo10022 sockets are separate and excluded from body-controller
counts. No MPP/Xray/H2 process appears in the sampled process state.

All82 profiles retain HTB rate/ceil62500000B/s in both directions,
burst/cburst65536B, DOWN30ms/UP70ms, jitter0 and netem limit8192, without a loss
clause. Every sampled HTB/netem drop counter is0. Service elapsed spans
0.000032–40.004406s and0.000028–40.004707s. Changing healthy epochs does not
change these effective values.

| Whole-run sampled cost | One body | Three bodies |
|---|---:|---:|
| DOWN class byte / packet-counter delta | 2363299170 /1561037 | 2376653110 /1569863 |
| UP class byte / packet-counter delta | 2646204 /40036 | 7631190 /115659 |
| DOWN backlog median / peak B,41 samples | 1998480 /8299748 | 8820564 /14510176 |
| UP backlog median / peak B,41 samples | 4752 /6400 | 13926 /16038 |
| Client worker peak/last RSS KiB | 15788 | 16228 |
| Client worker peak / last lifetime CPU % | 10.6 /3.5 | 15.2 /9.7 |
| HTTP server peak/last RSS KiB | 7992 | 8336 |

Final sampled queue backlogs are0. The HTTP server's sampled process-lifetime
CPU rounds to0% in both cells; the echo service is3732KiB/0%, and the inactive
sink8680KiB/0.1%. Those long-lived lifetime averages do not prove zero server
work or exclude host scheduling. RSS is a sampled finite value, not a leak
proof. Class traffic includes native overhead and acknowledgements, not just
HTTP body bytes. Packet counters are offload-sensitive. HTB and its netem child
describe the same queue and must not be added.

Native aggregate sender state, sample indices1..39 only, supplies context:

| Aggregate server8080 median / peak B | One body | Three bodies |
|---|---:|---:|
| Send-Q | 27891376 /33063632 | 83617656 /95618680 |
| notsent | 21867696 /31095800 | 72398552 /83482992 |
| unacked × MSS | 6103320 /17639536 | 12881408 /18036288 |
| cwnd × MSS | 12432528 /17639536 | 13307120 /18036288 |

These are not additions to qdisc backlog or an echo's exact queue position.
The segment-based flight proxy is not exact payload ownership. Pooled server
body SRTT median is101.912ms (40 observations) versus214.248ms (120); range
100.054–298.639 versus100.086–301.938ms. Both retain minimum RTT near100.024ms.
Server echo SRTT median106.352→204.182ms and client echo106.656→203.173ms also
support shared service delay. Client body SRTT near100ms does not contradict
this: its acknowledged send bytes remain72 for the GET while received body
bytes grow, so that sender-side value is not current download RTT.
`bytes_retrans` is absent, not an independently measured zero-retransmission
result. No queue/C calculation is substituted for measured per-echo latency.

## Outcome and decision

The forecast is supported: three independently progressing raw TCP bodies
produce a sustained~111ms median echo increase without MPP, while aggregate
useful rate changes by only~0.26%. Common native/shared-cut or host competition
is therefore a real contributor worth retaining in the mixed-mode model; a
missing MPP priority or reader lock is not required to produce a large penalty.

This does **not** prove controller count alone explains the entire mixed case.
Three raw TCP streams are not three MPP TCP carriers plus QUIC: their native
work, feedback, packet scheduling, application/host work and allocation differ.
The remaining mixed result must still be judged against competitive useful
service and timing. One ordered diagnostic pair is not a universal limit,
and presently queued delay is not proof that all earlier queue formation was
unavoidable. No BBR version diagnosis, coupled-controller model, queue cap,
protocol preference, RFC change or production policy is selected here.
