# TCP echo delay: exact protected-byte initial-send frontier

Recorded: 2026-09-10. Feature-only observation on ordinary policy `ba56290`.
**A material native unsent-queue residence is established for exact echoes,
including the slowest request. No queue limit or performance fix is selected.**
For that request, the kernel's initial-send frontier remains below the protected
frame's end for at least 1.352786 seconds of its 1.497461-second
elapsed time. This is an exact same-frame timing bound, not aggregate Q/C.
It does not claim every echo has the same cause or that all delay is removable.

## Question, instrument and proof boundary

The preceding ordinary TCP-only tests repeatedly show roughly 900 ms loaded
echo latency, compared with roughly 206 ms for a simultaneous direct echo and
213 ms native RTT. The direct capture has only limited early overlap; it does
not establish a full-run stage. Approximately 50 MB aggregate NOTSENT motivates
this discriminator but cannot itself locate a particular echo.

Root ran one declared TCP-only healthy DOWN capture using the existing 40-second
bulk-plus-echo workload, unchanged 500/500 Mbps cut and 30/70 ms delay, without
loss, jitter, QoS or outage. The feature build took 69 seconds, retaining only
the existing unused `apply_and_write_ready_stream_data_batch` warning. There was
no runtime scheduling, controller, queue-size, limit or lifecycle intervention.
Inputs are the five files under
`./.tmp/reflection/results/tcp-combined-down-tcp-wire-frontier-0910/`.
Raw event/probe/service histories, rather than this summary, preserve every
observation. This diagnostic capture is not an ordinary performance comparison.
Root created and listed the [eight-file raw archive](TCP_WIRE_FRONTIER_20260910.raw.tar.gz):
the five result files, feature build log, run log and exact runner. The frozen
executable is `./.tmp/reflection/bin/tcp-wire-frontier-20260910/mptunnel`, built
on checkpoint `e4a7c9f` with ordinary policy `ba56290`. The feature-only overlay
remains present for the causal checkpoint; it was not reversed before traffic.

The explicit diagnostic selector is stream 0, with no default selector. The
workload's first successful 64-byte echo precedes bulk opening; the captured
stream's complete 64-byte intervals also match all 46 serial echo transactions.
Server observation requires a single protected frame with a nonempty payload
at most 64 bytes. There is only one pending diagnostic interval per carrier,
never replacement of an unfinished sample. Later frames on that carrier may
therefore be unobserved: this is not an unbiased all-echo native distribution.

For the same exclusive Noise writer/socket lifetime, `W` counts all successful
protected writes since split and `Q` is native NOTSENT. Signed `W-Q` is the
initial-send frontier relative to that writer baseline. Older handshake debt
is retained by signed subtraction; unavailable Q is not replaced by zero.
TLS is excluded because read-side protocol writes invalidate exclusive writer
accounting. The counter/identity assumptions were independently audited before
the capture. Crossing means the complete protected interval has entered native
initial transmission, not acknowledgment, client delivery or ordered consumption.

Only existing native observation turns sample Q. For an admitted protected
interval ending at E, a below-E syscall bracket and a later crossing bracket
give the conservative relative bound:

`last_below_begin_after_admit <= actual E crossing <= sample_end_after_admit`.

Use the last below-bracket **begin**, not its end, as the lower bound. A late
first above-E observation alone proves only an upper bound; actor/writer service
can postpone observation after actual transmission. Client events occur after
authenticated frame decode, before the reader's bounded actor queue. Unix log
timestamps permit approximate cross-process joins at millisecond resolution;
local `t_mono_ms` origins differ and are never subtracted across roles. Native
relative microsecond brackets do not depend on matching those clock origins.

## Identity, coverage and duplicate handling

All events use session `2821970637992442049`, stream 0. Wire path IDs are not
management slot ordinals. Server wire path/registration instances are 0/2,
1/3 and 2/1. Client wire paths retain one socket each, with local ports
54454, 54466 and 54468 respectively, remote port 7443; no observed identity
reuse makes these joins ambiguous.

- Server emits 148 events for 75 distinct protected intervals: 73 start/cross
  pairs and two immediately crossed intervals without a preceding start event.
  Every logged start has a crossing; every logged interval has exactly one
  matching same-session/path/stream/DSN client authentication.
- All 148 events satisfy `frontier = W-Q` exactly, with zero unknown samples.
  Every tracked interval is 112 protected bytes carrying exactly 64 stream
  bytes. Client emits 93 authenticated 64-byte frames for 46 distinct stream
  intervals `[64*i,64*(i+1))`, with no same-path/same-interval duplicate ambiguity.
- The 47 additional authenticated frames repeat stream bytes on other carriers.
  They are not 47 additional completed application responses. These events do
  not label Original versus particular recovery cause; the report does not
  infer that origin from the carrier or duplicate count.
- Only 35 of 46 earliest-authenticated frames have a captured server frontier;
  40 tracked intervals authenticate later than another copy, and 18 client
  frames have no matching tracked server interval. Eleven earliest frames
  are therefore explicitly unobserved, not assigned zero native delay.

"Earliest authenticated" is not a claim about which reader's later actor
delivery wins. It is a necessary lower boundary for application delivery, and
all copies are included when locating that boundary. Of the 35 observed earliest
frames, 24 have a native lower bound of at least 500 ms and two at least one
second. Their median lower/upper endpoints are 590.148/631.288 ms, descriptive
only of this selected sample. Do not publish that as all-echo native p50/p95.

## Decisive same-request bounds

Echo 38 is the slowest probe transaction: start 31.813372, completion 33.310833
seconds, elapsed 1497.461 ms. Its exact stream interval is `[2432,2496)`.
The earliest authenticated frame is wire 2, matching server instance 1 and
protected interval `[600083336,600083448)`.

| Boundary | Exact observation |
|---|---|
| Server start, seq 125 | Unix 1789019145643 ms; protected write/flush took 4 us |
| Initial frontier | W=600,083,448; Q=19,693,694; W-Q=580,389,754 |
| Last below-E bracket | 1,352,786–1,352,788 us after admission; frontier 600,047,802 < E |
| Crossing, seq 128 | Unix 1789019146997 ms; bracket 1,354,334–1,354,336 us; frontier 600,102,826 >= E |
| First client authentication, seq 77 | Unix 1789019147067 ms, wire 2 |
| Other-copy authentications | Wire 1 at 1789019147508 ms; wire 0 at 1789019147603 ms |

At the first sample, 19,693,582 not-yet-initially-sent bytes precede the echo's
protected start. The complete frame remains natively unsent for at least
1.352786 seconds, with only 1.550 ms between the lower and upper bounds. That
lower bound is 90.34% of this exact request's whole elapsed time. Authentication
is about 1424 ms after native write admission and 70 ms after the observed
crossing, using approximate Unix anchors. Thus a tiny successful native write
does not imply timely transmitted echo service. This is not a promise that
removing queue occupancy would recover that percentage without other costs.

Echo 27 independently shows the same stage: interval `[1728,1792)`, wire 2/
instance 1, protected `[370269446,370269558)`, start/cross server seq 85/88.
Its native bracket is 1155.971–1166.647 ms; earliest authentication is client
seq 54 at Unix 1789019135963 ms, 30 ms after the crossing log. The probe's
whole elapsed is 1268.800 ms. This is another substantial exact interval,
not evidence confined to cold startup.

A necessary negative control exists within that same echo: its later wire-1
copy authenticates at Unix 1789019136097 ms, **275 ms before** the server's
first logged crossing at 1789019136372 ms. The valid native bracket for that
copy remains 825.719–1301.026 ms. Treating the upper crossing timestamp as exact
transmission time would falsely add delay. The tight below/cross bounds above,
not a first-above timestamp alone, support the material attribution.

## Whole workload and every echo attempt

Probe status is `ok`, HTTP 200, one intentionally duration-stopped partial
8 GiB body and no complete object. Body bytes are 2,097,414,902 over 40.001977
seconds, 419.462 Mbps. First body is at 0.585643 seconds; maximum read gap is
0.367512 seconds at 32.227963–32.595475, with body counters
1,694,376,502–1,694,442,038 bytes. That gap is not itself joined to an echo or
body-owned protected interval by this observer. Stderr is empty.

All 46 echo attempts succeed, 2,944 response bytes, no failure/disconnect.
Whole echo p50/p95/max are 893.015/1253.589/1497.461 ms; maximum successful
completion spacing is 1.497477 seconds. Last echo ends at 40.613695 seconds.
Quantiles use sorted index `round((n-1)*rank)`; phase membership follows request
start. Slow serial replies explain fewer attempts than nominal 500 ms slots.

| Fixed healthy phase | Body Mbps | Echo count / p50 / p95 / max, ms |
|---|---:|---:|
| 0–5 s | 321.730 | 9 / 481.534 / 1081.179 / 1081.179 |
| 5–15 s | 433.507 | 11 / 855.095 / 1168.145 / 1168.145 |
| 15–25 s | 433.973 | 11 / 909.058 / 1268.800 / 1268.800 |
| 25–40 s | 433.023 | 15 / 955.050 / 1253.589 / 1497.461 |

All 40 untrimmed one-second body Mbps bins, from second zero:

```text
2.097,276.945,297.675,514.434,517.499,258.162,645.941,472.247,396.495,506.575,366.368,160.660,684.384,458.751,385.484,564.134,408.303,381.959,406.032,539.491,480.265,228.865,528.006,420.796,381.878,486.111,430.965,433.649,388.459,502.997,434.834,491.731,266.473,444.382,451.675,435.385,436.920,426.482,457.193,408.094
```

Each echo i corresponds to DSN `[64*i,64*(i+1))`. Native brackets below are
relative to that frame's completed server write admission, not request start.
"Unobserved" means the earliest-authenticated frame lacks this bounded observer's
native sample; other copies may have samples but cannot replace it.

| Echo | Probe start–end, s | Elapsed, ms | First authenticated wire | Native end-frontier bracket, ms |
|---:|---|---:|---:|---|
| 0 | 0.103070–0.203819 | 100.748 | 1 | 0.000–0.002 |
| 1 | 0.500427–0.601729 | 101.302 | 1 | 0.000–0.002 |
| 2 | 1.000521–1.322524 | 322.003 | 1 | 144.420–201.211 |
| 3 | 1.500555–2.159312 | 658.757 | 1 | 319.542–320.480 |
| 4 | 2.159339–2.954189 | 794.850 | 1 | 346.837–347.736 |
| 5 | 2.954212–3.435747 | 481.534 | 2 | unobserved |
| 6 | 3.454303–3.865731 | 411.428 | 2 | 74.431–156.260 |
| 7 | 3.954361–4.511068 | 556.707 | 2 | 273.752–274.686 |
| 8 | 4.511091–5.592270 | 1081.179 | 2 | 816.941–819.330 |
| 9 | 5.592291–6.557368 | 965.077 | 1 | 329.701–333.681 |
| 10 | 6.557395–6.881187 | 323.792 | 2 | 104.525–111.834 |
| 11 | 7.057504–7.876108 | 818.604 | 1 | unobserved |
| 12 | 7.876131–8.731227 | 855.095 | 1 | 629.177–630.021 |
| 13 | 8.731249–9.899394 | 1168.145 | 2 | unobserved |
| 14 | 9.899424–10.893428 | 994.004 | 2 | 761.197–870.519 |
| 15 | 10.893452–11.922492 | 1029.041 | 2 | 830.182–887.756 |
| 16 | 11.922512–12.561061 | 638.549 | 1 | unobserved |
| 17 | 12.561080–13.580592 | 1019.512 | 2 | unobserved |
| 18 | 13.580610–14.435213 | 854.603 | 2 | 590.148–654.898 |
| 19 | 14.435233–15.027994 | 592.761 | 1 | unobserved |
| 20 | 15.028014–15.937072 | 909.058 | 2 | unobserved |
| 21 | 15.937092–16.423439 | 486.346 | 2 | 268.802–281.785 |
| 22 | 16.437172–17.273725 | 836.553 | 2 | 526.335–631.288 |
| 23 | 17.273743–18.420624 | 1146.881 | 2 | 891.960–997.690 |
| 24 | 18.420652–19.238642 | 817.989 | 2 | 547.514–653.150 |
| 25 | 19.238666–19.936027 | 697.362 | 2 | 479.345–480.407 |
| 26 | 19.936051–20.937865 | 1001.815 | 2 | 796.087–797.862 |
| 27 | 20.937883–22.206683 | 1268.800 | 2 | 1155.971–1166.647 |
| 28 | 22.206710–23.336062 | 1129.352 | 1 | 579.700–579.881 |
| 29 | 23.336079–24.091918 | 755.839 | 2 | 517.646–520.599 |
| 30 | 24.091938–25.078501 | 986.563 | 2 | 774.391–782.804 |
| 31 | 25.078519–26.154884 | 1076.365 | 1 | 855.031–858.965 |
| 32 | 26.154902–27.338579 | 1183.677 | 2 | 900.106–1007.486 |
| 33 | 27.338596–28.135350 | 796.754 | 2 | 575.020–598.501 |
| 34 | 28.135373–28.926286 | 790.914 | 2 | 574.232–577.761 |
| 35 | 28.926306–30.026728 | 1100.422 | 1 | unobserved |
| 36 | 30.026745–30.858285 | 831.541 | 1 | 620.427–622.676 |
| 37 | 30.858306–31.813356 | 955.050 | 1 | 725.435–758.656 |
| 38 | 31.813372–33.310833 | 1497.461 | 2 | 1352.786–1354.336 |
| 39 | 33.310854–34.252355 | 941.501 | 2 | 710.465–734.049 |
| 40 | 34.252377–35.145391 | 893.015 | 2 | 662.494–664.407 |
| 41 | 35.145415–36.053813 | 908.398 | 2 | 670.426–697.290 |
| 42 | 36.053841–36.980485 | 926.645 | 2 | 705.972–710.303 |
| 43 | 36.980510–38.156345 | 1175.835 | 1 | unobserved |
| 44 | 38.156366–39.409955 | 1253.589 | 2 | unobserved |
| 45 | 39.409977–40.613695 | 1203.718 | 1 | unobserved |

## Shared path and resource context

All 41 service rows verify 500/500 Mbps, 30/70 ms delay, zero configured
impairments, HTB rate equal to ceil, 65,536-byte burst/cburst and 8,192-packet
netem limit. Actual class/qdisc drops are zero. Three native TCP carriers use
kernel `bbr`. Server pooled native RTT median/p95/max is
211.159/271.441/338.424 ms; median BBR minimum RTT is 100.035 ms. These values
are not application timing or the exact selected echo's RTT.

The service window is 0.000060–40.006183 seconds. DOWN/UP class byte deltas
are 2,387,270,938/21,436,776; sampled backlog median/max is
8,934,030/16,806,476 bytes DOWN and 41,589/47,061 bytes UP. Aggregate server
socket NOTSENT median/p95/max is 47,890,003/51,821,482/55,216,008 bytes;
client median/p95 is zero, max 1,358 bytes. These queues have different scopes
and must not be added or interpreted as an MPP memory leak.

Client process RSS peak/final is 61,380/49,984 KiB; server 174,772/165,372 KiB.
Last lifetime `ps` CPU is 57.8% client and 126% server, not interval or total-host
CPU. Class windows, native sample instants and probe intervals differ; no
packet-category cost or exact wire amplification is inferred. Duration-close
BrokenPipe/RemoteClosed warnings occur after successful probe service, not
additional failed echoes. Raw files preserve the full context.

## Decision supported, and what is not proved

The information forecast is met: for material exact requests, substantial
delay occurs after fast server write acceptance but before the full protected
frame enters native initial transmission. The tight worst-request bracket
rules out attributing that dominant interval solely to later client actor
service, a cached capacity label or common propagation. This establishes the
native handoff/FIFO service boundary as a justified model target.

It does not establish an optimal NOTSENT/LOWAT value, a safe buffer reduction,
which earlier sending decision should change, or a universal performance gain.
Restraining irreversible queued work may alter refill/wake service, high-BDP
throughput, CPU, repair and terminal semantics; those need their own model and
ordinary controls. No native controller retuning, physical-inevitability claim
or promotion follows automatically. The original mixed upload stall, return-
round outage tradeoff and global acceptance requirements remain open.
