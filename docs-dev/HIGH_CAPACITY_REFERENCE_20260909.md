# High-capacity reference panel

2026-09-09. **All twelve observations finished; two baseline UP probes lack
terminal confirmation. No performance acceptance.** MPP sustains roughly
403–441 Mbps, but the clean DOWN baselines deliver 444–466 Mbps with
loaded-echo p95 0.114–0.127 s. MPP TCP/mixed p95 is 1.247 / 0.501 s,
and QUIC 0.191 s. This is a material matched experience gap, not near-perfect
service or a delay that this physical cut alone makes necessary.

## Scope and comparability

This is the declared high-capacity comparison under the
[mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) and
[current ledger](CURRENT_CLOSURE_PLAN.md), not another runtime adjustment.
MPP is checkpoint `4c7e232`, frozen as
`./.tmp/reflection/bin/latest-credit-actor-yield-20260909/mptunnel`,
ordinary default build, diagnostics off at both endpoints.
The [preceding two mixed cells](LATEST_CREDIT_ACTOR_YIELD_20260909.md) are
reused exactly, not rerun for favorable numbers. Fresh TCP/QUIC UP and DOWN
use tag `latest-credit-reference-0909`.

All completed MPP shaping samples match: 500 Mbps shared HTB rate/ceil
both ways; mirrored eth0 server→client 30 ms and eth1 client→server 70 ms;
no configured jitter, random loss, QoS change or UDP blackhole; netem limit
8,192 and HTB burst/cburst 65,536 unchanged. This is a 100 ms configured
round trip, with UP data on the 70 ms leg and DOWN data on the 30 ms leg.
TCP uses three TCP carriers, QUIC one QUIC carrier, mixed three TCP plus
one QUIC sharing the cut. Aggregate membership does not identify the
current output of a particular loaded echo.

The completed baseline observations are raw TCP, Xray VMess/TCP and Hysteria2,
both directions, with the same routed shape/load and existing configurations.
Hysteria2 retains an explicit 500 Mbps up/down prior; MPP discovery is
dynamic. Raw TCP has no tunnel encryption, multiplexing or recovery layer.
Xray's supplied client configuration has no explicit mux setting; Hysteria2's
bandwidth prior is explicit in both directions. Endpoint containers retain
the existing four-CPU allocation. These differences remain visible, not
silently equalized or described as identical algorithms. All cells are
sequential; no compiler overlaps the panel, but Native/host histories are
not identical realizations.

## Useful upload service

One 40 s source load followed by existing settlement. All completed MPP UP
cells are exact accepted = target-confirmed = final bytes, 1/1 stream
complete, zero failures, valid sink-ACK accounting, empty `probe.err`
and runner exit 0. There is no concurrent loaded-echo workload in UP:
echo latency is not measured, not zero. Times are seconds, rates Mbps.

| System | Confirmed B | Probe s | Observed Mbps | First / max confirmation gap s | First / max local-write gap s |
| --- | ---: | ---: | ---: | ---: | ---: |
| MPP TCP | 2,188,967,936 | 41.551668 | 421.445 | 0.415239 / 0.491610 | 0.105618 / 0.520560 |
| MPP QUIC | 2,282,749,952 | 41.398857 | 441.123 | 0.210794 / 0.901023 | 0.109619 / 0.894112 |
| MPP mixed, reused | 2,131,230,720 | 41.055800 | 415.285 | 0.411310 / 0.789938 | 0.106810 / 0.630792 |
| Raw TCP, exact complete | 2,321,088,512 | 40.370270 | 459.960 | 0.204594 / 0.296817 | 0.104352 / 0.401349 |
| Xray VMess/TCP, incomplete | 2,266,115,368 LB | 41.613802 | 435.647 LB | 0.207770 / 0.251004 | 0.004812 / 0.655626 |
| Hysteria2, incomplete | 2,345,730,237 LB | 40.137256 | 467.542 LB | 0.207634 / 0.204053 | 0.106703 / 0.013493 |

**LB means the probe's confirmed lower bound during the observation, not
completed-transfer goodput.** Xray locally accepts 2,277,703,680 B and Hysteria2
2,357,002,240 B, respectively 11,588,312 / 11,272,003 B beyond their confirmed
counts. Both report `complete=false`, 0/1 complete, 1 failed stream,
`upload_ack_accounting_valid=false`, `status=loss`, and
`upload sink closed before terminal acknowledgement`, despite probe/runner
exit 0 and empty `probe.err`. Neither is full upload success. This is a
terminal-confirmation/half-close-path limitation with attribution unresolved,
not proven physical loss of those bytes or a reason to declare an MPP win.
Their interval arrays are empty: no curves or missing-tail gaps are invented.
Reported maximum gaps are observations before the error, not a bound through
an unobserved successful completion. Raw TCP does settle exactly.

Time beyond the 40 s load is 1.551668 / 1.398857 / 1.055800 s for
TCP / QUIC / mixed; completed work differs, so these are not fixed-work
settlement speedups. UP probes retain maxima but no exact gap timestamps.
QUIC's first bin is only 0.096 Mbps despite its early first confirmation;
one cannot locate its 0.901023 s maximum at startup from that alone.
MPP QUIC has the highest whole UP throughput here, but not the smallest
reported maximum confirmation or local-write gap.

## Useful download and loaded echo

All six DOWN cells are HTTP 200 / bulk status `ok`, one
**duration-partial successful 8 GiB response**: zero full response completions
and one partial response after 40 s. Body bytes below are actually read;
they are not a completed 8 GiB transfer or a failed download. All runner
exits are 0 and probe error files empty.

| System | Exact body-read B | Bulk probe s | Whole Mbps | First body / max read gap s |
| --- | ---: | ---: | ---: | ---: |
| MPP TCP | 2,102,234,952 | 40.001123 | 420.435189 | 0.595154 / 0.401498 |
| MPP QUIC | 2,123,359,818 | 40.002036 | 424.650344 | 0.412425 / 0.100845 |
| MPP mixed, reused | 2,016,075,971 | 40.000306 | 403.212112 | 0.615795 / 0.323468 |
| Raw TCP | 2,225,952,480 | 40.000072 | 445.189691 | 0.403344 / 0.100224 |
| Xray VMess/TCP | 2,220,519,824 | 40.000663 | 444.096599 | 0.406128 / 0.200357 |
| Hysteria2 | 2,332,075,082 | 40.001232 | 466.400650 | 0.406933 / 0.007427 |

Echo is a serial 64 B request/reply workload with nominal 500 ms interval
and 3,000 ms timeout. All 445 actual attempts succeed, without timeouts,
disconnects or unavailable-after-disconnect slots. Counts differ because a
slow completed attempt delays the next one, not because missing attempts
are silently classified as successful.

| System | Echo successes / attempts | p50 / p95 / maximum s | Max successful-response spacing s |
| --- | ---: | ---: | ---: |
| MPP TCP | 45 / 45 | 0.920054 / 1.246636 / 1.343971 | 1.395982 |
| MPP QUIC | 80 / 80 | 0.103476 / 0.190583 / 0.418649 | 0.714934 |
| MPP mixed, reused | 80 / 80 | 0.334069 / 0.501201 / 0.561082 | 0.714449 |
| Raw TCP | 80 / 80 | 0.103386 / 0.126073 / 0.301917 | 0.679128 |
| Xray VMess/TCP | 80 / 80 | 0.103655 / 0.127198 / 0.297625 | 0.686883 |
| Hysteria2 | 80 / 80 | 0.111112 / 0.114263 / 0.118621 | 0.507286 |

TCP's p50 of 0.920 s is sustained loaded latency, not just an isolated
tail. Mixed's 0.334 s median and 0.501 s p95 also remain well above the
configured round trip. QUIC achieves similar body throughput with much
lower loaded echo in this panel; neither equal throughput nor all-success
status erases these differences. Whether shared Native ordering, physical
queues, stream placement or Product service explain them remains unjoined.

Exact maximum-gap and echo intervals differ:

- TCP body gap: 11.222872 → 11.624370 s; worst echo index 37:
  32.192328 → 33.536300 s.
- QUIC body gap: 0.513254 → 0.614099 s; worst echo index 1:
  0.500349 → 0.918998 s.
- Mixed body gap: 33.011939 → 33.335407 s; worst echo index 30:
  15.018660 → 15.579741 s.

Only the MPP QUIC maxima overlap in time; that alone does not supply an exact
carrier/byte/queue causal join. Raw/Xray/Hysteria2 worst echoes occur at
2.000595→2.302513 / 2.000722→2.298347 / 34.006636→34.125257 s.
Their worst body gaps are 28.048830→28.149054 / 11.225053→11.425410 /
34.429290→34.436717 s. All complete attempt records are retained.

The baselines carry at least as much useful DOWN load while showing much
lower loaded latency, so lower offered throughput is not their explanation.
The identical physical cut alone does not require MPP TCP/mixed's observed
latencies. Carrier/stream sharing, scheduling and Product work still differ;
this panel does not select one as the internal cause. Hysteria2's explicit
prior and different transport remain relevant, not grounds to erase its
lower latency and higher body rate.

## Full histories and bounded costs

Full untrimmed series are retained in each original `probe.json`:
42 confirmation bins per MPP UP cell, 40 body bins per MPP DOWN cell,
246 bins and 205 echo-attempt records in the six MPP cells. Raw UP adds
41 confirmation bins, and the three baseline DOWN cells add 120 body bins
and 240 echo attempts: **407 available bins / 445 complete attempt records**
overall. Xray/Hysteria2 UP raw arrays are explicitly unavailable (empty),
not zero-service curves. No array is reconstructed from management counters
or replaced with a trimmed average. Buffered read/confirmation bins above
500 Mbps are not physical link capacity.

For orientation, two fixed body windows preserve an important mixed-mode
difference without selecting the whole result:

| Mode / direction | 5–15 s raw-bin mean Mbps | 25–35 s raw-bin mean Mbps |
| --- | ---: | ---: |
| TCP UP | 434.738 | 421.161 |
| QUIC UP | 452.277 | 448.343 |
| Mixed UP | 431.370 | 391.887 |
| TCP DOWN | 424.866 | 428.356 |
| QUIC DOWN | 428.386 | 433.944 |
| Mixed DOWN | 417.455 | 439.524 |
| Raw UP | 460.488 | 478.595 |
| Raw DOWN | 451.599 | 456.905 |
| Xray DOWN | 454.537 | 473.478 |
| Hysteria2 DOWN | 472.744 | 469.579 |

Mixed UP's later body is less steady; mixed DOWN does not trail every
individual body window despite its lower whole average. Full source/target/
reply sampling remains in `service.jsonl`; source is not claimed C,
target-written is not mux F, and reply delivery is not an assumed exact
response frontier. Client/server management timestamps are independently
generated. Baseline Product management is absent; no baseline S/T/Rs/Rc
series is manufactured from physical byte counters.

RSS is KiB, peak / last. ps %CPU is a sampled process-lifetime average,
not interval or handler-exclusive CPU. Last samples are not post-load
settled ownership; unequal useful work is not normalized away.

| Tunnel cell | Client / server PID | Client RSS peak / last | Server RSS peak / last | Client ps %CPU max / last | Server ps %CPU max / last |
| --- | --- | ---: | ---: | ---: | ---: |
| TCP UP | 330287 / 335914 | 183,288 / 155,436 | 59,404 / 51,852 | 89.6 / 88.2 | 46.8 / 46.1 |
| QUIC UP | 331261 / 336876 | 491,872 / 464,368 | 35,072 / 35,072 | 126 / 126 | 101 / 101 |
| Mixed UP | 329313 / 334954 | 311,664 / 287,100 | 124,064 / 124,064 | 167 / 164 | 104 / 100 |
| TCP DOWN | 332230 / 337836 | 73,716 / 37,496 | 186,524 / 157,816 | 60 / 60 | 124 / 124 |
| QUIC DOWN | 333182 / 338782 | 39,300 / 39,300 | 336,068 / 331,836 | 96.8 / 96.4 | 152 / 152 |
| Mixed DOWN | 328374 / 334009 | 89,856 / 89,856 | 337,060 / 318,196 | 124 / 124 | 199 / 199 |
| Xray UP, incomplete | 334583 / 340169 | 35,164 / 34,900 | 34,536 / 34,168 | 5.3 / 5.2 | 14 / 13.7 |
| Xray DOWN | 335979 / 341545 | 35,808 / 35,800 | 37,816 / 37,528 | 11 / 10.8 | 6.3 / 6.3 |
| Hysteria2 UP, incomplete | 335059 / 340638 | 42,484 / 42,484 | 30,152 / 29,392 | 84 / 84 | 94.2 / 94.2 |
| Hysteria2 DOWN | 336443 / 342004 | 27,812 / 26,884 | 45,540 / 45,540 | 93.2 / 93.2 | 83.3 / 83.3 |

Each tunnel endpoint PID is stable per cell. All DOWN cells have 41 service
rows; MPP/Xray UP 42, raw/Hysteria2 UP 41. MPP has substantially higher
tunnel-process CPU than Xray at similar or lower useful rates. These are
real observed cost differences, not a measured critical-handler fraction
or a leak diagnosis.

Raw TCP has **no tunnel-process cost**, so it has no comparable row here.
Its client Python probe peaks at 14,708 KiB UP / 17,936 KiB DOWN and
maximum lifetime CPU 14.2 / 19.3%; it is absent at the final sample.
Long-lived server Python origin/echo/sink processes are endpoint-service
scope, not a raw tunnel. Their memory/CPU must not be compared as if they
were equivalent tunnel endpoints.

Router HTB **class first→last byte deltas**, not summed parent/child counters;
all twelve observations' sampled class/netem drops are zero:

| Cell | eth0 server→client delta B | eth1 client→server delta B | Peak class backlog eth0 / eth1 B |
| --- | ---: | ---: | ---: |
| TCP UP | 22,204,551 | 2,462,366,197 | 19,383 / 17,172,674 |
| QUIC UP | 34,571,292 | 2,414,793,486 | 36,061 / 13,656,654 |
| Mixed UP | 56,684,158 | 2,429,715,096 | 99,442 / 45,952,728 |
| TCP DOWN | 2,396,835,778 | 45,338,993 | 16,503,474 / 203,124 |
| QUIC DOWN | 2,252,813,427 | 34,753,148 | 14,104,854 / 89,625 |
| Mixed DOWN | 2,427,896,902 | 140,977,420 | 27,646,828 / 383,733 |
| Raw UP | 2,686,196 | 2,415,753,738 | 2,787 / 11,453,410 |
| Raw DOWN | 2,333,644,930 | 2,609,001 | 14,034,910 / 5,874 |
| Xray UP, incomplete | 2,695,695 | 2,397,705,698 | 2,706 / 12,036,300 |
| Xray DOWN | 2,342,650,497 | 2,625,939 | 14,080,357 / 6,402 |
| Hysteria2 UP, incomplete | 16,208,410 | 2,471,576,175 | 14,672 / 5,248,984 |
| Hysteria2 DOWN | 2,466,526,923 | 16,185,342 | 2,675,403 / 31,925 |

Physical totals include protocol/control/retransmission/copy traffic and
omit traffic outside the sampling interval. They do not reveal an exact
duplicate fraction or locate an echo's critical byte. Zero configured loss
or sampled drops is not zero queuing.

## Evidence inventory and conditional disposition

Under `./.tmp/reflection/results/`, the four fresh MPP directories are
`{tcp,quic}-combined-{up,down}-latest-credit-reference-0909/`;
the two reused mixed directories are
`mixed-combined-{up,down}-latest-credit-actor-yield-0909/`.
Tunnel result directories retain all five original files: `probe.json`,
`probe.err`, `service.jsonl`, `client.log`, `server.log`.
Baseline directories are
`{raw,xray,h2}-combined-{up,down}-latest-credit-reference-0909/`.
Raw TCP has only the three probe/service files: no tunnel client/server logs
exist, and no placeholders were created.
Fresh run logs are `./.tmp/reflection/latest-credit-reference-mpp-{up,down}-0909-run.log`;
the reused mixed logs/archive are linked in the preceding ablation report.
Fresh runner times are TCP/QUIC UP 42.005372 / 42.005418 s and
TCP/QUIC DOWN 41.004359 / 41.006051 s.

Baseline run logs are
`./.tmp/reflection/latest-credit-reference-baselines-{up,down}-0909-run.log`.
UP runner times raw/Xray/Hysteria2 are 41.004446 / 42.004896 / 41.009056 s;
DOWN 41.004560 / 41.005663 / 41.011862 s. All runner exits are zero,
which does **not** override the two failed terminal-confirmation probe outcomes.

[HIGH_CAPACITY_REFERENCE_20260909.raw.tar.gz](HIGH_CAPACITY_REFERENCE_20260909.raw.tar.gz)
preserves all twelve result sets: 56 existing result files, six run logs,
the ordinary-build log and exact actor-yield patch, **64 regular files**.
Members total 9,476,459 B; archive size 841,001 B. Gzip integrity, exact
ordered member list and byte-for-byte comparison against every source file
pass. Full available bins, all attempt timestamps, physical snapshots and
embedded probe errors remain inspectable. Configurations/credentials and
executables are not copied into this archive.

The outcome is useful high-capacity MPP throughput, but a matched DOWN
throughput/loaded-latency and cost gap remains. MPP TCP/mixed are not
near-perfect or globally performance-accepted; QUIC is better for loaded
latency here but still trails the strongest baseline. Xray/Hysteria2's
incomplete UP accounting prevents a complete-transfer ranking there and
is not an MPP victory by default. Independent audits verify all twelve
profile/probe/cost summaries without assigning the internal cause.

This ordinary healthy panel is conditional evidence, not real-Internet
competitiveness, heterogeneous independent-link aggregation, QoS/outage
recovery, restart/churn, or release acceptance. Any next change must select
an observed owner/critical boundary; no further tuning follows solely from
these throughput or CPU totals.
