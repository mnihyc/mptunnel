# Asymmetric feedback service — 2026-09-09

Status: ordinary-binary diagnostic comparison, not performance acceptance.
During a client→server bottleneck, mixed DOWN body service falls to 124.563Mbps
and one actual echo times out. QUIC, raw TCP and Hysteria2 remain useful under
the same feedback constraint. Mixed bulk recovers after restoration, but the
probe does not reconnect its echo socket, so restored echo service is untested.

## Actual profile, correction and reproducibility

The predeclared question was whether current service recovered after a 10Mbps
DOWN bottleneck returned to 500Mbps. **That DOWN-QoS gate was not run.**
The retained mirrored configuration instead produced:

| Runner epoch | Router eth0: server→client / DOWN | Router eth1: client→server / UP |
|---|---:|---:|
| 0–15s | 500Mbps | 500Mbps |
| 15–25s | 500Mbps | 10Mbps |
| 25–40s | 500Mbps | 500Mbps |

UP carries feedback, requests and other transport/control traffic—not ACKs alone.
All saved class snapshots confirm DOWN 500Mbps throughout and UP 10Mbps in
service rows 16–25, restored 500Mbps from row 26. Both directions retain HTB
rate=ceil, 64KiB burst/cburst; DOWN 30ms and UP 70ms delay, zero jitter, no configured
random loss, netem limit 8192, mirrored=true, no UDP blackhole. Configured loss 0
does not mean zero observed queue drops: mixed alone records 8508 UP drops.

The setup error follows the existing `run.py` mapping: `shape()` first swaps
server/client for `REFLECTION_MIRROR_IMPAIRMENT`, then maps the resulting side
to router link 46/47. The combined server-side QoS request consequently limits
UP under this mirror setting. The experiment is retained as asymmetric-feedback
evidence, not relabeled as its intended DOWN-cap recovery proof or rerun here.
The useful forecast was to distinguish persistent mixed collapse from common
physical limits; the actual direction changes the question and leaves the
original recovery gate open.

Four sequential cells, order QUIC→mixed→raw→H2, tag
`latest-credit-qos-recovery-0909`. MPP uses ordinary runtime `4c7e232`,
frozen `./.tmp/reflection/bin/latest-credit-actor-yield-20260909/mptunnel`,
at both endpoints, without diagnostic hooks. No compiler/lab overlap or runtime
change. H2 retains its explicit 500Mbps up/down prior; MPP dynamic discovery is
unchanged. This is one realization per mode, not paired packet-identical traffic.
The [healthy reference](HIGH_CAPACITY_REFERENCE_20260909.md) retains topology,
configuration-prior and ordinary-comparison context.

Sources are `./.tmp/reflection/results/{quic,mixed,raw,h2}-combined-down-latest-credit-qos-recovery-0909/`.
[Raw archive](ASYMMETRIC_FEEDBACK_SERVICE_20260909.raw.tar.gz) preserves all 18
result files, the common `./.tmp/reflection/latest-credit-qos-recovery-0909-run.log`,
and exact `run.py`, `shape.sh`, `lab/mixed_workload_probe.py` sources: 22 files.
Archive size 264468B, members 3075519B uncompressed; gzip integrity, exact ordered
manifest and byte-for-byte tar comparison against every source pass. No hashes.
Raw TCP creates no tunnel client/server logs; no missing logs were fabricated.
No credentials, private configurations or executables are included.
All 160 untrimmed bins and all 313 echo records remain at original JSON precision:
271 actual attempts (270 successes, one I/O timeout) and 42 unattempted placeholders.
All `probe.err` files are empty.

## Complete body and loaded-echo outcomes

Each mode makes one HTTP200 request for an 8GiB body, deliberately stopping after
40s: zero complete bodies and one duration-partial successful body. All runners
exit0; mixed's combined probe nevertheless reports `status=loss` because its
interactive stream times out. Runner exit0 is not a clean interactive outcome.

| Mode | Body bytes / probe seconds | Whole-run Mbps | First body / maximum read gap, s | Runner seconds |
|---|---:|---:|---:|---:|
| QUIC | 2125615078 / 40.000938342 | 425.113043 | .447667 / .101260 | 40.261235716 |
| Mixed | 1684440736 / 40.003207488 | 336.861135 | .579848 / 1.246742 | 41.007819704 |
| Raw | 2257119232 / 40.000931293 | 451.413336 | .403475 / .100337 | 41.004650771 |
| H2 | 2318320875 / 40.000562423 | 463.657656 | .406833 / .114587 | 41.007453190 |

| Mode | Actual successes / attempts | Unattempted after disconnect | Successful echo p50 / p95 / max, ms | Max successive-success gap, s |
|---|---:|---:|---:|---:|
| QUIC | 80 / 80 | 0 | 106.687 / 164.976 / 325.314 | .602364 |
| Mixed | 30 / 31 | 42 | 318.416 / 685.874 / 1107.107 | 1.176784 |
| Raw | 80 / 80 | 0 | 103.512 / 124.495 / 294.966 | .688274 |
| H2 | 80 / 80 | 0 | 111.034 / 114.352 / 119.890 | .509864 |

Echo payload 64B, nominal interval 500ms, timeout 3000ms. QUIC/raw/H2 each send
and receive 5120B. Mixed sends 1984B and receives 1920B; the final 64B is not echoed
before its timeout. The successful-latency quantiles exclude that timeout and
the subsequent silence; they are not an all-attempt latency distribution.

Mixed attempt 29 succeeds at 15.302589967→15.988464235s (685.874268ms).
Attempt 30 then fails at 15.988480505→18.990226970s, an actual 3001.746465ms I/O
timeout (`interactive_error="timed out"`). The probe closes the socket and never
reconnects. Its 42 later `unavailable_after_disconnect` records, beginning
18.990295601s, are not 42 independent sends, network failures or failed reopenings.
Thus `interactive_fail=43` must be decomposed into one attempted timeout plus 42
unattempted slots. The 1.176784s successive-success statistic does not bound the
timeout or censored tail.

## Phase history and recovery limits

These are arithmetic means of untrimmed probe bins in nominal elapsed windows,
not exact same-clock physical throughput samples.

| Mode | 0–15s | 15–25s feedback bottleneck | 25–30s | 30–35s | 35–40s | All 25–40s |
|---|---:|---:|---:|---:|---:|---:|
| QUIC | 411.222 | 440.691 | 447.515 | 405.248 | 433.067 | 428.610 |
| Mixed | 385.970 | 124.563 | 452.194 | 425.268 | 410.502 | 429.321 |
| Raw | 424.014 | 475.851 | 437.864 | 474.064 | 475.616 | 462.514 |
| H2 | 455.775 | 470.379 | 470.282 | 470.426 | 460.509 | 467.072 |

Mixed's lowest body bin is 0.716Mbps at index 20; its longest exact body gap is
19.675832847→20.922575278s, bytes 816614856→816626856. QUIC's maximum gap is
startup .548079008→.649338510s; raw's is 29.602756348→29.703093162s; H2's is
37.403746424→37.518333821s. These probes retain the maximum-gap witness, not an
event series of every read gap. All modes have no zero bins after restoration.

Echo phases group attempts by start time, independently of body bins:

| Mode | Before 15s | 15–25s | After 25s |
|---|---|---|---|
| QUIC | 30 successes | 20 successes; max 155.816ms | 30 successes; max 169.487ms |
| Mixed | 29 successes | 1 success, 1 timeout, 13 unattempted | 29 unattempted; no recovery test |
| Raw | 30 successes | 20 successes; max 126.972ms | 30 successes; max 124.495ms |
| H2 | 30 successes | 20 successes; max 113.324ms | 30 successes; max 116.021ms |

Bulk restoration is directly supported; mixed post-restoration interactive recovery
is not. The asymmetric phase is a genuine high-impact mixed contrast, not a
subsecond-extrema-only difference or a universal failure of a 10Mbps feedback link.

## Physical queue and process-cost evidence

The following sample-window differences use each service file's rows 16→25,
both inside the reduced-UP phase. Actual elapsed endpoints: QUIC 15.002118852→
24.003287677, mixed 15.001646116→24.003412784, raw 15.001752248→24.002711648,
H2 15.002049930→24.003009219s. They are not synchronized to probe bin edges.

| Mode | DOWN class bytes Δ | UP class bytes / packets Δ | UP drops Δ | UP class backlog, first→last B |
|---|---:|---:|---:|---:|
| QUIC | 513422064 | 8152508 / 76117 | 0 | 56575→75928 |
| Mixed | 157070253 | 11048191 / 29306 | 8508 | 3817355→5631597 |
| Raw | 549028702 | 605526 / 9157 | 0 | 4752→4686 |
| H2 | 543917425 | 3519226 / 43920 | 0 | 26574→26014 |

Mixed has a sustained multi-megabyte UP queue and overflow drops while body
service deteriorates; the other modes show neither comparable feedback queue
growth nor drops. This supports a feedback-service bottleneck, but does not
identify the responsible serialized frame kinds, ACK policy, duplicate fraction,
native behavior or exact stalled-byte queue position. UP packet size distributions
and batching differ; physical bytes are not unique logical acknowledgements.
At restoration mixed's UP class backlog falls to 212505B in row 26 and ends at 0B.

| Mode | Full sample-window DOWN bytes / packets Δ | UP bytes / packets Δ | Peak class backlog DOWN / UP, B |
|---|---:|---:|---:|
| QUIC | 2234572938 / 1495701 | 34135371 / 316651 | 5279796 / 86085 |
| Mixed | 1998947607 / 1444846 | 120598240 / 538705 | 24301812 / 5631597 |
| Raw | 2367844808 / 1564044 | 2645481 / 40017 | 14022798 / 6402 |
| H2 | 2451611345 / 1706512 | 16014472 / 199635 | 3056540 / 32161 |

Only mixed UP records drops, 8508 total; all DOWN and other UP counters remain 0.
These HTB class counters include framing, control, bulk, copies and native
transport traffic. Parent HTB and child netem are overlapping accounting layers,
not additional byte totals. Neither queue divided by capacity nor these aggregate
deltas yields an exact per-byte critical delay or an attributable speedup.

| Tunnel process | Client PID; RSS peak/last KiB; ps %CPU max/last | Server PID; RSS peak/last KiB; ps %CPU max/last |
|---|---|---|
| QUIC | 337856;38176/38176;97.7/97.7 | 343408;295528/295528;154/154 |
| Mixed | 338787;100216/100216;87.7/87.7 | 344328;335152/335152;167/167 |
| H2 | 340187;44160/29604;93.1/93.1 | 345719;47480/47480;83.1/83.1 |

QUIC has 40 service rows, last elapsed 39.261028274s; the others 41, last
40.007672427/40.004467146/40.007287337s for mixed/raw/H2. Each tunnel PID is stable.
Raw has no tunnel process: client Python probe 339735 has RSS peak/last-observed
17940KiB, CPU max 16.1%/last 3.0%, observed in rows 1–40 but absent at the last row.
Its long-lived server Python processes are endpoint services, not raw-tunnel costs.
All ps percentages are process-lifetime aggregates, not exclusive feedback CPU;
RSS is sampled, not post-load settled ownership or proof of a leak.

## All 160 untrimmed one-second body bins

Each mode lists indices 0–39, in Mbps. No trimming, dropped zeros or invented
curve; values above 500Mbps are buffered application delivery, not wire capacity.

### MPP QUIC

```text
9.533, 363.741, 454.986, 440.646, 442.899, 463.382, 385.896, 445.084, 447.875, 456.553
460.642, 447.150, 438.129, 470.207, 441.612, 441.939, 383.923, 446.328, 444.875, 464.907
433.137, 436.092, 459.259, 451.808, 444.644, 453.410, 449.297, 442.580, 455.557, 436.731
331.455, 377.595, 433.013, 452.554, 431.625, 447.136, 378.507, 442.368, 441.556, 455.766
```

### MPP mixed

```text
2.620, 103.688, 198.373, 756.836, 394.191, 441.598, 425.403, 450.733, 419.806, 317.212
605.584, 256.745, 609.666, 453.557, 353.537, 295.151, 208.209, 87.097, 62.583, 90.329
0.716, 75.229, 149.133, 234.880, 42.306, 519.390, 438.689, 463.279, 384.163, 455.447
330.364, 479.457, 427.434, 439.619, 449.465, 316.975, 565.795, 352.813, 359.301, 457.628
```

### Raw TCP

```text
5.734, 369.275, 476.890, 475.813, 471.179, 476.334, 473.554, 475.164, 473.728, 471.758
473.380, 477.666, 478.130, 477.353, 284.248, 476.102, 476.218, 478.130, 477.550, 477.782
470.832, 476.508, 477.875, 475.465, 472.048, 474.075, 477.377, 477.122, 473.844, 286.901
469.442, 476.543, 476.624, 475.639, 472.071, 474.064, 476.855, 476.056, 476.450, 474.654
```

### Hysteria2

```text
280.871, 473.313, 464.953, 471.744, 469.773, 465.672, 473.579, 428.777, 472.792, 473.877
473.342, 472.246, 469.852, 473.446, 472.383, 467.561, 475.005, 470.390, 470.684, 469.627
470.444, 470.129, 470.740, 469.238, 469.968, 469.365, 471.332, 467.597, 472.429, 470.687
468.339, 469.486, 470.230, 471.859, 472.215, 472.623, 469.294, 423.044, 467.868, 469.717
```

Disposition: ordinary asymmetric-feedback service remains unaccepted because mixed
alone has an interactive timeout and suffers a material bulk collapse.
The exact feedback work causing that collapse is unresolved; no ACK-only cause,
controller correction, threshold or protocol preference is authorized by this
report. The intended DOWN-bottleneck recovery gate remains unexecuted. The smallest
next discriminator is existing-boundary per-kind feedback byte/service accounting
joined to the critical interval, not another favorable ordinary realization.
