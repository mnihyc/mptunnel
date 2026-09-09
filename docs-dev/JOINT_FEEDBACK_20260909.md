# Conditional joint feedback: ordinary comparison

Recorded: 2026-09-09. Category: mixed-mode feedback service.
**REJECTED as a performance correction under the predeclared healthy-tail rule.**
The return-restricted gain is material, but adverse healthy echo tails recur
in the no-QoS discriminator. Lower healthy median latency and body gap are
retained below; this is not a claim that every metric regresses or that the
exact timing mechanism has been identified. No accepted runtime commit,
QUIC/upload follow-on, README update or release follows this trial.

## Mechanism, forecast and verification

[The scoped feedback model](SCOPED_ACK_SERVICE_MODEL.md#conditional-immediate-write-pairing-bounded-discriminator-not-code-selection)
contains the proof and original contract. `444fb38` introduced pre-write
receipt/startup service because a real one-byte-then-Pending sink blocked
ACK, OPEN and FINAL. The candidate preserves that protection when delivery
is Pending, but moves the current receipt publication behind one poll of the
same retained write/flush future. Ready success permits pairing one exact ACK
chunk with an independently due MAX; Pending/error offers ACK only. The same
future/cursor survives Pending, a Ready future is never repolled, and no
Product lock spans the poll. Startup/prepared-error service and final ACK
before shutdown remain protected. The changed ACK timing is explicit.

A typed internal pair retains two original frames and two pressure units in
one existing priority envelope. Acceptance advances its ACK chunk and grant
fences together, not an incomplete whole ACK generation. Failed admission
advances neither; standalone ACK/MAX retries, sparse catch-up, new attachments,
terminal filtering and every TCP/QUIC writer preserve independent ownership.
No feedback recipient, wire format, timer, native controller or numeric limit
changes. Audit corrected a candidate-only ordering issue: retrying an old ACK
before current receipt publication could occupy the only newly freed slot.
The prelude now retries that obligation only after current receipt is offered.

Forecast: remove at most one separate record/native transaction per eligible
pair. Existing packetization can erase the saving; the earlier readiness
capture is neither an exact pairing count nor a throughput forecast. Its
73,914 nonempty Ready successes would remove at most 3,991,356 TCP record bytes
over 40 s across three attachments under the model's generous assumptions.
Current return-service pressure justified this bounded comparison, not a
promise to recover the whole deficit. Any weak/adverse timing stops promotion;
no wider pairing or compensating controller/threshold change is authorized.

The real publisher RED validates original ACK/MAX facts, grant/generation
fences, pressure and retry deduplication before failing at two envelopes
instead of one. This is avoidable work, not corruption or proof of speed gain.
The [RED patch](JOINT_FEEDBACK_20260909.red.patch) and logs retain the result.
Two focused runs pass 1,267 checks each (3.40/3.30 s), including original
blocked startup, partial delivery/flush/error, FIN, publication/backpressure,
queue/drop and encrypted transport checks. The post-cleanup test build takes
34.55 s; the ordinary optimized build takes 3 min 33 s with one unused request
publication-wrapper warning. An earlier missing test import was fixture
integration, not a Product RED or a platform/model failure. Independent
publisher, actor and all-writer ownership reviews precede the ordinary runs.
These checks establish mechanism support, not practical acceptance.

After rejection, all24source/RFC files are restored exactly. The restored
affected1260checks pass in3.33s after a warning-free1m27s build. The
[standalone candidate patch](JOINT_FEEDBACK_20260909.candidate.patch) is retained
only as rejected evidence. The raw archive passes gzip integrity and tar's
byte-for-byte comparison against every retained input; it contains no binary.

## Fixed cells and effective profile

Control is ordinary `b2aa215`, frozen at
`.tmp/reflection/bin/scoped-ack-20260909/mptunnel`; candidate is the exact v3
patch frozen at `.tmp/reflection/bin/joint-feedback-20260909/mptunnel`.
Both are ordinary optimized binaries without observers or suppression policy.
The [raw archive](JOINT_FEEDBACK_20260909.raw.tar.gz) retains the four result
directories, complete echo series, service samples, run/build/test logs and
exact candidate patch. Result suffixes below expand from
`mixed-combined-down-joint-feedback-` and end with `-0909`.

The first pair runs `control` then `candidate`. Both use one 40 s HTTP download
and serial 64 B echoes, nominal 500 ms cadence with 3 s timeout. Three TCP
carriers plus QUIC share one cut: DOWN 500 Mbps/30 ms; UP 500→10→500 Mbps/70 ms
during nominal 15–25 s. There is no configured random loss, jitter or blackhole;
HTB burst/cburst is 65,536 B and netem limit is 8,192. All 82 sampled effective
profiles agree. UP is actually 1,250,000 B/s on rows 15–24 in each run:

| Cell | Runner elapsed endpoints, s | Client snapshot Unix ms | Server snapshot Unix ms |
|---|---|---|---|
| Control | 15.001880–24.004082 | 1788933110642–1788933119642 | 1788933110647–1788933119647 |
| Candidate | 15.003135–24.009323 | 1788933171078–1788933180078 | 1788933171084–1788933180083 |

The first pair improves restricted service but worsens the longest body gap
and some healthy/restored echo statistics. Promotion therefore stops. The
already-planned healthy comparison becomes one discriminator: same binaries
and workload, candidate→control order, only `NO_QOS=1`. It asks whether adverse
tails recur without the rate transition; recurring adverse healthy tails
reject the trial. All 82 healthy samples confirm 500/500 Mbps and the same
delay/burst/queue settings. No favorable-repeat selection or concurrent build
is involved. This discriminator does not replace the original adverse result.

## Complete outcomes

`R` is the return-restricted profile; `H` is the healthy discriminator.
Means below use all raw bins in the named interval, not the probe's trimmed
34-bin summary. For H, 15–25 and 25–40 remain healthy, not QoS/recovery phases.

| Cell | Body bytes / elapsed s | Whole Mbps | 0–5 / 5–15 / 15–25 / 25–40 Mbps | First body, s |
|---|---|---:|---|---:|
| R control | 1,606,508,870 / 40.000173 | 321.300 | 309.321 / 426.771 / 162.871 / 360.596 | 0.602283 |
| R candidate | 1,743,665,507 / 40.000309 | 348.730 | 295.010 / 427.538 / 237.654 / 388.150 | 0.583380 |
| H control | 2,067,376,482 / 40.003014 | 413.444 | 251.684 / 438.548 / 433.371 / 437.420 | 0.576858 |
| H candidate | 2,011,678,628 / 40.000349 | 402.332 | 314.606 / 422.776 / 416.535 / 408.479 | 0.581279 |

| Cell | Maximum body gap, s | Exact interval, s | Body counter endpoints, B | Echo success/failure | Echo p50 / p95 / maximum, ms |
|---|---:|---|---|---|---|
| R control | 0.580302 | 16.671538–17.251840 | 783,698,068–783,710,068 | 77 / 0 | 268.026 / 582.035 / 1282.519 |
| R candidate | 0.657530 | 16.998487–17.656017 | 804,101,735–804,113,735 | 80 / 0 | 306.753 / 398.251 / 614.209 |
| H control | 0.389551 | 31.476905–31.866456 | 1,601,581,410–1,601,646,946 | 80 / 0 | 323.976 / 467.854 / 606.069 |
| H candidate | 0.330935 | 22.262481–22.593416 | 1,114,385,692–1,114,451,228 | 78 / 0 | 291.257 / 561.990 / 813.918 |

Echo phases use attempt-start membership. Each entry is count / p95 / maximum
in milliseconds; all are successful. Exact complete attempt histories remain
in `probe.json`, including cadence stretch rather than imaginary missing slots.

| Cell | 0–5 s | 5–15 s | 15–25 s | 25–40 s |
|---|---|---|---|---|
| R control | 10 / 571.118 / 571.118 | 20 / 382.885 / 387.820 | 17 / 777.695 / 1282.519 | 30 / 289.449 / 392.481 |
| R candidate | 10 / 381.368 / 381.368 | 20 / 396.374 / 560.866 | 20 / 575.490 / 614.209 | 30 / 382.363 / 398.251 |
| H control | 10 / 335.804 / 335.804 | 20 / 467.854 / 509.940 | 20 / 504.634 / 606.069 | 30 / 367.822 / 379.100 |
| H candidate | 10 / 761.789 / 761.789 | 19 / 496.470 / 813.918 | 20 / 561.087 / 576.423 | 29 / 434.200 / 676.254 |

Worst echo intervals: R control 21.330251–22.612770 s; R candidate
16.067975–16.682184 s; H control 17.018654–17.624723 s; H candidate
9.337341–10.151259 s. H candidate also has 0.500514–1.262302 s startup and
36.365854–37.042108 s late echoes: its adverse tail is not only one phase edge.
All four runners exit 0 with empty probe stderr. Each receives HTTP 200 and
one deliberately duration-stopped partial 8 GiB response, with zero completed
responses; this is not four completed fixed-size transfers. Client BrokenPipe/
ConnectionReset and server RemoteClosed warnings occur at workload stop
(05:52:15, 05:53:16, 05:56:27 and 05:57:50 UTC), followed where logged by normal
QUIC connection closure. No measured echo failure or mid-workload warning is
hidden as teardown noise.

## Traffic, queues and resources

Whole values are last-minus-first sampled class counters, about 40 s. Packet
counts are kernel accounting units, not a claim about physical wire packets.

| Cell | DOWN bytes / packet units | UP bytes / packet units | DOWN / UP peak backlog, B | DOWN / UP class drops |
|---|---|---|---|---|
| R control | 2,062,626,765 / 1,528,863 | 79,901,085 / 721,271 | 13,313,276 / 854,291 | 0 / 0 |
| R candidate | 2,166,751,335 / 1,553,769 | 58,346,783 / 478,628 | 21,705,990 / 687,620 | 0 / 0 |
| H control | 2,404,550,844 / 1,781,151 | 90,434,973 / 806,767 | 24,294,304 / 306,118 | 0 / 0 |
| H candidate | 2,416,046,404 / 1,729,122 | 69,669,506 / 548,874 | 31,504,364 / 203,776 | 0 / 0 |

On actual restricted rows 15→24, DOWN/UP byte deltas are
246,466,924/10,846,081 for control and 338,178,635/10,910,380 for candidate.
UP stays approximately 9.64/9.69 Mbps; its packet-unit deltas are 97,944/86,855,
with no drops. Whole UP bytes fall 26.98%, but this does not make the restricted
cut uncongested or establish latency neutrality. Matched native epoch/instance
counter deltas over those same role snapshots are:

| Cell | Client TCP / QUIC acknowledged bytes | Server TCP / QUIC acknowledged bytes |
|---|---|---|
| R control | 3,508,677 / 1,158,628 | 106,527,176 / 153,261,671 |
| R candidate | 3,907,391 / 1,588,419 | 157,791,552 / 180,103,171 |

All four data carriers continue progressing. These are transport counters,
not unique ordered Product delivery; different allocation/copy histories
prevent treating their differences as exact saved ACK/MAX payload.

| Cell | Client RSS initial / peak / final, KiB | Server RSS initial / peak / final, KiB | Maximum sampled CPU client / server, % |
|---|---|---|---|
| R control | 31,228 / 84,124 / 77,288 | 29,744 / 422,952 / 422,952 | 103 / 193 |
| R candidate | 32,076 / 88,232 / 88,232 | 30,404 / 432,260 / 418,580 | 87.5 / 190 |
| H control | 31,808 / 95,328 / 95,328 | 30,008 / 393,820 / 393,820 | 112 / 196 |
| H candidate | 32,164 / 79,344 / 76,712 | 30,460 / 369,312 / 360,244 | 103 / 198 |

CPU is `ps` lifetime-average percentage at each observation, not instantaneous
peak. RSS is finite-run process residency, not a leak/reclamation verdict.

At nominal 16/17/18 s around the restricted body gaps, UP backlog is
630,288/183,888/637,519 B in control versus 687,620/135,850/287,680 B in candidate.
DOWN is 925,308/2,460,366/1,176,726 versus 397,427/6,006/964,865 B. The candidate's
extra 77 ms is not explained by uniformly larger sampled return queues or
observed loss; native progress continues, but the missing prefix's exact owner
and queue position are not captured. Healthy portions of the first pair have
larger candidate DOWN queues, and the no-QoS pair also raises peak DOWN backlog.
Around its worst echo, H candidate has 16.70/16.20 MB DOWN queued at nominal
9/10 s; H control has 10.67/12.94 MB at 17/18 s around its own worst echo.
These support a real queue/timing context, not an exact causal explanation.

Telemetry roles/classes are read sequentially and runner time precedes the
individual reads; probe timing has a separate startup origin. Actual restriction
rows bound cost deltas more safely than nominal bins, but do not locate an
individual frame. Q/C is a known queue's constant-service drain scale, not the
measured delay of a specific echo or a sum of overlapping waits. There is no
evidence here assigning the tail uniquely to first-poll work, native control,
Product scheduling or packing. No extra controller fix follows that ambiguity.

## Full one-second receiver body series

Each list contains all 40 consecutive bins from 0–40 s, in Mbps. Values are
rounded receiver-observation rates; a bin above 500 is not physical link
capacity and may include ordered release of earlier buffered data. These
series must not be substituted for native/class service or collapsed into
the best point or the trimmed average.

R control:

```text
2.097,164.199,206.582,773.150,400.578,503.949,317.787,552.578,407.326,425.319,
414.864,290.763,463.747,425.650,465.728,274.393,180.874,228.919,149.579,142.476,
144.302,128.715,65.226,153.292,160.934,250.861,373.793,379.147,385.532,378.267,
320.953,320.529,450.227,380.064,374.627,351.832,254.534,464.518,350.576,373.487
```

R candidate:

```text
2.620,106.748,224.063,686.714,454.905,498.022,424.609,419.258,243.892,605.296,
460.480,321.572,511.970,372.664,417.621,433.705,248.675,73.141,395.628,197.379,
215.103,212.251,229.432,181.115,190.113,257.953,303.342,424.894,369.694,406.327,
381.735,463.528,347.755,430.031,424.744,341.803,456.856,439.791,389.228,384.571
```

H control:

```text
2.620,119.611,140.745,661.472,333.971,589.159,344.019,464.441,264.854,561.978,
449.523,372.386,443.705,365.322,530.093,364.049,471.154,282.810,622.696,432.536,
367.397,492.737,467.917,433.372,399.046,480.381,463.905,383.582,468.954,458.346,
427.261,382.816,435.260,446.501,459.908,473.084,463.379,425.437,286.286,506.200
```

H candidate:

```text
3.121,196.176,231.403,654.858,487.470,421.224,294.296,542.446,416.103,447.106,
464.423,371.502,465.303,399.558,405.804,404.193,438.534,169.254,699.648,405.358,
411.404,426.002,443.319,388.622,379.016,455.696,480.360,466.075,361.278,391.711,
422.525,442.771,445.963,308.893,491.693,406.752,378.396,440.362,415.672,219.045
```

## Decision and lesson

The forecast's removable work is useful in the restricted pair: UP cost falls
and ordered throughput rises 45.9% during restriction, with lower upper echo
tails. But the same candidate fails the joint timing objective. Healthy upper
tails recur in the one predeclared no-QoS discriminator, despite an improved
median and body gap; whole healthy throughput is also 2.7% lower. This is enough
to reject under the declared gate, not proof of a universal causal regression.

Semantic preservation and fewer feedback transactions do not imply equivalent
feedback timing, queue evolution, estimator input or ordered experience. The
correct action is to preserve this mixed result and remove the candidate, not
retain its favorable throughput while adding compensating parameters. The
[global plan](CURRENT_CLOSURE_PLAN.md) owns the next bounded attribution
decision; this trial adds no new architecture or release obligation.
