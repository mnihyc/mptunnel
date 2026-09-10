# Fixed return-proof rounds: mirrored upload comparison

Recorded: 2026-09-10. Category: existing mixed return-feedback service owner.
Both uploads settle exactly. The candidate improves whole confirmed goodput
by 2.92% and the restricted-return phase by 22.40%, with slightly shorter worst
confirmation/write gaps. Healthy and restored confirmation rates are adverse;
sampled return traffic falls overall but rises after restoration. This is
bounded practical support, not global acceptance or release evidence.

## Question, provenance and effective case

Root ran ordinary optimized control `0cab2b5` then candidate `364d417`, with no
observer, suppression or build/lab overlap. The candidate removes the inherited
successor deadline: each proof round has its own fixed interval, without
changing native intervals or ACK cadence. The [active plan](CURRENT_CLOSURE_PLAN.md)
and [scoped model](SCOPED_ACK_SERVICE_MODEL.md) retain the predeclared contract.
Forecast: removing the observed serialized-proof budget may increase selected
service and reduce redundant return work; no numeric speed gain was promised.
Fresh-proof delays, physical queues and adverse timing remain possible.
Critical service harm stops promotion, without timer or profile rescue.

Inputs are the five completed files per cell under
`./.tmp/reflection/results/mixed-combined-up-return-round-{control,candidate}-0910/`:
`probe.json`, `probe.err`, `service.jsonl`, `client.log`, `server.log`.
No code change, build or lab rerun was made for this analysis.
Both five-file uploads are included in the root-listed
[37-file ordinary archive](RETURN_ROUND_ORDINARY_20260910.raw.tar.gz), alongside
the four DOWN cells and seven focused mechanism/build logs.

This is one 40 s SOCKS5 upload, not an echo workload or fixed-byte race. The
probe's `protocol=tcp-upload` names the application socket; MPP remains mixed,
with three TCP carriers plus QUIC sharing one cut. All 84 service rows report
four active paths. `REFLECTION_MIRROR_IMPAIRMENT=0` gives UP data 500 Mbps
throughout and DOWN return 500→10→500 Mbps at nominal 15–25 s. Actual router
rows verify UP 30 ms / DOWN 70 ms delay, no configured random loss, jitter or
blackhole, HTB rate=ceil, 65,536 B burst/cburst and netem limit 8,192 packets.

| Cell | First DOWN 10 sample, s | First restored DOWN 500 sample, s | Service window, s |
|---|---:|---:|---|
| Control | 15.003609 | 25.005269 | 0.000052–41.007349 |
| Candidate | 15.001738 | 25.002770 | 0.000052–41.004841 |

The runner clock starts after launching the probe; no exact shared wall anchor
is exported. Router/native/management reads are serial and management data are
cached. Phase comparisons are not packet/queue joins or identical native
histories. These ordinary records do not expose selected-output residence.

## Exact confirmation, startup and settlement

Both metric-v2 records use `target_sink_ack`, with exact and valid ACK
accounting, `complete=true`, status `ok`, exit 0, one completed stream, no failed
streams and no probe errors. Terminal sink acknowledgement verifies equality
of all locally accepted and target-confirmed bytes; local acceptance alone
would not prove this result.

| Metric | Control | Candidate |
|---|---:|---:|
| Target-confirmed bytes = local-accepted bytes | 2,064,384,000 | 2,131,492,864 |
| Whole elapsed, s | 41.533671 | 41.666659 |
| Confirmed whole goodput, Mbps | 397.631 | 409.247 |
| First local write, s | 0.105363 | 0.106063 |
| First positive target confirmation, s | 0.412094 | 0.410934 |
| Maximum confirmation gap, s | 0.483374 | 0.473387 |
| Maximum local write gap, s | 0.443350 | 0.411651 |
| Elapsed after nominal 40 s load cutoff, s | 1.533671 | 1.666659 |
| Confirmed after 40 s, approximate decimal MB | 89.667 | 118.332 |

The candidate confirms 3.25% more bytes, including more after the nominal load
cutoff. Whole goodput includes settlement. Time after 40 s also includes the
final write loop, half-close, sink acknowledgement and worker join: the last
write timestamp is not exported, so this is not an exact write-to-ACK drain
measurement. Both complete before the 50 s completion timeout.

Confirmation timestamps are positive sink-ACK arrivals at the client, not
target arrival times. Gaps include return/probe service and exclude startup,
which is retained separately. Exact gap intervals and local-write bins are not
exported. `recovery_gap_s=0` and `local_recovery_gap_s=0` with
`failover_after_s=-1` do not demonstrate zero QoS recovery delay. No echo or
short-object latency result exists in this UP workload.

| Nominal probe phase | Control mean confirmed Mbps | Candidate mean confirmed Mbps | Change |
|---|---:|---:|---:|
| Startup 0–5 s | 315.719 | 329.187 | +4.27% |
| Healthy 5–15 s | 437.461 | 411.338 | −5.97% |
| Restricted return 15–25 s | 340.206 | 416.408 | +22.40% |
| Restored 25–40 s | 429.498 | 412.126 | −4.04% |

### All 42 confirmation bins

Each row covers `[second, second+1)` in Mbps; none is trimmed. The final bin is
only partly occupied but normalized by a full second. Rates above 500 Mbps
reflect bursty confirmation of prior bytes, not instantaneous path capacity.
Phase means and post-40 s MB derive from these rounded bins; exact byte totals
come from terminal accounting above.

| Second | Control | Candidate |
|---:|---:|---:|
| 0 | 20.156 | 20.444 |
| 1 | 136.271 | 117.921 |
| 2 | 678.440 | 670.040 |
| 3 | 391.643 | 342.552 |
| 4 | 352.085 | 494.980 |
| 5 | 416.477 | 235.028 |
| 6 | 339.930 | 262.233 |
| 7 | 546.309 | 793.107 |
| 8 | 386.356 | 411.565 |
| 9 | 114.472 | 194.372 |
| 10 | 781.621 | 204.044 |
| 11 | 444.884 | 723.901 |
| 12 | 451.519 | 412.238 |
| 13 | 474.503 | 141.934 |
| 14 | 418.540 | 734.956 |
| 15 | 377.251 | 338.646 |
| 16 | 244.444 | 402.705 |
| 17 | 619.066 | 436.392 |
| 18 | 238.448 | 565.278 |
| 19 | 257.721 | 414.586 |
| 20 | 321.381 | 321.425 |
| 21 | 464.659 | 145.287 |
| 22 | 360.548 | 443.902 |
| 23 | 320.851 | 604.932 |
| 24 | 197.692 | 490.926 |
| 25 | 317.971 | 411.396 |
| 26 | 482.245 | 507.089 |
| 27 | 254.476 | 249.347 |
| 28 | 732.946 | 565.138 |
| 29 | 386.304 | 350.232 |
| 30 | 172.727 | 154.030 |
| 31 | 361.618 | 693.833 |
| 32 | 690.295 | 418.574 |
| 33 | 472.243 | 421.720 |
| 34 | 193.603 | 363.759 |
| 35 | 741.823 | 211.429 |
| 36 | 305.192 | 789.341 |
| 37 | 468.765 | 390.064 |
| 38 | 208.999 | 469.097 |
| 39 | 653.263 | 186.845 |
| 40 | 426.534 | 247.546 |
| 41 | 290.800 | 699.107 |

## Sampled traffic and queue cost

Count the sole HTB child class once per direction, as last-minus-first byte
and packet counters; both remain monotonic. Do not add parent/netem counters.
These roughly 41 s windows end before settlement: they are not exact
whole-transfer amplification ratios. Kernel packet units are offload-sensitive,
not physical packets or MPP frame counts. Return bytes include all reverse
traffic, not only logical feedback. All sampled class and qdisc drops are zero.

| Whole service window | Control | Candidate |
|---|---:|---:|
| DOWN return bytes | 51,800,838 | 44,544,230 |
| DOWN return packet units | 513,580 | 444,996 |
| DOWN sampled Mbps | 10.106 | 8.691 |
| DOWN backlog median / max, B | 87,930.5 / 608,017 | 68,854 / 250,072 |
| UP data bytes | 2,378,729,032 | 2,442,467,794 |
| UP data packet units | 1,650,952 | 1,681,752 |
| UP sampled Mbps | 464.060 | 476.523 |
| UP backlog median / max, B | 5,977,070 / 16,462,716 | 9,519,227 / 20,716,260 |

Return bytes fall 14.01%; UP bytes rise 2.68%. Interior service windows use
samples 2–14, 16–24 and 26–39 to avoid transition samples. They differ from the
probe's phase windows and cannot establish per-byte timing or amplification.

| Interior phase | DOWN Mbps C→T | DOWN backlog median / max B C→T | UP Mbps C→T | UP backlog median / max B C→T |
|---|---|---|---|---|
| Healthy, ≈2–14 s | 10.326→6.356 | 95,336 / 135,354→51,867 / 93,347 | 493.647→465.620 | 12,072,250 / 16,462,716→9,369,250 / 18,702,700 |
| Restricted, ≈16–24 s | 9.161→9.268 | 188,972 / 608,017→147,236 / 250,072 | 395.013→480.862 | 2,359,116 / 8,163,926→13,208,448 / 20,716,260 |
| Restored, ≈26–39 s | 9.676→10.939 | 65,335.5 / 143,184→108,598 / 156,610 | 478.820→491.605 | 5,327,419 / 13,633,728→11,684,378 / 18,083,434 |

Here C→T means control→trial. Restored return rate rises 13.05%, despite the
whole-window saving; UP queue medians grow markedly in the restricted and
restored windows. Neither lower byte totals nor zero drops establishes smaller
native/local delays. Queue size without the blocking byte's position is not
an observed proof-reply delay or a causal explanation of these phase changes.

## Resources and limits of the result

| Process metric | Control | Candidate |
|---|---:|---:|
| Client RSS peak / final sample, KiB | 317,668 / 305,384 | 311,880 / 305,484 |
| Server RSS peak / final sample, KiB | 123,972 / 123,972 | 121,892 / 121,892 |
| Client largest / final sampled ps %CPU | 184 / 182 | 173 / 172 |
| Server largest / final sampled ps %CPU | 107 / 107 | 97.7 / 97.5 |

`ps %CPU` is lifetime-average multicore accounting, not instantaneous CPU load.
Each role retains its PID/start identity through all 42 samples; no restart,
management error or admission rejection is recorded. Final cached client queue
bytes are 9,214,046→58,530 and server queue bytes 41,575→0, but both cells still
report one active flow at the last sample. This is not post-teardown ownership
or leak testing. Client log and probe stderr are empty. Each server log has one
later `H3_NO_ERROR` connection-close warning; the exact upload results remain
complete/error-free, without claiming an entirely warning-free process log.

The forecast is partly supported: restricted service and whole sampled return
cost improve, with exact settlement and no worse maximum measured gaps. The
healthy/restored goodput losses, longer nominal settlement tail and restored
return/UP queue costs remain adverse dimensions of this one ordered pair.
No universal speed, latency, failure-recovery or resource-reclamation claim
follows; CURRENT_CLOSURE_PLAN owns the next gate.

The [earlier UP observer](CONFIRMED_RETURN_OBSERVER_UPLOAD_20260910.md) also
found independent fresh-proof delay, including 1,942 ms from client receipt
reply admission to server logical receipt. Admission precedes native writing;
the existing record cannot divide that interval among pre-handoff service,
native transport and server-owner waiting. These ordinary results neither
re-measure nor resolve it, and do not attribute their gains to a proven increase
in selected residence. No native/timer redesign is justified by this report.
