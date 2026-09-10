# Confirmed-return participation: DOWN observation

Recorded:2026-09-10. Category: existing mixed return-service candidate.
**Actual selection churn is established; performance promotion remains held.**
The capture identifies an inherited-successor proof budget that expires even
with prompt, individually timely exchanges. It also records genuinely slow
post-admission service. These are separate causes; fixing the former cannot
claim to remove the latter or establish competitive user performance.

## Contract and provenance

The information forecast in CURRENT_CLOSURE_PLAN was to distinguish failed
confirmation/mostly-full-fanout service from effective selection whose costs
remain dominated elsewhere. This is one diagnostic DOWN capture, not a new
ordinary comparison. The [ordinary trial](CONFIRMED_RETURN_ORDINARY_20260910.md)
retains its adverse phases and unaccepted disposition.

Feature observer checkpoint `f2481d0` observes candidate `0cab2b5` without
changing its policy. Five source files add session/stream-scoped transitions,
actual marker admission and logical-owner/reply boundaries; no DATA tracing or
per-retry event was introduced. The frozen feature build took1m07s, with the
existing unused-helper warning. The frozen observer executable was explicitly
used in both captures; the source overlay was not reversed, and the ordinary
binaries remained untouched in separate paths. Inputs are all five files in
`./.tmp/reflection/results/mixed-combined-down-confirmed-return-observer-0910/`;
build log is `./.tmp/reflection/confirmed-return-observer-build-0910.log`.
The root-created, tar-listed
[raw archive](CONFIRMED_RETURN_OBSERVER_20260910.raw.tar.gz) contains11files:
both five-file capture cells and the feature build log. Raw events, all75DOWN
echo attempts and all41DOWN service rows remain the primary record.
No README/PERFORMANCE claim changes.

Join keys are `(session, protocol stream, initiating role, token)`; the same
token number in the opposite direction is unrelated. One session contains
bulk protocol stream1 and echo stream0. Bulk desired MAX reaches1,894,346,102B,
versus67,113,664B for echo, including the initial64MiB credit. Management flow
IDs2/1 respectively are display identities, not protocol stream IDs.

Local `seq` orders same-process events. Cross-role joins below use same-host
Unix milliseconds with millisecond quantization, **not** the two independently
zeroed `t_mono_ms` clocks. Deadline values are relative to the event's actual
local observation instant. Per-output identity includes physical instance and
attachment/incarnation; client and server namespaces are not interchangeable.

## Actual participation and residence

Client bulk emits441created/admitted probes. All441reach the actual server
logical owner and all441obtain reply admission. Every observed owner already
has `applied_peer_max_offset >= required_max_offset`; there is no measured
MAX-barrier wait. Client observes435receipts:99valid confirmations and336ignored.
Six replies are not observed before terminal and are right-censored, not
proved lost. Server bulk creates no reverse probe: its small HTTP request
direction has only initial feedback, not a missing bulk trace.

Ignored receipts are not synonymous with failed proofs. Of336,118have an
explicit expiry of that token;218are obsolete without their own expiry, such
as losing discovery siblings. There are120actual expiry events:76while selected
and44during discovery; two expired-token receipts are censored. There are
76entries into selection and76returns to full fanout. Of99valid confirmations,
93prove TCP outputs and6prove QUIC outputs.

Integrating the exact client route transitions from first bulk probe through
terminal gives the following residence. Initial/final intervals outside that
recorded window are not extrapolated. Interior phase anchors consistently use
cached **client** management Unix timestamps, safely inside the shaper phases;
they are not an exact wall-time reconstruction of the shaper's transition.

| Window, Unix ms | Full fanout, ms | Selected TCP, ms | Selected QUIC, ms | Selected share |
|---|---:|---:|---:|---:|
| 1789010841991–1789010881280 | 24,740 | 13,718 | 831 | 37.03% |
| Healthy interior1789010846224–1789010855224 | 5,775 | 2,853 | 372 | 35.83% |
| Restricted interior1789010857224–1789010865224 | 4,699 | 3,301 | 0 | 41.26% |
| Restored interior1789010867224–1789010880224 | 7,864 | 5,136 | 0 | 39.51% |

These are publication-policy residence, not fractions of traffic or time in
which an underlying carrier is healthy. New-output baseline and terminal
exceptions still exist while selected.

## Where proof time goes

Quantiles use the probe convention: sorted index `round((n−1)*rank)`, without
interpolation. Values below are elapsed milliseconds, not CPU execution time.

| Stage | Joined count | Median | p95 | Maximum |
|---|---:|---:|---:|---:|
| Creation→local admission | 441 | 0 | 1 | 2 |
| Admission→opposite logical owner | 441 | 73 | 614 | 1,655 |
| Logical owner→reply admission | 441 | 0 | 1 | 4 |
| Reply admission→any observed receipt | 435 | 226 | 895 | 6,744 |
| Creation→any observed receipt | 435 | 328 | 1,612 | 6,837 |
| Creation→valid receipt only | 99 | 224 | 405 | 881 |
| Creation→ignored receipt only | 336 | 366 | 2,185 | 6,837 |

Successful-only timing is selection-conditioned and cannot represent every
probe. Admission is queue acceptance, not native transmission. The long
post-reply interval includes output scheduling, native/physical service,
decode and local actor delivery; this observer does not split those stages.
It therefore rules out multi-second reply-admission waits in these joined
events, not all local-service or native-queue causes.

### Exact serialized-successor counterexample

Bulk token7 and its successor11 use the same client TCP output
`index0/physical4/attachment0`, mapped here to server `TCP path1/incarnation1`.
All rows below share that one session and initiating direction. Times are Unix
milliseconds minus1789010840000; `C` and `S` identify independent local sequences.

| Event | Relative ms | Local seq | Relevant value |
|---|---:|---|---|
| Token7 created/admitted | 2454 | C27/C31 | Initial interval175.538ms |
| Token7 successor anchored | 2459 | C35 | Frozen interval175.609ms |
| Token7 logical owner applied / reply admitted | 2524/2525 | S26/S27 | Required/applied MAX67,892,114B |
| Token7 selected | 2555 | C41 | Successor remaining79.848ms |
| Token11 created/admitted | 2555 | C42/C43 | Inherited remaining79.792ms |
| Token11 logical owner applied / reply admitted | 2625 | S39/S40 | Required/applied MAX68,550,864B |
| Token11 expires, selection cleared | 2636 | C61 | Deadline overdue1.040ms |
| Token11 receipt arrives, ignored | 2655 | C74 |100ms after creation |

Both proof exchanges take approximately101/100ms. Admission is immediate and
the second exchange traverses the configured70ms forward/30ms reply delay
without material extra residence. Nevertheless, the successor starts only
after its predecessor's receipt, having already spent about96ms of its own
175.609ms budget. Its79.792ms remainder cannot accommodate another100ms trip.
This establishes a candidate route-policy timing consequence, not a byte,
credit or cache-reclamation violation.

Symbolically, for round-trip `D`, native interval `P`, and successor facts at
`t+epsilon`, the next probe starts near `t+D` with only `P+epsilon−D` left.
When `D < P < 2D−epsilon`, individually timely exchanges can still clear
selection. No larger native interval or renewed per-fact timer is justified
by this example. The model decision belongs to CURRENT_CLOSURE_PLAN.

This is not an isolated admission artifact:98observed selections materialize
an inherited successor. Their creation budgets range7.318–1,028.672ms, with
arithmetic median137.525ms;31are below100ms. Outcomes are23valid confirmations,
74ignored receipts and one censored receipt.75explicitly expire. Those75
account for75of76selected→fanout losses. This does **not** establish that every
one would succeed without inheritance: many also encounter long real service.

### Slow actual service remains independently present

The other selected loss is fresh token2: creation/admission at relative2093ms
(C7/C8), server owner/reply2164ms (S8/S9), expiry2270ms (C10), ignored receipt
2529ms (C39). Its full175.827ms budget is exceeded by a436ms trip;365ms occurs
after reply admission. Removing successor serialization cannot explain away
that wait.

A later discovery token235 is created/admitted at Unix1789010866397
(C1712/C1716), applied/replied at1789010866490(S1464/S1465), but received only at
1789010873234(C2277):6,837ms total,6,744ms after reply admission. Its token is
obsolete rather than explicitly expired itself. A sibling selecting earlier
does not erase the observed late service. No exact packet/byte queue position
or native transmission boundary was captured, so attributing all6.744s to a
particular queue or controller would exceed the evidence.

## Workload, costs and limits

All41service rows verify DOWN500Mbps, UP500→10→500Mbps,30/70ms delay, zero
configured random loss/jitter and no blackhole. HTB rate equals ceil,
burst/cburst65,536B; netem limit8,192packets. First sampled UP10/UP500 restoration
are service elapsed15.001805/25.002885s. Service spans0.000047–40.005453s;
the probe and sampled management timestamps do not share an exact origin.

Actual queue overflow is **not zero**: UP class drops rise3,367, while DOWN
drops remain0. UP drops first appear at service19.002229s, rise again around24s,
and reach3,367by25.002885s. Whole class bytes are2,260,000,462DOWN/66,441,374UP;
peak backlogs30,086,426/1,053,701B. Strict interior service rows16–24 account
for366,362,191DOWN/9,941,240UP bytes. Count one HTB child per direction, not
parent plus child/netem; offloaded packet counters are not physical packets.
Class counters cannot attribute bytes to ACK/MAX/Probe/Receipt individually.

Client peak/final RSS94,132/79,700KiB, server356,520/356,520KiB; maximum/final
lifetime `ps` CPU100/100% and198/198% respectively. These are sampled process
costs, not CPU task time or evidence of post-teardown retention/leak.

Probe status is `ok`, HTTP200:1,831,258,934body bytes in40.000264s,
366.249Mbps, one duration-stopped partial8GiB object and zero complete objects.
First body is0.611207s. Maximum read gap0.452663s spans16.687866–17.140530s,
counter827,591,762→827,657,245B. All75serial echo attempts succeed; p50/p95/max
282.872/679.338/1,750.876ms. The worst attempt37spans18.939312–20.690188s;
maximum successful-completion spacing is1.750894s. Fewer attempts reflect
serial service, not silently missing fixed-cadence failed slots. Probe stderr
is empty. Client Broken pipe follows bulk terminal by1ms; server RemoteClosed
and H3_NO_ERROR follow teardown, not recorded probe failures.

| Nominal probe phase | Body mean Mbps | Echo count | Echo p50 / p95 / max, ms |
|---|---:|---:|---|
| 0–5s | 309.477 | 10 | 223.748 /374.633 /374.633 |
| 5–15s | 421.803 | 20 | 326.310 /509.657 /560.894 |
| 15–25s | 314.505 | 15 | 457.581 /872.533 /1,750.876 |
| 25–40s | 382.634 | 30 | 239.010 /395.999 /404.994 |

All40raw body bins follow, in one-second order (Mbps); no trimming. Buffered
application-read bins above500Mbps are not instantaneous link capacity.

```text
1.049 99.772 238.315 738.242 470.006
401.880 350.901 556.225 419.379 411.445
440.895 351.891 459.798 465.147 360.471
579.156 276.163 323.330 409.085 168.501
207.251 220.139 228.166 414.094 319.168
59.367 672.746 409.296 393.013 394.405
387.117 408.170 407.724 392.693 359.043
362.583 286.128 450.341 368.518 388.364
```

The information forecast succeeds: full fanout dominates and the exact
serialized-successor budget demonstrably destroys a valid selection. It is
not sufficient evidence that removing that budget yields a specific Mbps gain,
that every proof miss is avoidable, or that physical/shared queue pressure has
been resolved. Ordinary affected DOWN/UP timing, completion, return cost and
selected-output failure service remain necessary before promotion.
