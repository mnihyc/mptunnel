# ACK decision trace — 2026-09-09

Status: measurement-only continuation of the existing return-feedback owner.
No cadence correction, performance acceptance or release follows this capture.
The ordinary source comparator remains `b2aa215`; the temporary observer is
preserved in [ACK_DECISION_TRACE_20260909.patch](ACK_DECISION_TRACE_20260909.patch).
Root built/froze the observer and reversed its source before the run.

## Question and result

Unconditional client prewrite forcing is already source-proven. The question
here is how many changed receive-state decisions also satisfy the existing
first, gap, byte or elapsed-time predicates. This is an information gate for a
logical-generation model, not a replay of that model.

The last captured bulk prefix contains **68,524 forced-only changed decisions
out of 85,674 changed decisions: 79.982%**. In the central restricted-associated
observer window the fraction is **7,736 / 9,108 = 84.936%**. Urgent gap predicates
therefore do not dominate this captured old-clock population. Completing the
bounded logical-generation proof is justified; implementing it or predicting
an 80–85% frame/traffic reduction is not. Removing earlier forced publications
would change subsequent byte deltas, timer ages, gap exposure and sender work.

The diagnostic itself still has a 0.927526-second body read gap and a
1.618498-second echo. It is not an improvement claim.

## Artifacts and observation boundary

All raw records are retained at:

`./.tmp/reflection/results/mixed-combined-down-ack-decisions-0909/`

- `client.log`, `server.log`: cumulative decision and codec events.
- `service.jsonl`: 41 actual profile, queue, socket, management and resource
  observations. Each router string contains three concatenated JSON arrays
  followed by process text; parse arrays with `JSONDecoder.raw_decode`, not
  one `json.loads` over the entire string.
- `probe.json`, `probe.err`: complete receiver timing history and completion
  classification. `probe.err` is empty.
- Frozen diagnostic: `./.tmp/reflection/bin/ack-decisions-20260909/mptunnel`.

The observer uses one process-local fixed counter set, separated into bulk and
nonbulk. Its six-bit mask is:

| Bit | Existing predicate |
| --- | --- |
| 1 | forced |
| 2 | first ACK with received progress |
| 4 | gap-state change |
| 8 | enough contiguous bytes since the last ACK |
| 16 | progress, positive contiguous delta and elapsed ACK timer |
| 32 | changed cumulative state |

Mask33 means changed state supported only by force under the **existing**
clock. Masks37/41/45 additionally satisfy gap/byte/both. Counts are calls to
`should_send_ack`, not queue acceptance, completed native writes, independent
samples or safe substitutions. An unchanged forced generation only retries
pending attachments. Process bulk/nonbulk counters do not retain stream IDs.

The observer reuses the existing decision timestamp and predicates. It adds
no Product clock mutation, rate selection, queue policy or payload retention.
Its locking and periodic logging have diagnostic overhead. Reports are
event-driven, cumulative and approximately one second apart; no final flush
exists, so the last incomplete reporting tail is censored.

The codec hook counts each successfully validated frame encoding. Later batch
failure, cancelled handoff and native retransmission are outside its boundary.
It does not measure protected-record, TCP/UDP/IP or useful application bytes.

## Actual profile and time alignment

Every sampled DOWN class has rate=ceil=500Mbps. UP is 500Mbps in samples0–14,
10Mbps in15–24, and500Mbps in25–40. The first restricted sample has
`elapsed=15.002246528`; the first restored sample has `elapsed=25.003437853`.
The shared classes have 65,536-byte burst/cburst. All sampled netem queues have
8,192-packet limits, DOWN30ms/UP70ms delay, zero configured jitter and no
configured random loss. All `udp_blackhole` flags are false. Queue overflow
drops are observed despite zero configured random loss.

The runner sets its monotonic origin after launching the probe. It does not
record a corresponding wall timestamp. Client/server diagnostic monotonic
origins are different, and management timestamps are from a cached sampler,
not the time the runner retrieves them. Observed cache-generation increments
are999–1001ms on the server and998–1002ms on the client, but fetch/publication
latency is not recorded: this does **not** establish a hard cache-age bound.

Accordingly, the following are conservative **phase-associated observer
windows**, with exact diagnostic endpoints retained, not precisely aligned
probe/traffic-shaper phase totals. No subsecond boundary attribution is made.
Actual service costs below use the service records' own elapsed clock.

## Bulk trigger mix

| Observer window | Client diagnostic seconds | Changed | Force only | Force-only share | Gap only | Byte only | Gap + byte |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Healthy-associated interior | 3.104–14.111 | 21,972 | 14,223 | 64.732% | 6,332 | 275 | 1,142 |
| Restricted-associated central interior | 17.112–23.114 | 9,108 | 7,736 | 84.936% | 1,162 | 0 | 210 |
| Restricted-associated wider interior | 16.111–24.115 | 12,489 | 10,621 | 85.043% | 1,571 | 13 | 284 |
| Restored-associated interior | 26.115–39.119 | 35,433 | 31,094 | 87.754% | 3,103 | 208 | 1,028 |
| Last cumulative captured prefix | through40.120 | 85,674 | 68,524 | 79.982% | 13,418 | 864 | 2,868 |

The central interval is exactly UNIX milliseconds
`1788964779316–1788964785318` (6.002s). Its remaining calls are12,091mask0 and
2,983mask1; changed calls have no first or timed-delivered bit. Whole bulk
counts are206,720calls:103,360mask0,17,686mask1 and85,674changed calls. The lack
of bulk timed-delivered decisions is **not** proof that a replacement timer
would be unnecessary: the current forced prewrite call continually advances
the ACK clock.

Whole-prefix bulk step range is65,536–125,848bytes; raw
`max(delivery_rate_bps, product_progress_rate_bps)` ranges151,576–4,414,144bps,
with no absent snapshot. These extrema are cumulative, not interval extrema,
and do not identify measured/native/startup provenance. The raw rate precedes
the capacity helper's final minimum-one clamp. Nonbulk step is1byte; its last
prefix has117changed decisions and no forced-only changed decision. The server
has only nonbulk decision reports in this workload, not a bulk-upload sample.

## Codec and class costs

Latest client codec prefix, at UNIX1788964802324–1788964802325ms:

| Encoded kind | Frames | Plaintext codec bytes |
| --- | ---: | ---: |
| STREAM_ACK | 343,006 | 9,585,118 |
| STREAM_MAX_DATA | 206,903 | 5,379,478 |
| All frame kinds | — | 14,975,069 |

Every encoded ACK contains one positive range;16,500have a scope and326,506
do not. A one-range scoped ACK can still describe a genuine gap. There are no
encoded multi-range ACKs in this prefix; this does not prove that the receiver
never held a larger cumulative ledger or remove missed-generation obligations.

The central codec interval is separately
UNIX1788964779210–1788964785214ms, client diagnostic17.006–23.010s (6.004s).
It contains36,346ACKs /1,018,216bytes (1.357Mbps) and23,926MAX frames /
622,076bytes (0.829Mbps). Of those ACKs,2,172are scoped. Its endpoints differ
from the decision interval; do not divide their counts into an exact fanout.

The latest server codec prefix has105,195STREAM_DATA encodings /
2,064,285,478bytes,157ACKs /3,763bytes and165MAX frames /4,290bytes.
These do not distinguish Originals from recovery or identify winning receipt.

Class counters below use one HTB child per direction, never adding its parent
or netem counters. Bytes are **tc class-accounted traffic**, not Product bytes.

| Service interval | Direction | Byte delta | Packet delta | Accounted Mbps | New drops | Backlog min / median / max, bytes |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Whole0.000057–40.005218s | DOWN | 2,174,489,044 | 1,580,048 | 434.842 | 0 | 132 /10,469,558 /24,275,025 |
| Whole | UP | 61,847,164 | 559,030 | 12.368 | 2,365 | 0 /133,789 /902,987 |
| Healthy2.000328–14.001872s | DOWN | 732,990,720 | 519,596 | 488.598 | 0 | 1,229,754 /14,320,969 /24,275,025 |
| Healthy | UP | 16,544,262 | 151,950 | 11.028 | 0 | 67,358 /103,970 /144,201 |
| Restricted16.002356–24.003335s | DOWN | 276,290,176 | 211,628 | 276.256 | 0 | 478,944 /1,112,986 /2,271,924 |
| Restricted | UP | 9,875,481 | 89,861 | 9.874 | 446 | 96,081 /433,497 /902,987 |
| Restored26.003549–39.005094s | DOWN | 809,550,258 | 591,080 | 498.126 | 0 | 4,525,424 /14,651,566 /20,216,144 |
| Restored | UP | 26,210,619 | 234,276 | 16.128 | 0 | 71,188 /141,202 /206,854 |

The restricted return class remains nearly saturated with queued traffic and
overflow drops. Codec overhead does not account for all that work: protection,
packetization, native feedback/recovery and other traffic remain in the class.
The different observation endpoints prohibit exact codec-to-wire subtraction.

## Receiver timing, completion and resources

The single HTTP request has an8GiB content length and intentionally runs for
40seconds:1,809,382,373body bytes in40.001605s,361.861951Mbps. It is one partial
request, zero completed full-object requests, not a completed8GiB download.
HTTP200 and probe/bulk status are `ok`. First body appears at0.595943s.
The maximum read gap is0.927526s, from16.942428 to17.869954probe seconds.

All77interactive attempts succeed; no timeout/failure is hidden. Echo
median/p95/max are334.830/548.221/1,618.498ms; maximum successful-completion gap
is1.618516s. Full77attempt start/end/latency records remain in `probe.json`.
Nominal probe-clock0–15/15–25/25–40s body-bin means are
388.860/234.576/419.725Mbps; these are not exact shaper-clock phase boundaries.

All40raw one-second receiver bins are retained below, without startup/end
trimming or replacing the adverse intervals:

```text
0–14:  2.097 156.002 184.463 717.090 442.647 428.717 418.460 443.580
       344.270 368.153 544.951 447.645 406.114 480.312 448.399
15–24: 383.453 265.061 28.202 480.946 184.783 209.170 194.873 228.715
       204.701 165.855
25–39: 360.901 457.481 422.778 401.395 406.295 219.747 611.203 429.604
       424.421 467.070 371.974 406.948 416.866 439.355 459.843
```

Bins describe buffered application reads, not a claim that instantaneous link
capacity exceeds its500Mbps class. Aggregate class traffic is reported above.

Across41process samples, server RSS peaks at351,912KiB and ends at331,768KiB;
client RSS peaks/ends at88,968KiB. Sampled server/client lifetime `%CPU` peaks
are186.0/87.9. `ps %CPU` is a process-lifetime average, not interval CPU or a
proof of a specific bottleneck. Final cached management still reports two
active flows on each endpoint, zero failed flows and zero admission rejects;
this capture is not a post-teardown reclamation proof.

Two end-of-observation warnings remain explicit: client connection reset by
peer at UNIX1788964802101ms and server RemoteClosed stream reset at
1788964802173ms. They are consistent with terminating an intentionally partial
body, but the observer does not join them to a specific flow. No additional
WARN/ERROR/panic line occurs in these logs. Do not call the logs error-free or
infer a failed probe from these warnings alone.

## Decision and retained model obligations

The information gate supports completing the logical ACK-generation model:
forced-only changed decisions are material in the captured old clock, including
the restricted-associated interior. It does not select a runtime change or
promise service improvement. MAX cost, native contention and forward queues
remain competing contributors. A candidate must preserve the existing scoped
receipt truth and immediate independent fanout of every materialized generation.

One unmaterialized dirty-receipt owner must remain serviced while the **same**
application write/flush future is pending. Its deadline cannot be renewed by
new bytes, MAX, retries or unrelated ready work. A due sparse extension must
publish even when the contiguous frontier does not move; the current unforced
timer predicate alone does not do this. First/Latency/terminal/gap feedback,
startup control, independent exact-attachment capacity retries and no early
MAX credit remain required. Missed-generation multi-chunk cursor restart is
still a separate conditional catch-up limitation, not solved by fewer logical
generations. RFC8.3 must be explicitly revised before any deferred cadence.

The rejected ready-receipt merging, first-poll pairing and deferred-sibling
models are not reinstated. Full timing, completion and resource composition
still decide any future candidate; these counters are not acceptance.
