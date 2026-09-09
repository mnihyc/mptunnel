# Scoped ACK: first ordinary control/candidate pair

Updated: 2026-09-09 09:52 +08:00. Category: mixed-mode feedback/stall evidence.
**Partial benefit; not performance acceptance, stall closure, or release authority.**
Global scope remains [CURRENT_CLOSURE_PLAN](CURRENT_CLOSURE_PLAN.md).

## Transaction, model and forecast

The observed defect is mixed TCP+QUIC downstream collapse when its return link
is temporarily narrowed, while matched QUIC/raw/H2 controls retain high service.
The [scoped model](SCOPED_ACK_SERVICE_MODEL.md) replaces repeated full received
range histories with independent positive updates and explicitly scoped missing
intervals. The receive stream owns dirty normalized range nodes; exact attachment
cursors retain cumulative catch-up for blocked, missed or replaced outputs.
Positive ACK release precedes gap intersection with the real send cache. Empty
negative scopes are omitted algebraically, not by a traffic threshold.

Control is ordinary runtime `4c7e232`; candidate is the uncommitted wire-v14
model frozen as `.tmp/reflection/bin/scoped-ack-20260909/mptunnel`. Both are
default optimized builds without the codec observer. No controller parameters,
clock thresholds, feedback fanout, queue replacement or link-profile change is
stacked here; changed ACK granularity can still change observed timing history.

The pre-change conditional forecast held frame count fixed: 35 B per one-range
update could reduce 47.375 MB of previously observed ACK encoding to 13.870 MB
(~71%), or ACK+unchanged MAX encoding by ~61%. That was not a promised wire or
goodput multiplier: faster service creates more updates, and native overhead,
multi-range changes and cumulative catch-up remain. This pair tests practical
service and costs, not merely that set equations or byte savings look correct.

Independent producer/consumer audits and 1,241 affected tests pass; the scoped
name filter separately passed 25 tests, including unrelated matching names.
The earlier affected run had 1,236 passes and five fixture failures: two still
supplied positives as gaps, one expected an omitted empty scope, and two
invented negative authority up to the assigned tail. The fixtures were corrected
to real scoped evidence without widening runtime recovery. These checks support
semantic integration, not performance acceptance or exhaustive test coverage.

## Matched configuration and accounting

Both runs: one 40 s HTTP download plus serial 64 B echo exchanges every 500 ms,
3 s echo timeout; default mixed three TCP carriers and one QUIC carrier share
one cut. Router DOWN stays 500 Mbps; UP changes 500→10→500 Mbps for 15–25 s.
Delay is DOWN 30 ms / UP 70 ms, zero configured jitter or random loss, no UDP
blackhole. Both use HTB 64 KiB burst and netem 8,192 packet queues. Sampled
UP rate is 1,250,000 B/s on rows 16–25; DOWN is 62,500,000 B/s throughout.
This is return-link restriction, **not** a DOWN capacity-recovery experiment.

Raw results are `mixed-combined-down-scoped-ack-{control,candidate}-0909` in the
[19-file archive](SCOPED_ACK_ORDINARY_20260909.raw.tar.gz): both five-file result
directories, run/build/check logs and focused/initial/final test logs. Every
40-bin body series and every echo attempt remains in its original `probe.json`;
no trimmed series is used below. `service.jsonl` retains all shape and resource
samples. Parent and child qdisc counters overlap and must not be added.

## User-visible outcome

| Measure | Control | Candidate |
|---|---:|---:|
| Body bytes / elapsed s | 1,635,702,821 / 40.000423 | 1,682,150,995 / 40.000037 |
| Whole-run Mbps | 327.137 | 336.430 |
| Raw bins 5–15 s, Mbps | 441.551 | 427.275 |
| Restricted 15–25 s, Mbps | 104.527 | 185.569 |
| Restored 25–40 s, Mbps | 411.605 | 385.708 |
| First body, s | 0.587153 | 0.635090 |
| Longest body read gap, s | 2.249929 | 0.811220 |
| Successful / failed actual echoes | 34 / 1 | 76 / 0 |
| Unattempted after disconnect | 35 | 0 |
| Successful echo p50 / p95 / max, ms | 348.526 / 736.810 / 2225.260 | 250.174 / 580.197 / 1479.040 |

HTTP 200 means one duration-partial body in each run, zero completed 8 GiB
requests. Control's actual timeout is attempt 34, 19.954176526→22.954309976 s
(3.000133450 s); its next 35 disconnected slots are not failed network attempts.
There is no control echo-recovery proof after that socket closes. Candidate's
76 successes are all attempted exchanges, not 80 fixed-cadence opportunities:
slow exchanges delay the serial schedule.

Control's longest body gap is 22.724777281→24.974706400 s, byte positions
863,787,761→863,853,297. Candidate's is 16.823837405→17.635057426 s,
787,187,719→787,199,719. Its worst echo is attempt 32,
16.155896930→17.634936846 s. Application delivery bins can exceed configured
link rate while buffered/reordered bytes become readable; they are not physical
capacity measurements. The full series includes control's zero bin at 23–24 s.

## Physical service and resource cost

| Sampled measure | Control | Candidate |
|---|---:|---:|
| Service rows / last elapsed s | 40 / 39.464046 | 41 / 40.004978 |
| DOWN class byte / packet deltas | 1,962,247,289 / 1,412,484 | 2,102,402,646 / 1,545,496 |
| UP class byte / packet deltas | 118,606,438 / 556,903 | 81,780,513 / 731,328 |
| DOWN / UP drops | 0 / 7,613 | 0 / 418 |
| Peak DOWN / UP backlog, B | 18,434,538 / 3,913,683 | 17,199,645 / 922,835 |
| Client RSS peak / last, KiB | 87,276 / 80,816 | 87,160 / 78,252 |
| Server RSS peak / last, KiB | 317,988 / 314,516 | 403,688 / 354,920 |
| Client / server peak `ps %CPU` | 98.3 / 173 | 109 / 191 |

These are first-to-last HTB class counters, not exact probe-window or MPP ACK
bytes; telemetry windows differ by ~0.54 s. `ps %CPU` is process-lifetime
average, not interval CPU. Higher sampled RSS is not evidence of a memory leak.
Candidate has fewer UP bytes but more UP packets, higher CPU and higher server
RSS. No exact wire-efficiency improvement follows from body totals alone.

The restricted interior, row 16→25, drains 10,951,513 B in 9.001064479 s
(9.733527 Mbps) for control and 11,006,708 B in 9.001054790 s (9.782594 Mbps)
for candidate. Both remain queued at every restricted sample. Exact successive
class-byte deltas below use approximate row elapsed windows; collection of
endpoint management and router counters is sequential, so individual computed
rates can overshoot 10 Mbps without establishing a shaper violation.

| Approx. s | Control ΔB / ending backlog B / drops | Candidate ΔB / ending backlog B / drops |
|---|---:|---:|
| 15→16 | 1,059,485 / 1,516,920 / 0 | 1,014,655 / 881,317 / 418 |
| 16→17 | 1,195,382 / 767,890 / 0 | 1,244,194 / 377,105 / 0 |
| 17→18 | 1,214,103 / 1,320,138 / 0 | 1,344,705 / 709,872 / 0 |
| 18→19 | 1,439,627 / 3,913,683 / 2,299 | 1,152,985 / 671,881 / 0 |
| 19→20 | 1,441,687 / 3,261,734 / 1,601 | 1,651,069 / 672,620 / 0 |
| 20→21 | 962,078 / 2,709,052 / 624 | 818,943 / 673,571 / 0 |
| 21→22 | 1,190,137 / 1,903,480 / 65 | 1,319,942 / 341,662 / 0 |
| 22→23 | 1,138,177 / 1,794,710 / 0 | 1,186,866 / 922,835 / 0 |
| 23→24 | 1,310,837 / 649,296 / 0 | 1,273,349 / 892,737 / 0 |

## Decision and next discriminator

Restricted goodput and read-gap/echo outcomes improve materially, but the return
link remains pressured, a 1.479 s echo persists, and restored goodput is ~6.3%
lower in this pair. The earlier matched QUIC/raw/H2 restricted results remained
~441–476 Mbps. This does not prove stall closure, no downgrade or competitiveness.
It also does not establish that ACK encoding still dominates the residual.

Next: reuse the existing 69-line, two-file codec observer, rename its old
`ack_complete` count to `ack_scoped`, and capture one identical mixed cell.
Same-kind frame/encoded-byte/range counters separate remaining repeated range
support, ACK frequency and MAX costs. Encoding is not successful wire delivery;
native ACKs, retransmission and transport overhead remain outside that counter.
Freeze the observer build and reverse its source before capture. Low encoded
ACK cost rejects further ACK-serialization work; dominant encoded ACK cost
selects producer/publication service investigation, not congestion thresholds.
README/PERFORMANCE and release claims remain deferred pending practical proof.
