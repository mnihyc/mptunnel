# Mixed recovery: ordered frontier versus carrier service

2026-09-06 09:13 UTC. Status: causal frontier and native-queue evidence;
refusal predicates adjudicated; allocation and writer interaction remain open.
No new runtime policy is
accepted. Continue CURRENT_CLOSURE_PLAN, not a separate optimization inventory.

## Exact mixed counterexample

The diagnostic binary combines the archived receive/excess candidate with the
Native/Product qualification projection correction. The run
`mixed-combined-down-qualified-frontier-trace-0906` has the documented 500/500
Mbps routed service, asymmetric jitter/loss, 15--25 s downstream QoS reduction
and 30--33 s UDP outage. It does not overlap compilation. Its average is
64.758 Mbps, first body .584 s and maximum application read gap 5.542 s.

The longest observed Product hole is `[297186476,297252012)`. Times below are
relative to the first diagnostic event, not the router's first sample.

| Time (s) | Exact event |
| --- | --- |
| 15.008 | Original 65,536 bytes assigned to TCP output 0. |
| 29.532 | Client exposes this missing frontier with a later received suffix. |
| 29.567 | The full range is reinjected on TCP output 2. |
| 29.769 | A 14,600-byte prefix is reinjected on QUIC output 0. |
| 30.378--34.737 | Stale recovery repeatedly reports no target for the retained range. |
| 34.737 | Another full-range copy is admitted to TCP output 1. |
| 35.073 | The client frontier advances; up to 10,539,360 later bytes were held. |

The application clock reports 29.563--35.105 s. Its HTTP-body offset differs
from the Product offset by the 208-byte response header. These clocks and
offsets must not be silently equated. Assignment timestamps also do not prove
native transmission or identify which physical copy delivered the bytes.

The trace commits 361,233,706 original bytes through QUIC and 26,373,070
through TCP. Repair commitments are 40,759,056 TCP bytes and 1,524,824 QUIC
bytes. A stale-owner evaluation at 9.129 s queues 1,026 frames after ranking
one 14,936-byte preview; an evaluation at 38.132 s queues 1,030 frames.
Queueing intent is not acceptance or wire amplification, but the accepted
repair total and kernel socket backlogs establish actual alternate work too.

At 30 s the three TCP sockets retain about 2.79, 4.02 and 3.86 MB of send
queue, mostly not-yet-sent data. At 34 s they still retain 1.17, 3.28 and
1.81 MB. TCP is supplying service throughout the UDP outage; this is not a
simultaneous all-carrier outage. The TCP-1 published native observation remains
unchanged for about 20 s although its kernel ACK counter advances. The actor
polls native metrics outside its pending writer transaction; a pending commit
does not service that observation timer. This proves an observation blind
spot, not yet that refreshing it would make the Product target eligible.

## Model boundary and why an arbitrary cap is not a fix

Ordered delivery implies `delivered_frontier <= first_missing_offset` even
when later QUIC data has arrived. Shared physical contention is a second,
independent route by which TCP can affect QUIC. Neither fact grants MPP
permission to let stale metadata or avoidable speculative work create long
stalls. Native throughput estimates are not application goodput guarantees.

T05 deliberately removed percentage-based reachability and retained exact
range/slot ownership. T06 corrected unranked expansion for active-live gap and
tail repair, explicitly leaving stale-output handoff separate. This run
exposes that separate branch; it does not show that T06 was reverted. RFC 15.2
describes bounded stale handoff, but its concluding live-owner/frontier wording
is broader than the implementation's distinction. Resolve that semantic
boundary explicitly if the handoff model changes; do not silently copy a
threshold into this branch.

For an ordered carrier with `B` earlier bytes and available service `r`, even
without further loss a later repair needs at least `8B/r` seconds to reach the
front. Four MB at 4 Mbps costs eight seconds. Conversely, replacing bulk
handoff with stop-and-wait 64 KiB repair at 100 ms feedback limits recovery to
about 5.24 Mbps. Both are unacceptable as universal policies. A capacity
ceiling in bytes is not proof of prompt frontier service; a one-frame cap
alone is not a sustained recovery model either.

Next: identify each refused target's exact occupied-range, qualification,
queue and Product-service predicates, alongside native ACK/write progress.
Then distinguish necessary ordered/native delay from avoidable stale handoff
or target exclusion. Preserve exact-copy ownership, terminal-failure liveness,
qualification separation and the existing startup/frontier protections. No
fixed QUIC preference, percentage gate, enlarged timer or smaller buffer is
authorized by this evidence. Remaining upload, aggregation, browser and
sustainability gates stay open.

## Refusal adjudication and stronger allocation counterexample

The next trace, `mixed-combined-down-qualified-repair-refusal-0906`, keeps
ordinary model behavior and adds only opt-in diagnostics. It averages64.783
Mbps, with a7.153 s maximum application gap. The longest observed Product
hole starts at242855434: TCP2 originally owns it; QUIC and TCP0 already hold
copies; TCP1 is stale. Refusal details confirm those exact occupied/stale and
queue predicates. There is no evidence here that the distinct-copy exclusion
or Product-capacity subtraction is falsely making a healthy vacant target
unavailable. Removing those predicates is rejected. Single TCP writer commits
take12.717 and12.706 s, confirming that a one-frame writer transaction does
not bound time behind an existing native queue.

The earlier frontier trace also captures the original placement decision for
297186476. At15.008 s, the live QUIC frontier owner's legacy ETA is2382.055 ms.
It is absent from the queue-admitting candidate set; its previous sample has
about8.07 MB of command backlog. The admitted TCP candidates have ETAs95868.855,
109066.378 and131840.253 ms. The scheduler chooses TCP0, even though it retains
the much faster QUIC owner as its advisory reference. That exact TCP range
subsequently blocks the observed frontier. The next bulk original on QUIC is
at19.264 s. These estimates are not literal service bounds: changing placement
would also change the following queue history. The trace establishes the
decision and its consequence, not a measured counterfactual speedup.

This behavior conforms to the current Profile7 rule forbidding the advisory
order from declining the only immediately admitted action. It is therefore an
allocation-model limitation, not just a lost comparison in code. Rank-after-
queue-filtering is work-conserving for individual carriers but need not be
completion-efficient for an ordered logical stream. A scheduling correction
must revise that explicit RFC policy while preserving genuine singleton and
failure fallback. It must not silently reinterpret an ETA as resource credit.

The uninstrumented asymmetric variable-loss/jitter ablation with no QoS step
and no outage produces150.994 Mbps mixed versus164.407 Mbps QUIC. Both retain
80/80 echoes, but mixed has a3.442 s read gap and three zero-delivery bins;
QUIC's maximum gap is.507 s. The subsequent668.816-Mbps application bin is
buffered ordered release, not throughput exceeding the500-Mbps physical cut.
Thus the mixed stall is not explained solely by the deliberate QoS/outage.
These are single random trials, not a precise non-downgrade ranking.

The proposed next model boundary is documented separately in
BOUNDED_PLACEMENT_DEFERRAL_PROPOSAL.md. It is not yet implemented or accepted.

The score comparison is additionally limited by TCP's compatibility startup
rate, not a fresh operational capacity value. T03 already proved that a static
winner or a repeatedly renewed winner can starve underestimated independent
capacity. Even a finite wait per source head does not cure that starvation.
The proposal records this counterexample before implementation; no ETA-based
wait, smaller queue, new traffic cap or protocol preference has been applied.

## Baseline control, not a release ranking

MIXED_FRONTIER_EVIDENCE_20260906.json retains every one-second application
series and individual echo outcome for the two diagnostic and five ordinary
cases. The ordinary cohort keeps asymmetric variable loss AND jitter, with
QoS reduction and UDP outage disabled. The `variable-loss-only` case tag does
not mean jitter was disabled. Linux reports the native TCP controller `bbr`;
Hysteria2 requests 500 Mbps in both directions. Runs are sequential and do not
overlap compilation. They do not replay identical packet-loss realizations.

| Ordinary case | Mean Mbps | First body s | Maximum read gap s | Echo p95 ms |
| --- | ---: | ---: | ---: | ---: |
| MPP mixed candidate | 150.994 | .574 | 3.442 | 287.9 |
| MPP QUIC candidate | 164.407 | .564 | .507 | 285.6 |
| Raw TCP | 3.423 | .561 | .364 | 406.6 |
| Xray | 4.590 | .446 | .999 | 408.5 |
| Hysteria2 | 10.034 | .399 | 25.009 | 496.4 |

Hysteria2's bulk stalls while its separate echo remains live in this capture.
Its poor local result is retained, not used to proclaim MPP universally better
or dismiss the user's different deployed result. These controls require the
remaining healthy/ablation repetitions before publication. In particular,
MPP mixed's three-second gap remains an acceptance failure regardless of the
other systems' mean rates. No README performance claim is updated from this
single diagnostic cohort.

## Exact next discriminator

The TCP writer pins one transaction and routes Product input while it waits.
On the first backpressured Product or non-Product frame it retains that exact
input and stops polling subsequent input until the write completes. The
existing trace records deferred inputs but not their kind. A lab-only event
now distinguishes Product backpressure from a frame requiring normal actor
dispatch, without printing payload. This will decide whether the observed
long queue residence also creates an avoidable feedback obstruction. No
actor behavior changes before that attribution. Periodic native observation
starvation is independently confirmed; its performance effect remains open.
