# Native drain model diagnosis and decision

Date: 2026-09-05T17:55Z. N1 only; no controller correction accepted.
Updated 18:00Z: candidate rejected and production changes removed.

Updated19:23Z: the retained-rate/queueing limitation is real, but two
dequeue-verified directional experiments recover without restart. No native
model change is accepted in this batch. Results below delimit that conclusion.

## Counterexample and obligation

The established asymmetric lossless 400→10 Mbit/s case retains a 395 Mbit/s
model while current ACK delivery is 10 Mbit/s. At 80 ms propagation, the old
half-BDP ProbeRTT target is about 2 MB, while the new complete BDP is 0.1 MB.
An expired minimum can admit a queued RTT of seconds. Recomputing the probe
allowance as 0.5 × retained bandwidth × that queued RTT then enlarges the very
queue the experiment was meant to drain. The native callback simulation shows
this without MPP scheduling or loss compensation.

## Smallest model correction to evaluate

One ProbeRTT is one drain experiment. Its maximum allowed flight must never
increase because observations taken while draining revise a model parameter.
At entry, retain the existing model-derived target. During the experiment,
allow it to decrease with stronger model evidence, never increase. On exit,
retire this experiment's target. Ordinary bandwidth sampling, pacing, window
restoration, loss policy and max-filter epochs remain unchanged.

Let T(k) be the candidate half-BDP target on ACK k and D the experiment's
target. D(0)=T(0), D(k+1)=min(D(k),T(k+1)), with the existing native minimum
packet floor. Therefore D(k+1)≤D(k). A queued RTT refresh cannot bootstrap
additional flight within the same drain epoch. This is a state-ownership
invariant, not a new RTT threshold, gain, configured speed or Product clamp.

This alone does NOT prove that the old bandwidth maximum becomes correct or
that the queue fully drains. In particular, an old half-BDP can still exceed
the new whole BDP. The existing real-callback trajectory must decide whether
normal feedback subsequently corrects that estimate. If not, this is only a
partial mechanism correction, not N1 acceptance; do not add unrelated tuning
to disguise that result.

## Controls / tradeoffs

Healthy constant-model probes have exactly the same target. Lower model
targets still apply immediately. Genuine propagation changes remain measurable;
no RTT is frozen globally. A target increase waits for the next experiment,
which can temporarily reduce probe throughput but cannot add queue debt.
Existing idle, erasure, spurious-loss, fast-tail and native probe controls must
remain valid. Compare asymmetric forward-rate restriction and reverse ACK
restriction independently before claiming end-to-end recovery.

The source model is the experimental BBR draft, not a published RFC:
[draft-06 ProbeRTT](https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-06.html#section-5.3.4).
This candidate is a deliberate local invariant strengthening, not an assertion
that unmodified upstream Quinn was tested or found defective.

## Executed result and rejection

The local invariant test failed before the patch (2 MB target grew to200 MB)
and passed with the non-increasing target. The complete 400→10 trajectory
nevertheless still reproduced the defect in both operational-RTT attribution
arms: at75s raw delivery10 Mbit/s, model400 Mbit/s, minimum RTT23.9s,
window67.8 MB, and max-filter cycle still5. The test consumed56.13 wall seconds.
Trace: `.tmp/n1-drain-candidate.log`.

Why: an old half-BDP target already exceeds the new entire BDP. It admits a
queued RTT as a completed probe. The next independent probe can therefore
start with an inflated target even though each individual target is monotone.
The required model must establish an actually drained observation, not only
an internally stable per-experiment bound. No production patch or failing
invariant test from this candidate is retained in the release changes.

## Additional causal isolation — recorded 18:00Z

Using the existing private ProbeRTT gain override only in a test to select
the native minimum-packet flight also fails the full downshift acceptance.
It DOES recover minimum RTT to81ms and bounds ordinary flight around8.1 MB.
It does NOT retire the400-Mbit/s maximum: cycle5 remains stuck to80s.
Trace `.tmp/n1-minimum-flight-diagnostic.log`;17.99s execution. This proves
that a minimum-flight parameter change alone is not a valid solution either.

The trace exposes a second structural interaction: at10 Mbit/s that restored
8.1 MB requires about6.5s to drain. The next periodic RTT probe is due before
ordinary bandwidth exploration completes a feedback round. It repeatedly
preempts Refill/UP, so no completed bandwidth-probe epoch can retire the old
maximum. This is experiment starvation, not a need for a different5s timer.

The next discrimination is fair ownership between experiments: after one
ProbeRTT, an established backlogged bandwidth model must complete one real
ProbeBW epoch before another ProbeRTT may preempt it. Startup retains its
existing first-probe behavior. This is a discrete state condition, not an
extra duration or rate threshold. Test it with both ordinary and minimum-
flight probes before choosing any production model; neither arm alone is
release acceptance. Check the idle and unavailable-feedback limitation:
without an eligible ordinary feedback epoch, another disruptive RTT probe
cannot manufacture evidence that the absent bandwidth experiment lacks.

Executed at18:00Z: minimum flight plus this phase-fairness gate made one
bandwidth epoch complete (cycle5→6 by70s), but still retained400 Mbit/s at75s.
Each restored8 MB flight costs about6.5s at the new service rate, and the
several-round bandwidth experiment remains much too slow.18.48s execution;
`.tmp/n1-probe-fairness-diagnostic.log`. Neither the gate nor the private
minimum-flight override is retained in production or acceptance tests.

These ablations identify why simply freezing a target, changing its gain, or
delaying an interrupt cannot be the complete fix. The older maximum supplies
both offered flight and the feedback clock needed to revoke that maximum.
The next model decision must break that circular evidence dependency using
native observations from a genuinely busy drain interval, without treating
intentional low sending or application goodput as a path-capacity ceiling.

## Dequeue-verified directional discrimination — 2026-09-05T19:23Z

The first real experiment used netem rate changes on already queued packets.
Those packets retained scheduled serialization deadlines; changing the label
back to400 Mbit/s did not establish restored dequeue service. Its apparent
post-restoration stall is not evidence of a persistent MPP controller defect.

The replacement uses HTB service at dequeue with a separate, fixed netem
propagation delay:70ms forward and10ms reverse, zero injected loss,8192 queue
entries,64KiB HTB burst. One fixed HTTP request lasts80s. Server egress is
400→10→400 Mbit/s at20s and50s; reverse service remains300 Mbit/s. Both
endpoints use the same existing6636091 diagnostic binary (wire10), not the
new telemetry/wire11 build. No restart occurs between the rate transitions.

| Interval | Actual HTB dequeue | Product delivery | Native model / RTT |
| --- | --- | --- | --- |
| Before reduction | median391.58 Mbit/s | 327–375 Mbit/s | about395 Mbit/s /82ms |
| Reduced service | about10 Mbit/s | 9.34–9.53 Mbit/s | about396 Mbit/s /4.94s |
| First restored second | service restored | 342.264 Mbit/s | retained model remains high |
| Following seconds | median397.64 Mbit/s | 375.422,377.059,374.246 Mbit/s | queued RTT falls; transient window inflation remains |
| Late restored interval | median389.46 Mbit/s | 321–375 Mbit/s | about398 Mbit/s /82ms |

No qdisc drops, no replacement request, first body170ms, maximum positive
read gap92ms. Restored transient flight allowances grew as high as the
queued RTT model allowed; this is not a low-latency acceptance claim.
Artifacts: `.tmp/qos-rate-restored/{download.json,service.jsonl}` and the
management/native logs in that directory. Exact transitions and dequeue
counters, not configured rate labels, determine the intervals.

The complementary warm-connection case holds forward service at400 Mbit/s
and changes reverse service300 Mbit/s→250 Kbit/s→300 Mbit/s at15s and35s.
One fixed request lasts60s. Reverse dequeue measures0.242–0.260 Mbit/s during
restriction, with up to137KiB queued ACK/control traffic and no drops. Forward
service stalls for several seconds while its feedback is delayed; Product's
maximum read gap is3.445459s. After restoration, Product reaches292.472 Mbit/s
in the first full recovery second, then374.674 and377.059 Mbit/s. No restart
or request replacement is used. Artifacts: `ack2-download.json`,
`ack2-service.jsonl`, `ack-management.jsonl` in the same directory.

An initial ACK-case attempt began its shaper observer at39s, after both
planned transitions; it never applied the restriction. That run is excluded,
not counted as a successful congestion test. The anchored replay starts the
observer before the request and records both applied transitions.

Disposition: current service and the retained model are different quantities,
now exposed by D1. Lossless deep-buffer adaptation/latency remains an open
native model limitation, not fixed by relabeling the dashboard. A persistent
need for restart after actual service restoration is **not reproduced** by
these two cases; neither result disproves the user's other network cases.
The unchanged native model is deliberate: all three attempted changes failed
the full causal gate. No trial gains, timers or rate clamps enter the commit.
The component counterexample is archived as `QUIC_DEEP_QUEUE_DIAGNOSTIC.rs`,
outside the acceptance suite because it asserts the observed bad behavior.
