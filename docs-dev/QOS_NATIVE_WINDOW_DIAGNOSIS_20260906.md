# QoS recovery: native-window discriminator

2026-09-06 18:02 UTC. Existing N1 / mixed QoS-history timing issue, following
the isolated1817f6d work-bound checkpoint. No controller change is proposed
or accepted by these observations.

## Five ordinary ablations

QOS_NATIVE_WINDOW_EVIDENCE_20260906.json retains complete probes and1Hz
application I/O, native state and unique router netem child counters. The
500Mbps/100ms profile, directional3--10%/unequal reverse loss, and15--25s
500→10→500Mbps downshift are unchanged. UDP outage is removed in all five.
The no-jitter ablation removes only20/5ms jitter. MPP uses the ordinary frozen
sweep binary. H2 retains its configured500Mbps sender rate in both directions.

| Protocol / jitter | Bulk Mbps | Max read gap | First echo timeout |
| --- | ---: | ---: | --- |
| MPP QUIC /20,5ms |79.950 |3.782s |15.008--18.009s |
| MPP mixed /20,5ms |86.511 |4.152s |14.845--17.849s |
| H2 /20,5ms |2.967 |17.359s |15.003--18.005s |
| H2 /none |163.123 |5.658s |15.503--18.506s |
| MPP QUIC /none |315.716 |9.853s |15.004--18.005s |

All fail the three-second interactive usability observation during the
downshift. Later unavailable-after-disconnect rows are not additional
independent timeouts. Low success-only p95 is not successful service. The
no-jitter MPP bulk flow then immediately resumes approximately400--480Mbps
after25s; H2 also resumes its preceding useful rate. With jitter, MPP QUIC
instead declines through the recovery phase. H2's severe pre-QoS bulk stall
under this per-packet jitter profile prevents treating that run as a headline
MPP competitiveness win. Its no-jitter ablation confirms the same endpoint
configuration can carry ordinary bulk; no authentication/startup failure
appears in the preserved H2 logs. This is not yet an exact H2 root-cause proof.

## Separate the initial queue from the later collapse

QUIC-only router downlink backlog is6,367,650 bytes at the15s sample and
6,694,146 at16s, while10Mbps service is imposed. Parent HTB and child netem
report the same queued work and must not be added together. A6.37MB prefix
would require about5.09s at10Mbps if ahead of a packet; these aggregate samples
do not prove the echo's exact FIFO position. Initial multi-second latency has
real queued work to account for, and H2 also times out. Do not attribute that
entire interval to an MPP actor or make the probe timeout longer to hide it.

The later phase is different. At30s the router downlink backlog is zero and
MPP's native RTT is68.786ms, yet native flight303,600 nearly fills its304,560
byte congestion window. That window/RTT permits only about35.4Mbps, despite
retained rate308Mbps and pacing382Mbps. At39s RTT81.632ms accompanies a45,740
byte window,44,400 flight and356,698 queued bytes. Its window/RTT ceiling is
about4.48Mbps. Router backlog is then only17,388 bytes. Native application-
limited status is false. This is compatible with a native-window limit, not
an explanation based only on the still-visible large rate or the old queue.

`src/runtime/path/quic/estimator.rs` sets reported `inflight_hi` from the native
`congestion_window`, apart from the one-MTU minimum; it is not an invented
BDP display value. `Bbr3::window` returns its `cwnd`. The adapter's retained
delivery-rate sample and instantaneous pacing need not equal current useful
delivery. QUIC-alone reproduction means TCP selection is not necessary for
this collapse; it does not prove all mixed-path interactions correct.

## Bounded next observation, not a guessed fix

Native BBR3 has several distinct window limits: ordinary max flight,
short-term and long-term loss limits, and ProbeRTT. Source inspection alone
does not say which caused this trajectory. Reordering adaptation is also
held and unaccepted: it learns excess late-original age over current RTT and
ages by completed recoveries. The no-jitter comparison makes that interaction
relevant, but is not proof that its code is the cause or should be reverted.

Add temporary one-second native snapshots of those bounds/phase/RTT model and
the selected reordering loss deadline. Use one QUIC connection and the same
existing profile. No per-packet payload trace, controller parameter change,
rate cap, or threshold sweep. Diagnostic runs are not performance records.
Then identify the exact owner/action that lowers the window and check its
input evidence, intended history and model invariant before any implementation
fix. Archive/remove the trace overlay after attribution. The old
REFLECTION_NATIVE_TRACE runner switch currently has no source consumer; do
not claim it already captures these states.

Reference check: the [BBR draft06 congestion-window model](https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-06.html#section-5.6.4.7)
separately bounds volume using short-/long-term limits and phase headroom.
A high pacing rate alongside a smaller window is therefore not by itself a
model contradiction. Determine why the limiting evidence persists or shrinks
before altering those bounds. This is a draft, not a published RFC.
[RFC9002 loss detection](https://www.rfc-editor.org/rfc/rfc9002.html#section-6.1.2)
permits adaptive timing but explicitly trades reordering tolerance against
loss-detection delay. Permission to adapt does not validate the current held
excess-delay estimator or its performance under this history.

The initial physical-queue latency, post-recovery window collapse and other
mixed ordered-progress stalls retain distinct verdicts. The global restart,
retention, browser, aggregation and matched baseline gates are not waived.

## Native snapshot result — 18:10 UTC

QOS_NATIVE_WINDOW_PROFILE_20260906.json preserves the diagnostic probe and
84 native state/deadline observations. Its 91.118Mbps average is not an
acceptance record: the purpose is causal state attribution, with a temporary
trace overlay. Samples occur at most once per second when existing calls run,
not on a separate guaranteed-cadence timer. Native BBR rate fields are bytes
per second; management rate fields are bits per second.

After capacity recovery, several Cruise snapshots have the congestion window
exactly at the short-term limit: 1,738,639, 1,825,200, 2,064,000, 1,883,466,
982,800 and 528,360 bytes. The long-term limit also falls from 5,617,200 to
2,979,600 and 1,032,000 bytes. Later, 911,604 bytes is the 85% headroom of a
1,072,475-byte long-term limit. ProbeRTT briefly limits the window, but is not
the whole sustained trajectory. Refill does execute; a permanently stuck
refill gate is not supported by this trace.

The learned reordering excess is about 670ms while current RTT has recovered
to 50--80ms. That excess persists across roughly ten seconds of the recovery
phase, then expires after the counted recoveries and is replaced by smaller
79--102ms observations. This run does not prove repeated younger samples renew
the older maximum: its remaining count visibly decreases to expiry. Large
excess can be real during a downshift; expiry alone is not evidence that the
model should cap it at an arbitrary RTT multiple.

The one-second high-loss flags being false does not exclude loss actions
between snapshots. The next narrow trace records completed budget inputs and
the exact lower-/upper-bound actions, plus only late-original observations
that change learned evidence or renew its recovery lifetime. It does not
change loss thresholds, window formulas or the traffic profile. In particular,
test whether current loss evidence actually supports each reduction; do not
infer correctness merely from a high retained maximum or infer a bug merely
from ordinary BBR having a smaller sending window.

## Decision owner identified — 18:39 UTC

The exact action trace is in QOS_NATIVE_BOUND_ACTIONS_20260906.json. Of124
lower-bound calls,62 take unknown-evidence raw authority (45 strictly reduce
the short-term bound); none of those62 has explicit congestion authority.
The other62 are compensated-budget decisions and retain a separate verdict.
BBR3 deletes unacknowledged send snapshots after ten younger delivery rounds,
although QUIC still owns them under its delayed loss detector. A production-
callback counterexample is RED before removing that expiry and GREEN after.
NATIVE_PACKET_EVIDENCE_LIFETIME_MODEL gives origin, symbolic timing argument,
discard/clone cleanup obligations and component proof.466 native tests and
25 adapter tests pass. Ordinary candidate comparisons remain next; this does
not yet claim all recovery latency or budget decisions are corrected.
All temporary trace hunks are archived and removed from active source.
