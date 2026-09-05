# Current performance reflection: reordering and recovery

2026-09-05 UTC. Ordinary production build `7189e69`; prior released build
`d1a99ad`. No diagnostic feature or new controller patch in the application
measurements. This is an intermediate diagnosis, not release acceptance.

## Decision

Retain the evidence-backed lifecycle, retention, accounting and exact-frontier
corrections adjudicated in [the review](REVIEW_AND_PRACTICAL_ACCEPTANCE.md).
Do not increase loss compensation, install a fixed protocol preference, replace
native admission with estimated bandwidth, or rewrite the allocator to explain
this result. A practical native reordering deficit is now reproduced. It is the
first performance owner to investigate, before expanding the full matrix.

The same severe deficit occurs in the last release. This excludes the latest
six corrections as its introduction, but does not prove every historical
change performance-neutral. Identifying the *first* introducing commit still
requires older matched builds if a proposed correction depends on that history.

## Evidence and attribution

The compact [26-case evidence](review-evidence-20260905.json) preserves complete
application interval series, outcomes and scope caveats. Every row is one run;
these are not confidence intervals, best-of selections or general rankings.

All paths used 500 Mbit/s downstream, 100 upstream, 70/30 ms base one-way
delay. Jitter, where present, was independent normal 20/5 ms. Loss-only means
3% downstream and 1% reverse, without jitter. Jitter-only means **zero injected
loss**. Hysteria2 2.10.0 had explicit 500/100-Mbit/s Brutal priors; MPP used
unconfigured dynamic discovery. Xray 26.3.27 used VMess/TCP. Raw TCP used the
same host kernel's BBR. These baseline versions match the historical controls,
not a claim to be the newest releases.

### Isolating jitter from loss

| Ordinary build / path | Loss only, Mbps | Jitter only, Mbps | Placement |
| --- | ---: | ---: | --- |
| MPP QUIC | 421.915 | 0.658 | endpoint egress |
| MPP TCP | 398.367 | 82.512 | endpoint egress |
| MPP TCP+QUIC | 416.835 | 73.763 | endpoint egress |
| Hysteria2 | 242.230 | 15.210 | endpoint egress |

Endpoint-egress netem can interact with TCP TSQ. These TCP values are therefore
diagnostic, not a final native/product comparison. Repeating the relevant
jitter-only case on a **forwarding router** avoids the sender placement:

| Build / system | Receiver Mbps | Maximum bulk read gap |
| --- | ---: | ---: |
| Current MPP QUIC | 0.738 | 1.026 s |
| Last released MPP QUIC | 0.670 | 2.066 s |
| Raw TCP | 9.394 | 0.137 s |

The current routed QUIC run had zero qdisc drops in both directions. Its
independent echo timed out once after seven successful checks; 37 subsequent
scheduled slots were unavailable after the probe closed that connection. This
is not 38 independent network failures, nor a successful browsing gate.

A separate numbered UDP observation received all 1,000 original datagrams,
with 896 arriving below the highest already received sequence number and
maximum observed displacement 96. Adding a pfifo child did **not** remove this
reordering on this kernel; that attempted ablation must not be represented as
an ordered control. Its QUIC run also had zero qdisc drops and 0.566 Mbps.

During the endpoint jitter-only QUIC collapse, peer snapshots showed roughly
46–64% native declared loss, 7–13 KB windows, 0.73–1.03-Mbit/s pacing and about
0.5 MB pending Product work, with the native controller not application-limited.
The sender had already read tens of MB of source data. In an observed collapse
run server RSS plateaued around 161–162 MiB and CPU around 3% of one core:
there is no evidence that CPU saturation or a missing source explains that run.
Those short observations do not close the separate deployed memory incident.

### Why the native detector is implicated

`Connection::detect_lost_packets` uses the configured packet threshold 3 or
the time threshold `9/8 * max(latest_rtt, smoothed_rtt)`. An overtaken packet
can therefore be declared lost long before it is overdue in time. The current
detector does not learn a reordering allowance from later proof of delivery.

[The deterministic diagnostic](QUIC_REORDERING_DIAGNOSTIC.rs) uses real native
Pair endpoints and encrypted packets, not manually invented congestion callbacks:

1. Establish 100 ms RTT and send five PING packets at one time.
2. Hold only the first original datagram; deliver the other four normally.
3. At 100 ms, exactly one loss is declared, before the 112.5 ms time threshold.
4. Deliver that exact original, then its ACK. All five PINGs reach the peer.
5. Repeat on the same connection. The latter two rounds each report one
   successful spurious-congestion undo, yet the next identical episode again
   declares the same false loss.

Observed output is `loss_declared=1` on all three rounds, and `undo=0,1,1`.
The first warm-up round is not used as proof of completed undo. The initial
test incorrectly assumed every late delivery must increment the successful-
undo counter; that counter is narrower. The corrected diagnostic separately
asserts original delivery, packet-threshold timing and later successful undo.
It is archived outside desired-behavior tests, not installed as an acceptance
test that would require future code to preserve this deficit.

At 500 Mbit/s and 1,200-byte packets, nominal spacing is 19.2 microseconds;
three packets span only 57.6 microseconds. Tens of milliseconds of differential
delay can overtake far more than three packets. This calculation explains why
a fixed packet count is not a rate-independent reordering tolerance. It does
not prove that every declared loss in a real deployment is spurious.

The source predicate, exact packet counterexample, zero-drop routed run,
backlog/native metrics and loss/jitter ablations identify a credible causal
family: **reordering produces premature loss and repeated recovery responses**.
An exclusive attribution of the full throughput collapse to packet threshold,
time threshold, bandwidth sampling or incomplete undo still needs a native
packet/ACK chronology. Removing a BBR undo guard is not yet justified: existing
tests prove that genuine earlier deferred loss must not be rolled back by proof
about a different transaction.

The default detector follows the recommended starting values; this is **not a
claim of an RFC violation**. [RFC 9002 §6.1](https://www.rfc-editor.org/rfc/rfc9002.html#section-6.1)
allows adapting reordering thresholds after spurious detection. TCP's
[RACK-TLP model](https://www.rfc-editor.org/rfc/rfc8985.html) is a reference for
time-based ordering evidence, not permission to copy a number into QUIC.

### Same-connection recovery

On the router, remove jitter at about eight seconds, retaining 500/100-Mbit/s
service and zero injected loss. No endpoint or application request restarts.

| System | Full-run Mbps | Bulk maximum gap | Recovery to about 450 Mbps |
| --- | ---: | ---: | --- |
| MPP QUIC | 294.442 | 1.804 s | about 8 s after removal |
| Hysteria2 | 363.038 | 5.358 s | about 2 s after removal |

Hysteria2 stalls longer *during* reordering but recovers faster afterwards.
MPP's independent echo fails its three-second wait after four successes;
70 later scheduled slots are unavailable. Hysteria2 completes all 80 echoes.
MPP's low success-only echo p95 must not be advertised as a latency advantage.
The [public two-panel curve](../docs/assets/performance/quic-reordering-recovery.svg)
includes the missing echo service, not only the improving bulk rate.

### Combined disturbances and reverse-direction control

The endpoint-shaped combined cohort used eight five-second forward loss epochs
`3,8,5,6,10,3,5,8` percent (mean 6%), unequal reverse loss, a 10-Mbit/s forward
shaper from seconds 15–25 and a silent UDP blackhole from 30–33. Full-run Mbps
were raw 4.463, Xray 3.606, H2 9.003, MPP TCP 7.170, QUIC 0.474, mixed 7.777.
Read gaps reached 28 s for H2, 6.16 s for MPP TCP and 2.54 s for mixed. This
cohort fails practical acceptance; similar bad averages do not imply stability.
Its TCP placement caveat still applies. Do not use it as a final product ranking.

On the router, constant-loss/no-jitter uploads confirmed all sink bytes and
completed at QUIC 90.124, TCP 88.839 and mixed 89.693 Mbps on the 100-Mbit/s
uplink. Full completion includes the drain after the 25-second injection window.
These are useful directional controls, not proof that variable-loss upload or
browser experience is fixed. Interval timestamps are when cumulative sink ACKs
arrive at the sender; compressed ACK arrivals can exceed link rate in a bin.
They are not sink-timestamped throughput or read-gap evidence. Do not clamp them
to 100 Mbps or publish them as receiver latency.

## Bounded next model and acceptance work

1. Record exact native send/ACK/loss/late-proof order for the failing trajectory.
   Separate packet and time triggers and old/current controller epochs. This
   must choose the responsible owner, not create a new global performance knob.
2. If packet reordering remains the causal trigger, model a **path-scoped,
   evidence-learned reordering tolerance**. State its aging, migration/rollback,
   bounded memory, ACK-spacing and time-loss/PTO interactions before code.
   Admission, congestion evidence and recovery timing remain separate.
3. Prove delayed originals are handled without repeated needless recovery,
   while genuine isolated loss, bursts, CE, ACK compression, blackholes and
   post-congestion recovery remain effective. Test both directions. A larger
   fixed threshold, removal of all loss response, or a 50% compensation floor
   is not this model. Increased tolerance necessarily trades some promptness
   of true-loss detection; measure it, do not promise zero cost.
4. Rerun the affected ordinary QUIC and mixed trajectories against prior and
   H2 controls. Keep an intermediate commit only after practical benefit and
   no material regression are demonstrated; otherwise remove the candidate.
5. Return to the already SEEN deep-buffer model inflation and TCP/mixed ordered
   latency. These are different errors: false low service under reordering is
   not the same as a retained 400-Mbit/s maximum under a 10-Mbit/s policer.
6. Complete independent/shared aggregation, changing asymmetric links,
   blackhole/QoS combinations and ablations, upload/download, cold/warm single
   requests, concurrent/browser/Cloudflare, and sustained churn/RSS. The final
   gate requires matched repetitions, actual receiver/dequeue accounting,
   missing-service outcomes and baseline comparisons. No current claim of
   universal optimum or release readiness follows from this checkpoint.

Independent audit remains unavailable under worker usage limits. No independent
sign-off is claimed. Theoretical work must prevent irrelevant model changes;
it cannot eliminate testing of native transport timing or unknown future links.

## Reproduction and excluded setup errors

The owned `mptunnel-reflection` Docker project used existing lab images. The
corrected topology is client → forwarding router → server, on two isolated
subnets; shaping applies on router egress toward each receiver. Root HTB uses
the directional service rate, with an 8,192-packet netem delay queue. Record
`tc -s -j class/qdisc` each second. Existing `lab/mixed_workload_probe.py` uses
one continuous HTTP body plus 64-byte persistent echo every 500 ms; timeout is
3 s per echo. `lab/bulk_upload_probe.py` supplies exact target-confirmed totals.
Detailed disposable setup and raw logs remain in `./.tmp/reflection/`; the
compact committed evidence survives its later removal.

The initial 12-second timeout missed the transitions; an undersized 1,000-entry
delay queue obscured capacity; OUTPUT DROP returned socket EPERM rather than
silent remote loss. These attempts are excluded. Corrected runs use adequate
delay storage and endpoint INPUT DROP. Renaming the MPP executable prevented
the runner's name-based stop from finding it: that teardown timeout was an
experiment defect, not a Product shutdown failure. The failed pfifo experiment
and sender-side upload timing limitation are explicitly retained above.
