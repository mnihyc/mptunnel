# Upload recovery gates: distinguish cause from consequence

2026-09-07 00:47 UTC. Existing SEEN startup/QoS/recovery transaction, not a
new issue batch. No production change is accepted by this document.

## Concrete observations

Four diagnostic runs preserve exact target confirmation, but retain delivery
gaps of 3.380–5.447 seconds. Full probes and one-second observations are in
UPLOAD_RECOVERY_GATES_20260907.json. Their means are not acceptance or an A/B
comparison: diagnostics and random packet realization differ.

The requalification trace records QUIC probe 29 publishing at +36.109 s and
being accepted at +36.230 s. OriginalData resumes at +41.700 s. Thus an
arbitrarily larger probe-validity timeout is not a justified correction.

The head trace records an empty repair queue at +34.656 through +37.656 s,
with 2,043,616 bytes of raw source data pending. QUIC is non-stale and its
ordinary and repair command queues accept the preview. Changing critical
repair FIFO arbitration cannot fix this particular interval. The earlier
head at +33.656 s was indeed bound to busy TCP, but its existence does not
prove that it causes the later empty-repair-queue stall.

The model trace identifies an actual post-requalification gate:

| Client time | QUIC qualification / queue readiness | Exact original debt | New acquisition allowance |
| --- | --- | --- | --- |
| 47.491 s | unqualified / ready | 12,260,768 B | 524,288 B |
| 48.492 s | unqualified / ready | 5,493,024 B | 524,288 B |
| 49.493 s | unqualified / ready | 524,288 B | 524,288 B |
| 50.500 s | unqualified / ready | 512,288 B | 524,288 B |

Each preview requests 65,536 B and fails the existing acquisition gate. The
exact configured Product envelope remains 67,108,864 B. This is consistent
with RFC15.1's explicit retained-debt rule after qualification revocation,
not evidence that an estimated rate directly reduces configured Product
credit. It is a real service restriction, but changing it is NOT yet justified:
extra new bytes can merely queue behind the same old ordered prefix. Any
candidate must demonstrate earlier ordered delivery, not just more enqueue.

## Native live-packet ACK counter pause; attribution remains open

In that same model trace, native acknowledged bytes remain exactly415,820,809
from roughly31.1 s through44.1 s, while the deliberate UDP outage ends at33 s.
Published native flight is about4.16 MB. RTT/variation are stable at
50.112/8.378 ms. This counter advances again by45.1 s, before the later Product
acquisition restriction. It does NOT by itself establish absence of every
native ACK or prove the cause of the whole application stall.

Source audit at01:24UTC narrows its authority: InstrumentedController adds
these bytes in `on_ack` / `on_ack_with_packet_state`. A late ACK of an already
declared-lost retained original instead calls `on_lost_packets_retired`, which
does not increment that telemetry counter. A published snapshot also needs
its own sampling clock checked. Thus the earlier wording that this counter
freeze "accounts for the longer stall" was stronger than the evidence. Native
packet/timer progress and exact Product frontiers must be paired before that
causal conclusion. This is an interpretation correction, not authorization to
inject fake ACK callbacks, modify the counter or change Product admission.

The final apply trace records only one post33s bulk-authority failure and nine
exact-eligibility changes; it does not support a hot optimistic-apply retry
loop as the dominant delay. The owned client/server INPUT chains are ACCEPT
with no remaining injected UDP DROP rules after the run. The runner records
successful removal when the33s boundary is crossed; no host shaping is used.

## Next exact discriminator and rejected shortcuts

The native trace records timer choice/fire, PTO count/base, outstanding work,
received datagrams and learned reordering history. Determine whether the
post-outage pause is exponential PTO backoff, a long learned time-loss
deadline, or absent driver service. No controller gain, delay threshold,
requalification reset or Product limit is modified.

[RFC9002 §6.2.1](https://www.rfc-editor.org/rfc/rfc9002.html#section-6.2.1)
requires time-loss timer priority over PTO. Its
[§6.1.2](https://www.rfc-editor.org/rfc/rfc9002.html#section-6.1.2)
allows adaptive loss thresholds but identifies the detection-delay tradeoff.
Therefore changing the minimum timer selection alone would not be a clean
standards-aligned fix. First establish the actual learned evidence and clock
that delayed progress. Do not label this an upstream defect without that proof.

All Product diagnostic overlays are archived under .tmp/reflection/ and
removed from source after freezing their binaries. Native tracing is likewise
temporary. Independent audit is still usage-limited. Relative ACK encoding
and the older runtime composition remain held; rejected batching stays removed.
Global acceptance, browser, aggregation and sustainability gates are unchanged.

## Native timer observation — 2026-09-07 01:04 UTC

The next diagnostic confirms all509,804,544bytes in47.305s,86.217Mbps,
with a3.806s maximum confirmation gap. It does **not** reproduce the earlier
14s native ACK freeze. First-per-second native events plus every PTO-count
transition, the complete application probe and the router queue series are
preserved in NATIVE_ACK_TIMER_OBSERVATION_20260907.json. The selection is
explicit; it is not represented as a complete packet trace.

After the last pre-outage ACK, native PTO counts advance at31.498,31.920,
32.763 and34.447s. The outage ends at33s; a subsequent ACK resets PTO at
34.612s. That realization has ordinary exponential PTO recovery, not a14s
driver outage or evidence that BBR prevents probe transmission. A learned
1.147s excess remains after the QoS queue clears and affects subsequent
time-loss deadlines. This alone does not attribute the prior14s pause.

The bounded next check is an ACK-transaction consistency defect in the held
reordering candidate: it learns excess against the old RTT immediately before
the same ACK updates RTT. An actual encrypted packet/ACK fixture demonstrates
297ms learned delay instead of the model's251ms, double-counting the46ms
common RTT increase. The correction moves learning after the existing RTT
update, before loss detection. It neither changes timer priority nor supplies
a new RTT sample, congestion parameter or acquisition allowance. Full470
native tests pass for the rising-RTT case; falling-RTT fixture coverage and
ordinary timing controls follow before any disposition. This is not a new
upstream defect: the held MPP reordering integration owns the ordering mistake.
