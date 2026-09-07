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

## Dominant native no-progress interval

In that same model trace, native acknowledged bytes remain exactly415,820,809
from roughly31.1 s through44.1 s, while the deliberate UDP outage ends at33 s.
Native flight is about4.16 MB. RTT/variation are stable at50.112/8.378 ms,
despite the no-progress interval. ACK progress resumes by45.1 s. This precedes
the later Product acquisition restriction and accounts for the longer stall;
changing `E` cannot itself generate missing native acknowledgements.

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
