# Reordering evidence must exclude common path delay

2026-09-06 UTC. Proposed before implementation and tested; not accepted for
production or release. Exact candidate: `QUIC_REORDERING_EXCESS_CANDIDATE.patch`.
All [34 new runs](receive-history-evidence-20260906.json) retain complete speed
series and echo outcomes, plus the absolute-delay trace and a queue timeline.

## Evidence and why the preceding model failed

The combined receiver/codec plus absolute-delay sender candidate fixes the
stationary jitter collapse: QUIC reaches 184 Mbps with all 50 echoes and a
0.164 s maximum read gap. It fails restoration after genuine QoS. A low-volume
native trace records a learned delay of 2,015,429 us during queueing. Later,
conservative RTT is 54,912 us and base PTO 88,870 us, but actual loss declarations
still use 2,015,429 us. Further episodes recur at approximately that spacing
with RTT 76--128 ms. This is a real history-dependent detector defect in the
experimental model, not justification to loosen BBR gains or discard loss.

The absolute model was intended to stop ignoring late-original counterexamples
when fast-tail RTT sampling or a fixed reordering cap remained too small. It
incorrectly retained two different quantities together: common path RTT and
extra delivery delay of the reordered original. The old total then became
future authority even after its common RTT component disappeared.

## Corrected dimensional model

Let `T` be the existing conservative native RTT, `D0` the unchanged native loss
delay, `g` one native timer granule, and `L=ACK_time-original_send_time` for a
newly acknowledged, current-lineage, unexpired original previously declared
lost. Retain only its nonnegative excess over the current native base:

`J <- max(J, max(0,L-T_at_observation)+g)`

The current loss detector uses:

`D(now) = max(D0(now), T(now)+J)`

Before any proof, preserve both ordinary native detectors exactly. After proof,
packet-count alone cannot overrule the time test. Existing recovery-completion
aging, epoch separation, retained-proof expiry, controller ownership and the
ordinary QUIC timer priority stay unchanged. This introduces no new knob,
absolute timeout, congestion gain, rate prior, RTT sample or admission gate.

Conditional proof: if a subsequent original has total delay `T_new+J_new`, with
`J_new <= retained J-g`, its time deadline exceeds that delivery by at least
`g`. A common RTT change does not freeze the old RTT into `J`. For the observed
2,014,429 us delivery and 1,905,151 us baseline, retained excess is 110,278 us;
at a new 54,912 us RTT the learned term becomes 165,190 us, not 2,015,429 us.
This addresses the identified unit/lifetime conflation, not a chosen target
throughput. A genuine two-second *differential* delay would still be retained.

The premise is conditional: the native baseline can be biased or out of date,
and a future differential tail can be worse than past observations. A single
original still does not prove other losses spurious. ACK-path variation cannot
be perfectly separated from forward variation using these observations. Thus
this is an evidence-based policy allowed by RFC 9002 section 6.1.2, not a claim
of universal prediction or a literal implementation of RFC 8985's algorithm.

## Queueing is a separate owner

The prior mixed trace has about 6.64 MB queued just after the 500-to-10 Mbps
transition. Clearing that many bytes at 10 Mbps takes about 5.31 s if no more
arrive. Therefore an early echo timeout behind that backlog is not itself proof
of a software deadline defect. However, after restoration the routed link
dequeues about 58 MB during a four-second application read gap: physical link
capacity alone does not explain the missing application progress. Reordering,
repair and mixed ordered-prefix owners still need their own evidence. Do not
promise zero latency during an arbitrary capacity collapse, or waive slow
restoration after queues and native RTT have recovered.

## Gates

First test the constant-base late-original guarantee and the reproduced
common-delay downshift counterexample. Preserve duplicate-proof idempotence,
native starting behavior, real loss, recovery aging and same-lineage migration.
Then compare the same ordinary-build jitter, loss-only and combined disturbance
series, both directions and mixed mode. Keep all unavailable/failed echo slots
distinct from successful-latency percentiles. Upload confirmations are observed
at the sender; their arrival bins are not literal link-service bins.

The absolute-delay variant is rejected for shipment. The receiver/codec and
excess-delay composition remain a candidate until those affected gates pass.
Browser, shared/independent aggregation, TCP timing, long-lived memory and the
complete matched-baseline matrix remain in the global acceptance plan.

## Results and disposition

| Routed diagnostic | Current control | Excess-delay composition | Interpretation |
| --- | --- | --- | --- |
| QUIC, jitter only | 0.585 Mbps; 1.573 s max gap | 185.313 Mbps; 0.097 s gap; 50/50 echoes | Strong practical gain, not a full release gate. |
| Mixed, jitter only | 72.442 Mbps; 0.545 s gap | 185.475 Mbps; 0.349 s gap; 50/50 echoes | Gain, but early per-second swings remain. |
| QUIC, loss only | 402.222 Mbps; 0.228 s gap | 413.952 Mbps; 0.253 s gap; 50/50 echoes | No major bulk collapse; one cohort cannot establish precise latency equivalence. |
| QUIC, combined | 0.454 Mbps; 5.827 s gap | 80.024 Mbps; 5.278 s gap | QoS restoration improves; UDP-only outage still interrupts service. |
| Mixed, combined | 14.341 Mbps; 4.004 s gap | 83.744 Mbps; 5.051 s gap | Not accepted: average gain is insufficient; queue/ordering history still matters. |
| Mixed, isolated blackhole | 415.785 Mbps; 0.310 s gap | 402.765 Mbps; 0.806 s gap | Neither has echo failures. Long combined failure is not a universal failure to use TCP. No non-downgrade conclusion from one sample. |

The exact guarded candidate passes all 461 native tests. The counterexample
test was RED under the absolute model (2.015429 s instead of 165.190 ms), then
GREEN under the excess model. Initial jitter/loss binaries predate the explicit
no-evidence branch preserving unusually short native policies; MPP's default
`D0=1.125*T` makes the two expressions algebraically identical. Other candidate
rows use the exact guarded tree. No diagnostic prints remain in that tree.

When jitter ends at eight seconds, QUIC reaches about 440--460 Mbps within
roughly two seconds, keeps all 80 echoes and averages 367.577 Mbps over the
whole run. This is competitive with the earlier Hysteria2 recovery example,
not a matched-repeat ranking. Mixed averages 317.332 Mbps, keeps 80 echoes,
but has a 0.672 s maximum read gap and more variability. Exact-confirmation
uploads average 87.692 / 77.989 Mbps (QUIC/mixed) on the 100-Mbps direction.
Those bins record confirmation arrival, not wire departure; first-confirmation
and upload/browser latency gates remain separate.

Fresh raw/Xray/Hysteria combined controls average 5.504/4.469/6.998 Mbps.
Raw/Xray max gaps are 0.468/0.497 s with all 80 echoes; Hysteria's max gap is
21.775 s. These are individual diagnostic runs, not proof that a candidate is
acceptable merely because one baseline also performs badly.

Decision: archive, do not ship or leave the candidate active. Receive/codec and
excess-delay invariants plus stationary/recovery gains are supported, but mixed
combined stability and full practical non-downgrade are not. No compensation,
BBR gain, arbitrary rate ceiling, admission limit or fixed protocol preference
was changed to make the numbers pass. No new allocator or unproved ACK-history
rewrite is added.

Next priority is the already-SEEN interaction between QoS history, native
reliability and mixed ordered-prefix progress. Separate unavoidable drain of
the measured queue from the remaining application holes after restoration;
the isolated-blackhole control narrows that question. Reuse the exact candidate
and controls as ablations; do not restart the accepted restart/journal fixes,
pretend an echo connection closed by its probe later recovered, or promote a
success-only latency percentile. A further change requires ownership-level
evidence before another model or parameter is altered.
