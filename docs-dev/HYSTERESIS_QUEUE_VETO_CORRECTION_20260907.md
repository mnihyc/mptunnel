# Completion-time hysteresis: bounded queue-veto correction

Date: 2026-09-07 UTC. Status: NOT ACCEPTED. Exact RED/GREEN,136 affected
selector/recovery checks and independent source review pass, but the four-cell
ordinary comparison fails practical non-regression. The active source, tests
and RFC are restored to the pruned baseline. The exact candidate is preserved
in [its patch](HYSTERESIS_TIME_UNACCEPTED_20260907.patch), which passes
`git apply --check`. No release pass or runtime correction is committed.

## Practical disposition

| Ordinary mixed workload | Pruned control | Candidate |
| --- | ---: | ---: |
| Download Mbps | 84.025 | 56.247 |
| Download maximum closed read gap | 4.534 s | 4.510 s |
| Mirrored upload Mbps, all bytes confirmed | 95.349 | 51.024 |
| Upload confirmation gap | 3.061 s | 5.063 s |
| Post-load upload drain | 4.247 s | 26.214 s |

All four runs complete without the85s observation guard. Download first body
improves0.748->0.527s and pre-QoS mean119.33->143.38Mbps, but restored25--30s
mean falls196.15->9.27Mbps, then post-outage76.80->5.07Mbps. Both download echoes
time out duringQoS. The complete probes,193 management/router samples, phase
curves and receipt-clock limits are in
[the ordinary comparison](HYSTERESIS_TIME_ORDINARY_COMPARISON_20260907.json).

This does not prove the byte veto is semantically sound, nor that its deletion
caused every adverse recovery event. It does prove that the isolated deletion
has not earned the required practical acceptance. Do not retain it merely
because the exact selector fixture turns green or pre-QoS throughput improves.

The next bounded observation belongs to the already-seen native QoS/recovery
owner: candidate QUIC's ACK-byte counters and RTT stop advancing while its
sampling clock continues; flight exceeds cwnd and both peers add2400B during
the scheduled UDP outage. The restored router no longer has a large standing
queue. These facts do not distinguish absent ACKs from retained-only ACKs or
prove the suspected backed-off probe sequence. Capture actual native timeout,
probe and ACK classifications before changing recovery. A late-startup warning
also retires a TCP carrier, but lacks stream identity and follows the initial
stall; it cannot be declared the sole cause. No threshold change follows.

## Why this change, and why now

This is the ready-candidate branch of the existing mixed-carrier delivery stall,
not a new theoretical allocator. [Exact attribution](SELECTION_INPUTS_FRONTIER_ATTRIBUTION_20260907.md)
records five QUIC originals, one TCP original, then QUIC originals again. Both
candidates are admitted and ready; the live lower owner is QUIC. The TCP range
later holds the receiving frontier for 669 ms. The capture establishes the
actual dependency, not the unobserved candidate's counterfactual delivery time.

The recorded estimates are TCP499.502607ms and QUIC500.201997ms. The incumbent
passes the existing19.407ms measured-jitter tolerance but fails a second raw
queue-byte comparison:262144B >176898B +65536B. Queue service already contributes
to ETA, and raw bytes on different-rate paths do not compare service time.

Commit8a5450e introduced this check when replacing unconditional sticky-lead
selection. Its useful intent was stability without preserving a materially
slower incumbent. Commit9c5a125 extracted the same predicate; it did not create
the defect. [Independent review](HYSTERESIS_QUEUE_VETO_REVIEW_20260907.md) checks
the origin, every live caller and the unchanged countercases.

## Model correction and tradeoff

For eligible candidates whose ETAs already include queued service:

```text
retain incumbent iff
    incumbent ETA <= best ETA + max(incumbent jitter, best jitter)
```

Remove the separate byte-queue conjunct, the now-unused payload argument and
the request-side pure delegation wrapper. No new score, wait, controller,
constant, credit, protocol preference or discovery mechanism is introduced.
The candidate's RFC15.1 clause explicitly states the duration-only comparison. Request lead and
datagram preference share the dimensional rule, after their own eligibility,
native-credit ordering, pacing and deadline checks.

The tradeoff is intentional retention when an estimated advantage is inside
observed timing uncertainty. A queue-induced material ETA disadvantage still
switches; failed, excluded, stale or queue-unready owners gain no admission.
This does not fix busy-fast/free-slow mandatory placement, unknown startup
evidence, all mixed-rate projections or sustained aggregation.

## Exact RED, not an old-policy test relabeled as success

The previous test explicitly required a raw queue excess of one quantum to
override hysteresis. It is replaced, transparently, by
`raw_queue_bytes_cannot_override_observed_completion_hysteresis`, using the
captured production response candidates, authoritative-gap state and oldest
owner. The two worse candidates/later ledger entries are omitted; their debt
is nonbinding and does not enter these ETAs. Independent fixture review checked
that reduction. Actual observations and fixture reductions are not conflated.

Before the runtime change, the release library test build took3m09s without
compiler warnings. The initial Cargo invocation used the wrong exact module
filter and ran zero tests; that is NOT a pass or a RED. Running the actual test
entry directly then produced the causal failure:

```text
target/release/deps/mptunnel-3a813700b0d8f97b --exact \
  runtime::sender::response::scheduling::tests::raw_queue_bytes_cannot_override_observed_completion_hysteresis \
  --nocapture

1 test; FAILED; exit101
left:  CarrierPathKey { underlay: Tcp, path_id: PathId(2) }
right: CarrierPathKey { underlay: Udp, path_id: PathId(0) }
queue service is already in ETA; raw bytes cannot veto sub-jitter owner retention
```

Both exact ETA assertions passed before the selector assertion failed. The
existing material-completion-gain test remains unchanged. Runtime source review
finds no changes to readiness, Product resources, native authority or ownership.

## Remaining acceptance transaction

The candidate library build succeeds in3m06s without compiler warnings. The
following command passes135 tests,0 failed/ignored, in0.01s:

```text
target/release/deps/mptunnel-3a813700b0d8f97b \
  runtime::sender::response::scheduling::tests \
  runtime::sender::request::scheduling::tests \
  runtime::tests::datagram::udp_association \
  runtime::sender::response::service::tests::ordinary_ecf_retains \
  runtime::sender::response::service::tests::frontier_hysteresis \
  scheduler::policy::tests::material_completion tail_reinjection \
  --test-threads=3
```

This includes the exact RED-now-GREEN case, unchanged material-advantage and
queue-growth preemption, failed/draining/blocked/resource-exhausted owners,
datagram suppression, and existing terminal/tail recovery controls. No new
terminal or controller tests were invented for this advisory-only change.
The affected test-only request helper's direct control,
`retained_tail_uses_only_a_measured_earlier_completion`, also passes separately
(one test,0.00s), for136 total selected checks. Full-tree formatting and diff
checks pass.

The application builds in1m33s without warnings and is frozen at
`./.tmp/reflection/bin/hysteresis-time-20260907/mptunnel`; opt-in diagnostics are
off for ordinary execution. The source differs from frozen pruned runtime only
by the shared byte-veto deletion and mechanical callers. Compare
mixed download and mirrored mixed upload against frozen pruned runtimeb9a1600,
with the same existing varying-loss/QoS/outage profile and complete timing
series. Diagnose any adverse result; do not accept a mean gain as stability or
promote this correction as closure of the remaining global gates.
