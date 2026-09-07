# Independent review: raw queue veto in lead hysteresis

Date: 2026-09-07 UTC. Category: bounded source/history review. No runtime edits,
builds or labs were performed for this review. Verdict: removing the raw-byte
veto is justified; practical acceptance still requires the focused candidate
tests and ordinary timing comparison. This is not a general allocation pass.

## Origin and intended behavior

Commit `8a5450e` introduced the predicate while replacing unconditional
ordinary-lead retention with a best-ETA choice and measured hysteresis. Its
purpose was to avoid noisy ownership changes without keeping a materially
slower lead. The original checks combined maximum observed jitter with a
one-payload queue-byte allowance. Commit `9c5a125` subsequently moved the same
predicate into `src/scheduler/policy.rs`; that extraction did not create the
defect. The neighboring `path_has_material_completion_advantage`, present at
`3a6d0ea`, already states that queue work included in ETA must not be counted
again as an independent raw-byte test.

## Exact observed defect

Branch A of [the selection-input attribution](SELECTION_INPUTS_FRONTIER_ATTRIBUTION_20260907.md)
records stream 1, generation 2753, original range `[113231608,113297144)`.
The incumbent QUIC lower owner and TCP challenger are both admitted, live,
nonstale, policy-eligible, queue-ready and scorable. No resource suppression
applies. Their comparison is:

```text
QUIC ETA 500.201997 ms <= TCP ETA 499.502607 ms + jitter 19.407 ms
    true: nominal improvement is only 0.699390 ms

QUIC queue 262144 B <= TCP queue 176898 B + payload 65536 B
    false: raw excess is 19710 B
```

The byte veto causes a one-frame QUIC-to-TCP departure followed immediately by
QUIC placement again. The TCP-owned range later holds the client frontier for
669 ms while later reordered bytes accumulate. These observations identify the
actual choice and ordered dependency; they are not a measured counterfactual
claim that the candidate eliminates every gap.

`score_path` already includes queue service in completion work. Throughput
uses `max(queue + native flight, Product work)` to respect overlapping stages;
other traffic classes include queue/native work directly. A larger queue in
bytes need not take longer on a faster carrier. The extra byte veto therefore
overrides the timing comparison with incompatible service units. Converting
that veto into another queue-time penalty is unnecessary: the ETA already
contains the service term. This review does not endorse every existing rate
projection or replace the separate rate-scope audit.

## Affected callers and preserved countercases

- Response OriginalData selection retains an incumbent only from the already
  admitted candidate set. Failed, excluded or queue-unready owners remain out.
- Request ordinary-lead selection also uses scored candidates and preserves
  its separate eligibility and native-credit ordering.
- UDP association preference uses `score_path` through `udp_path_eta_for_ttl`,
  plus actual pacer-ready delay. Payload, suppression and TTL viability checks
  precede incumbent preference and remain unchanged.
- The request `tail_reinjection_earlier_completion_target_from_model` caller is
  `cfg(test)`, not a live recovery-policy change.

Material ETA deterioration still switches paths; queue growth can cause that
deterioration through the existing score. Equal-cost/noisy observations retain
the incumbent. Resource admission, backup tiers, native authority, exact
incarnation/commit validation and Product accounting do not change. Removing
the unused payload argument and updating callers is mechanical cleanup only.

The existing test
`queue_growth_beyond_one_quantum_preempts_lower_flight_owner_hysteresis`
asserts the old byte policy explicitly; revise it transparently to test the
corrected service-time behavior. Keep
`material_completion_gain_preempts_lower_flight_owner_hysteresis` and verify
blocked-owner fallback plus request/datagram callers.

## Tradeoff and scope

The incumbent may retain traffic when the challenger's predicted advantage is
inside measured jitter, even if its byte queue is larger. That is the intended
timing-hysteresis tradeoff, not an additional resource allowance. No new wait,
constant, discovery state, controller, protocol preference or queue limit is
justified here. The separate busy-but-faster queue-unready branch remains
unchanged, as do unknown-capacity, aggregation and rate-evidence limitations.
