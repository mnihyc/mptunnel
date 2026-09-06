# Native rate authority must preserve Product qualification

2026-09-06 UTC. Status: exact defect reproduced RED and corrected GREEN;
performance gate NOT passed. This corrects the existing RFC,
not a new congestion controller or allocation policy.

The exact source/test change is archived in
`NATIVE_PRODUCT_QUALIFICATION_CANDIDATE.patch`. A correct projection is not
being promoted as a sufficient runtime fix or release acceptance.

Before-correction series and echo outcomes are retained in
`qualification-closure-before-20260906.json`. Each named directory under
`.tmp/reflection/results` contains the full probe, router service, snapshots
and available owner traces. QoS-only/controls disable jitter, random loss and
outage; QoS-jitter disables random loss and outage; jitter/loss/blackhole
disables the QoS step. Combined retains all three. The assignment trace is
steady jitter without configured random loss. The controls cohort uses the
ordinary production baseline, while other MPP rows use the unaccepted excess
candidate. Owner/clock/assignment traces are diagnostic builds, not a matched
throughput ranking. Echo timeout closes that connection: later unavailable
slots must not be called independent timeouts or claimed recovered.

## Counterexample and practical impact

The 500/500 Mbps routed, asymmetric-jitter mixed-carrier trace
`mixed-steady-down-closure-assignment-rank-0906` records the QUIC additional
output rejected with `startup_flight_limit` while its estimated completion is
71.190 ms, versus 2622.854 ms for TCP. TCP then receives 11,468,800 further
original bytes in that contiguous dispatch block. Other blocks repeat this
pattern. These are scheduler observations and assignments, not wire service.
The separate receive-frontier trace proves earlier TCP originals can hold
megabytes already received through QUIC. Shared physical congestion remains
another possible influence; it is not needed to explain this admission error.

`server_native_bulk_output_snapshot_at` constructs a fresh `PathSnapshot` and
never projects `entry.product_qualification.qualified()`. Thus its default
`has_durable_product_progress=false` contradicts the same output ledger after
qualification. The ordinary projection does copy that fact. The response
scheduler's initial admission filter reads the missing snapshot fact before
the later exact Product-resource check reads the actual ledger qualification.
The early false-negative makes the later correct authority unreachable.

The effect is conditional on frontier ownership, which explains mixed-mode
sensitivity: a live contiguous owner bypasses the additional-output startup
check. Once another carrier owns the lower range, the same already-qualified
QUIC output is demoted to the startup tier. A complete missing-range ACK can
also put a sole output in additional-output position. This is not an inherent
property of TCP, QUIC, or an unavoidable shared-bottleneck cost.

## Intent, boundary and correction

The native-only projection was intended to prevent Product ACK samples and
peer hints from becoming QUIC's native service-rate authority. That separation
is retained. The omission is already present in the preserved v10 checkpoint
`3a6d0ea`; file history does not justify blaming a later typed-rate cleanup for
introducing it. A checkpoint is not proof of the original development date.

The model has independent coordinates:

`state = (lifecycle, Product qualification q_i, native rate authority)`

RFC section 15.1 already requires current-generation exact OriginalData ACK
coverage to establish durable `q_i=1`; native evidence creates or revokes
neither Product qualification nor its configured envelope. A projection of
native rate authority must preserve `q_i`, not reset it. Numeric Product rate
expiry also leaves it unchanged. Real lifecycle/qualification revocation must
still clear it, and a new incarnation must not inherit it.

The bounded correction is to project that one ledger-owned boolean in the
native snapshot, identically to the ordinary snapshot. This does not borrow a
Product rate, make native ACKs count as Product proof, enlarge any envelope,
change an ETA, choose QUIC by name, relax Apply fences, alter reordering or
restore ambiguous-ACK qualification. No new RFC policy is necessary; a brief
explicit projection requirement may clarify this existing invariant.

## Proof and acceptance obligations

RED: the real Native binding test fails at the qualified snapshot assertion,
after confirming the exact observation ledger reports qualified. This removes
the ambiguity of synthetic scheduler fixtures that directly set the missing
boolean true. The one-field implementation correction was applied only after
that failure. All 153 response tests and 90 sender tests pass, including the
new producer-boundary regression, ambiguous qualification and lifecycle/stamp
fences. These first component passes used the experimental native-reordering
tree. After removing that candidate, all 247 `response::` tests also pass on
the ordinary native tree, and its normal release build succeeds. The patch
does not depend on the experimental receive-history or reordering policy.

- A real Native response binding starts unqualified, receives exact tagged
  qualification coverage, and publishes matching qualification in both its
  observation and scheduling snapshot. Preserve the native rate when a much
  larger Product diagnostic rate exists; preserve qualification after that
  numeric evidence disappears; clear it after genuine ledger revocation.
- Existing additional-output, ambiguous-ACK, replacement, startup, native
  direction/instance/stamp and queue-resource tests must remain green.
- Compare the exact receive-history candidate with and without only this
  projection correction, and ordinary production control with and without it.
  Include mixed and QUIC-only jitter, QoS history and outages, both directions;
  retain first-body timing, read-gap series and echo failures, not just means.

This cannot alone prove a sustained optimal allocator, eliminate physical FIFO
drain after a 500-to-10 Mbps collapse, or waive the remaining native-reordering,
TCP observation, aggregation, browser and sustainability gates. Those remain
in CURRENT_CLOSURE_PLAN. Do not combine their hypotheses into this fix.

## Runtime results and architecture checkpoint

`qualification-closure-comparison-20260906.json` preserves the complete
application-bin and echo series. With the ordinary native transport, mixed
combined control averages 16.693 Mbps with a 3.001 s maximum gap and 73
successful echoes; the projection correction averages 8.053 Mbps with a 1.605 s
gap but an echo timeout after three successes. These single random trials do
not isolate the size of a causal throughput change, but they do not establish
practical non-downgrade. The correction cannot ship alone as a performance fix.

With both the existing native receive/excess candidate and the projection
correction, mixed combined averages 71.374 Mbps with a 13.519 s gap; QUIC-only
averages 86.002 Mbps with a 7.054 s gap spanning the UDP outage. Neither passes
the global gate. The mixed gap starts before the QoS step. Across that gap,
native QUIC ACK totals continue advancing by tens of megabytes. That excludes
complete absence of native network service, but not native stream head-of-line
blocking: exact MPP receive-frontier ownership must distinguish that from an
earlier TCP-assigned Product range.

Initial post-correction jitter-only runs (mixed 206.332 Mbps / 2.477 s gap,
QUIC 198.709 Mbps / .238 s) overlapped compilation and are pressure diagnostics,
not matched acceptance. During the following control a host test compiler was
SIGKILLed, the 25 s case runner took 43 s, and the next router initialization
failed before QUIC started. Only 6.5 GiB host RAM was visible, with substantial
swap use; container OOM flags were false. Kernel OOM attribution is not proved.
Router reinitialization subsequently succeeds unchanged. Do not diagnose a
Product defect from that setup failure or tune the runner to hide it. Builds
and measurements are now serialized.

Decision: preserve the independent-state correction and its RED/GREEN as a
candidate, but do not equate restored eligibility with a sound sustained
allocation/recovery model. In particular, do not retain the qualification bug
as an implicit QUIC suppression policy, claim a higher mean closes a long read
gap, or alter a rate/queue/timer threshold to mask either. Next attribution
records qualified path selection, native progress, the exact missing ordered
range and reinjection on one timeline. Any allocation/repair redesign must
follow that owner evidence and revise its RFC model explicitly.
