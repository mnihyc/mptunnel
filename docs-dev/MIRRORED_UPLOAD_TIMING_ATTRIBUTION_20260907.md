# Mirrored upload timing: current attribution boundary

2026-09-07. Read-only analysis of the frozen terminal-retirement candidate in
`TERMINAL_RETIREMENT_TIMING_CONTROLS_20260907.json`. No code, build, new test,
timer or numeric policy changed. The raw cell is
`./.tmp/reflection/results/mixed-combined-up-terminal-retirement-controls-0907/`.

## Observed outcome

The target confirms all 508,428,288 accepted bytes in 43.405385 s: 93.708 Mbit/s,
with a 6.215739 s maximum delivery gap and 6.200634 s local-write gap. The
40-second load is followed by 3.405385 s of receipt drain. This completes, but
does not meet the requested fluent timing condition.

The largest zero-delivery-bin run occurs during the 10-Mbit/s QoS interval,
not after restoration to 500 Mbit/s. Probe bins 18 through 22 contain zero
confirmed delivery; neighboring bins contain progress. Management samples
18.006 through 23.007 s likewise show identical client accepted and server
target-write totals. The exact endpoints of the 6.215739-second gap are not
retained as individual events in this ordinary probe, so sample timestamps
must not be presented as exact gap endpoints. Per-second sampling also has
collection skew. Both runtime logs are empty: no hidden source-hook trace is
available in this run.

## Product and native progress are different

During the management plateau, client Product input is 312,523,489 B and server
Product output to the target is 245,414,625 B. Their difference is exactly
67,108,864 B. Reported outstanding Product flight is also exactly 67,108,864 B,
entirely attributed to QUIC; TCP Product flight and active-flow ownership are
zero. Thus the existing 64-MiB Product envelope is fully occupied. Reopening
native flight does not release this Product debt: only exact Data ACK or
terminal cleanup does. This establishes a Product progress boundary, not its
missing native/MPP offset's identity.

| Sample elapsed | Server Product bytes | QUIC counted native ACK bytes | QUIC native flight | QUIC native SRTT | Shared upload router backlog |
| --- | ---: | ---: | ---: | ---: | ---: |
| 18.006 s | 245,414,625 | 231,958,266 | 4,642,702 B | 1,435 ms | 5,275,167 B |
| 20.007 s | 245,414,625 | 233,771,393 | 5,400,000 B | 3,433 ms | 4,002,406 B |
| 22.007 s | 245,414,625 | 235,637,393 | 3,411,600 B | 3,525 ms | 1,733,632 B |
| 23.007 s | 245,414,625 | 235,637,393 | 3,411,600 B | 3,525 ms | 503,906 B |
| 24.007 s | 249,776,001 | 237,755,393 | 1,149,600 B | 4,297 ms | 9,853,052 B |

The router's impaired-direction HTB counter advances 6,278,849 B between
18.006 and 23.007 s, about 10.04 Mbit/s over that interval. Counted QUIC native
ACK progress advances 3,679,127 B while contiguous Product delivery is flat.
This rules out the claim that all native progress stopped. It is compatible
with native or MPP ordered-prefix blocking while later packets progress, but
the exact hole, native timer choice, ACK ranges and packet classifications are
not observed here. A 5.28-MB queue alone represents about 4.22 s of service at
10 Mbit/s; this is a dimensional queue-residence explanation, not proof that
it accounts for the entire application gap or cannot be improved.

The TCP alternatives are not independently idle, empty escape routes. Live
kernel socket observations at 18.006 s contain approximately 13.33 MB of
unsent TCP bytes despite zero TCP Product flight. At 23.007 s about 13.26 MB
remain. Their kernel ACK counters do advance during the interval, even where
the dashboard's earlier native-delivery sample remains unchanged. These are
real ordered native queues sharing the same constrained cut; their contents
cannot be uniquely reconstructed as originals versus repair from these
ordinary samples.

## Does the previous requalification gate recur?

The earlier `UPLOAD_RECOVERY_GATE_ATTRIBUTION.md` proves a specific
unqualified/queue-ready state with 12.26 MB retained OriginalData versus a
524,288-B acquisition allowance. This ordinary run does not expose `q`, F/E,
the selected head, or exact requalification state. Its largest gap cannot be
assigned to that earlier state merely because both involve retained debt.

The current post-outage sequence is materially different. UDP restores at
33.051 s. By 34.166 s, server Product delivery advances from 314,057,417 to
361,354,273 B, while QUIC Product flight grows from 3,556,480 to 6,743,744 B;
by 35.166 s it reaches 55,128,440 B. New QUIC OriginalData is therefore being
admitted again promptly in this realization, not withheld for the previous
multi-second post-requalification interval. This does not exclude a shorter
unobserved acquisition transition.

Carrier instance 4 and native-delivery epoch 16423533392175702592 persist in
the samples. These are not the native activation stamp: the ordinary schema
does not identify controller activation/restore transitions. Do not infer a
particular recovered activation, absence of all late ACKs, or a fixed learned
loss deadline from a native counter plateau.

## Independent reflection and disposition

The lifecycle correction has its own successful finite gate: 1,941 same-
process requests, zero retained owners after each cycle and after 68 seconds
quiet, and flat RSS. That is meaningful reclamation evidence. It is not
performance acceptance for the surrounding composition.

This timing run proves an unsatisfactory combined experience and narrows its
largest upload gap to occupied Product resources plus genuine shared queued
service/ordered delivery during QoS. It does not identify the exact missing
offset or show that a particular held change caused the gap. It also does not
compare the released binary against this candidate under matched conditions;
claiming either improvement or regression against release would exceed the
evidence.

As recorded in `REVIEW_AND_PRACTICAL_ACCEPTANCE.md`, the receive-history/
reordering integration (including same-ACK RTT ordering), allocation-relevant
qualification projection, interlock/cooperative-service composition, paired
repair stream, and stateless/relative ACK representations still lack the full
practical timing acceptance. Their component RED/GREEN proofs must not be
promoted into a claim that this whole stack is faster or usable. No further
implementation is justified by this observation alone; current work is paused
for the requested reflection.
