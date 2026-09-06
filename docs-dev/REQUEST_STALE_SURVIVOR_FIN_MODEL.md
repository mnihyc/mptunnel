# Request FIN on a stale survivor

2026-09-06 16:58 UTC. Proven component defect; refined correction passes
component verification. Ordinary integration completes, but timing remains
unaccepted as detailed below.

## Cause and reachable counterexample

The production sender test `stale_survivor_can_finish_after_fresh_attachment_is_removed`
returns `ReliablePathSessionClosed` although a live authenticated attachment has
queue space for FIN. It enters stale state through the ordinary production
guard while another fresh attachment exists, then removes that fresh attachment
as exact terminal processing may do. This is not a fixture that bypasses the
stale-entry guard. OriginalData remains owned by the surviving attachment.

The low-level planner similarly returns `OutputUnavailable`. The sender turns
that selection result into a path error. The relay's pending-FIN branch tries
another attachment; when no new one can be added it ends the logical stream.
An application can therefore fail to finish after a carrier failure even though
its remaining carrier can still deliver the retained data and FIN.

The stale exclusion already exists in the shared selector in `f4206d0` (July).
`3e35938` later replaces the stale set with exact requalification state while
preserving the predicate. Withholding more payload from a non-progressing
attachment is a valid intent. Applying that predicate to terminal control is
not: payload qualification and carrier lifetime are separate authorities.
Existing tests check FIN while a fresh alternate exists, and OriginalData on
a stale sole survivor, but omit FIN after the fresh alternate disappears.

## Minimal correction and proof obligations

OriginalData has its own earlier selection branch, including the established
sole-survivor fallback. Apply that same eligibility distinction to terminal
control: retain the fresh-output preference, but remove stale exclusion when
no non-stale active policy-eligible output remains. Repair retains its existing
exclusion. Reuse `path_is_payload_schedulable` to identify that fresh output;
do not add a state, timeout, retry, protocol preference or capacity parameter.

For fixed topology, qualification, metrics and queue state:

- Every OriginalData and repair decision is unchanged.
- FIN may now use an existing otherwise-eligible stale attachment when no
  non-stale active policy-eligible output remains. With such a fresh output,
  FIN selection is unchanged, including existing native-capacity backpressure.
- Exact membership/eligibility revalidation, native reservation and the existing
  stream-ordered FIN queue remain unchanged. Closed admission still blocks;
  exact removal is still required before an attachment disappears.
- FIN has no new payload owner or credit and cannot clear stale evidence.
- Receiver final-offset validation, half-close and retained-byte settlement
  remain the authority for application EOF, not sender FIN publication.

The tradeoff is that terminal control may traverse a slow stale carrier when
it is the remaining usable output. It may wait for native delivery, but that
is preferable to manufacturing a session failure despite valid ownership.
No bulk-throughput improvement is claimed for this correction.

The RFC already preserves stale sole-survivor use and separates requalification
from carrier closure. Clarify terminal-control independence there without
changing that model or rewriting unrelated recovery rules.

## Evidence and limits

The test uses the actual sender, production stale-entry guard and exact
attachment removal. RED fails in 0.00 s after compilation with
`ReliablePathSessionClosed`. Verification must make that same call publish one
stream-ordered FIN while the surviving path remains stale, and retain the
existing sender/relay/half-close tests.

This is not attribution of the earlier 19.872-second mid-transfer reset.
That diagnostic lacks its initiating operation; five subsequent uploads
complete without reproducing it. Its investigation remains open, as does
mixed-path timing acceptance. No global resolution or release follows from
this component correction.

The first candidate removed stale exclusion from control unconditionally.
The new production test passed, but existing
`stale_path_is_not_selected_for_new_request_data` failed at its FIN assertion:
the stale TCP path displaced a fresh QUIC path. That is a relevant latency
regression risk, not an obsolete expectation to delete. The candidate is
revised to the existing sole-survivor distinction. The old test remains intact.

The refined candidate passes all 244 sender, 246 relay and 253 stream tests.
These include FIN final-offset settlement, retained final feedback, exact
attachment removal, response FIN capacity retry and request half-close.
Command: `cargo test --release --locked -j1 --config
'profile.release.package.mptunnel.opt-level=0' --lib runtime::<module> -- --quiet`.
The root-only opt0 override is for functional tests; ordinary compared binaries
retain the normal optimized release profile. Independent audit is unavailable
because the existing audit workers are usage-limited; do not claim sign-off.

The response control branch already dispatches through native control credit
without request-side stale exclusion. Its bound response FIN capacity tests
pass; no speculative symmetric edit is warranted. The temporary close/gap
diagnostic source overlay is archived separately and removed before building
the ordinary candidate. Its six probe records are retained, including the
incomplete reset rather than silently discarding it.

## Ordinary integration, 2026-09-06 16:58 UTC

Five ordinary optimized mixed uploads complete with exact target confirmation.
All use routed 500 Mbps in each direction and 100 ms aggregate delay. The
healthy ablation removes intentional random loss and jitter; it does not
disable congestion drops. The adverse pair retains asymmetric variable
loss/jitter, the 10 Mbps downlink QoS interval and the short UDP outage.

| Case | Completion s | Confirmed Mbps | Maximum reply gap s |
| --- | ---: | ---: | ---: |
| Healthy control 1 | 30.325399 | 268.840 | 1.246197 |
| Healthy candidate | 45.529800 | 195.403 | 13.069086 |
| Healthy control 2 | 39.745175 | 186.827 | 8.428562 |
| Adverse control | 51.272645 | 135.089 | 4.092118 |
| Adverse candidate | 42.248952 | 145.290 | 4.425137 |

These results do NOT accept the mixed-path stack or prove timing equivalence.
The candidate's healthy upload is fully written to the server target by the
26-second observation; its remaining delay is return-reply delivery. The old
control also exhibits that delay, including an 8.43-second gap on repetition.
This is not evidence for changing the FIN eligibility invariant or for claiming
a bulk gain. Keep the proven component correction as an intermediate commit;
the reverse-direction feedback/drain owner remains an explicit release blocker.
No threshold, path ranking, bulk extent or controller changed in this component.
