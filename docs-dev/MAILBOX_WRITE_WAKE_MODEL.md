# A pending native write does not own Product mailbox readiness

2026-09-06 09:23 UTC. Pre-implementation causal model. This is the next
bounded branch of MIXED_RECOVERY_QUEUE_DIAGNOSIS, not an allocator change.

## Reproduced cause and original intent

In `mixed-combined-down-qualified-input-deferral-0906`, TCP path 1 retains a
STREAM_ACK after a full Product mailbox at diagnostic time 12.767 s. It does
not return to that input until its write completes at 24.940 s: 12.173 s later.
Other deferred frames are STREAM_MAX_DATA, plus PATH_METRICS and STREAM_DETACH
at their normal actor boundaries. The run averages 79.518 Mbps and has a
6.028-second application gap. These observations are causal actor evidence,
not proof that this is the only cause of that application's gap.

The interlock was introduced to preserve one in-flight write/flush transaction
while processing peer feedback. One deferred frame bounds memory and preserves
carrier input order; genuine actor/lifecycle barriers must still be deferred.
However, mailbox-full and an actor-ordering barrier have different wake owners.
The current code stops input polling for either and waits only for the native
write. A Product consumer can free its mailbox without completing that write.
Consequently feedback remains parked even after its actual obstruction clears.

The same distinction is lost in the common QUIC write interlock and in client
TCP routing. This is one resource-wake model defect in corresponding branches,
not authorization to change transport recovery or lifecycle semantics.

History confirms that the common QUIC wait loop already has this conflation
in `3f40ca9` (2026-07-17). The current server TCP transaction has it in the
`3a6d0ea` v10 checkpoint. This is not evidence that the recent qualification
projection change introduced the obstruction, nor that the accepted R1
partial-write ownership correction should be reverted. R1 protects the write;
the missing mailbox wake is an orthogonal requirement.

## Required transition model

There are three routing outcomes:

- Routed: the exact frame has transferred to its Product owner.
- Mailbox pending: one retained frame, the exact recipient channel, and that
  channel's capacity wait. Native write and capacity remain independently
  polled. No later input overtakes the retained frame.
- Actor barrier: retain the frame for the existing outer actor after the
  protected native write completes. Requalification reply-credit pressure and
  terminal/lifecycle transitions retain this class unless separately proved
  safe; they must not wait for a resource only this writer can release.

Mailbox delivery must reserve the real recipient slot and transfer the frame
in the same poll/commit. It must not reserve then drop credit merely to emit a
speculative wake. Before successful transfer, cancellation when the write wins
must return the exact retained frame to ordinary actor handling, not drop or
duplicate it. Closing an old recipient must retain the caller's existing
stream-local versus carrier-terminal interpretation. No registry relookup may
silently deliver an old frame to a replacement recipient.

At most one not-yet-routed input is retained per existing interlock. The
mailbox capacity and writer transaction are unchanged. Native partial writes
are never restarted, cancelled for a capacity event, or re-encoded. Completion
evidence and writer debt remain published only at the current write+flush
boundary. No new timer, buffer size, traffic budget, rate source or scheduler
preference is needed.

## Proof obligations and acceptance

Let `Q` be the recipient mailbox, `m` the single retained frame, and `W` the
pinned write. If `Q` eventually supplies a reservation and actor service is
fair, delivery of `m` must not require completion of `W`. If `W` completes
first, `m` must remain owned exactly once for normal dispatch. If a genuine
ordering barrier is retained, later input and clean EOF must not overtake it.
These are conditional progress and ownership properties, not throughput
claims or permission to read without bounds.

Implementation may use one typed pending-mailbox object that owns the frame,
an exact recipient reservation future, and the preexisting closed-recipient
policy. The native write and that future remain independently polled. Acquire
and consume the real mailbox permit without a cancellable await between frame
transfer and send. If the write wins before that commit, cancel only the
reservation wait and recover the frame unchanged. A channel reservation is
mailbox credit, never transport service or Product ACK authority. A type-erased
permit at the existing carrier/Product port can keep server event internals
out of the carrier API; do not move unrelated lifecycle ownership to implement
the wake. There must still be at most one retained ingress frame.

First RED uses the real client QUIC routing function and production common
interlock: fill a one-slot Product mailbox; observe a second feedback frame
become pending; free the first slot while keeping the native write pending;
require that exact feedback to arrive before releasing the write. RED is
confirmed: `routed=0, retained=true` after the recipient slot was freed while
the write remained pinned. The focused release test failed in 0.10 s after
the 7m38s test build. It depends on no packet-loss or congestion assumption.
The pre-fix test is archived in MAILBOX_WRITE_WAKE_RED.patch. The candidate's
later GREEN result is recorded below; this remains the pre-fix counterexample.

The implementation must also test write-wins cancellation, receiver closure,
original input order, terminal-before-EOF, and requalification reply-credit
pressure. Corresponding TCP and QUIC, client and server producers must preserve
their lifecycle effects. Then rerun the ordinary mixed/QUIC timing case and
affected upload/failure cases. A component pass is not performance acceptance.
Do not use a mailbox fix to waive the independent busy-fast/free-slow
allocation, stale-handoff backlog, or native observation issues.

## Candidate boundary (not yet accepted)

The candidate adds `runtime/path/input.rs`: a three-way carrier input outcome
and one exact-recipient pending-mailbox object. It retains the frame separately
from a cancel-safe owned-slot reservation. Once the slot is ready, its
type-erased send permit wraps and transfers the frame without another await.
This preserves the neutral carrier/Product port rather than exposing server
event types or moving lifecycle state between layers. Allocation occurs only
after an actual full mailbox, not for ordinary successfully routed frames.

The registry returns this pending object for a full Product event queue, while
requalification reply-credit pressure remains an actor barrier. Server TCP and
the common QUIC interlock poll the mailbox independently of the pinned writer.
Client TCP retains its exact stream id and reset-retirement effect until the
pending delivery succeeds or its recipient closes. Client QUIC retains its
carrier-closed error policy; a retired server Product stream remains local and
does not fail a shared carrier. The test-only capacity-probe branch uses the
same mailbox wake, without changing its diagnostic authority.

The first `cargo check --tests --locked` completes, with only the existing
default-feature dead-code warnings. At 09:50 UTC, the focused release build
reports four GREEN tests: the exact production-interlock RED, write-wins frame
preservation, reservation cancellation with no slot leak, and both existing
closed-recipient policies. The native write remains pending during the mailbox
delivery test; delivery does not achieve progress by cancelling that write.

The cached release test artifact then passes all 297 `runtime::path::` tests
and all 253 `runtime::stream::` tests. This includes existing terminal-before-
EOF, writer transactions, requalification, registry teardown and restart
guards. These checks preserve the affected component contracts; they are not
proof that the broader performance cases pass. The ordinary, non-diagnostic
binary is rebuilding for mixed/QUIC comparisons and affected upload cases.
No isolated performance benefit, no independent audit sign-off, no accepted
runtime commit and no release are claimed at this point.

## Ordinary comparison: acceptance held

The ordinary binary builds in 3m19s and is retained as
`.tmp/reflection/bin/qualified-mailbox/mptunnel`. The before executable is
`bin/excess-qualified/mptunnel` under the same directory. Both include the
same still-unaccepted native receive/excess and qualification changes; only
MAILBOX_WRITE_WAKE_CANDIDATE.patch differs. No feature-enabled diagnostics,
compilation or competing experiment runs during these measurements.

At 10:05 UTC, the shared routed cut remains500 Mbps in each direction, with
asymmetric loss/jitter. Combined cases additionally restrict downstream to
10 Mbps at15..25 s and blackhole UDP at30..33 s. Thus the upload experiment
also tests restricted reverse feedback; it is not a symmetric500-to10 upload
capacity step. Full results, timing bins, echo attempts and upload confirmation
semantics are retained in MAILBOX_WRITE_WAKE_EVIDENCE_20260906.json.

| Workload | Before mean Mbps / max gap s | Candidate mean Mbps / max gap s |
| --- | --- | --- |
| Mixed download, combined | 71.374 / 13.519 | 69.782 / 5.357 |
| QUIC download, combined | 86.002 / 7.054 | 74.906 / 4.387 |
| Mixed download, loss/jitter only | 150.994 / 3.442 | 129.597 / .655 |
| QUIC download, loss/jitter only | 164.407 / .507 | 160.994 / .376 |
| Mixed upload, combined, first pair | 237.239 / 2.228 | 168.749 / 3.753 |
| Mixed upload, combined, reverse-order pair | 259.990 / 3.922 | 164.741 / 3.948 |
| QUIC upload, combined | 328.260 / 5.451 | 305.905 / 4.498 |

These are sequential random trials, not confidence intervals or identical
packet histories. Nevertheless the two mixed-upload differences prevent
acceptance. A shorter maximum read gap is also insufficient: the candidate
mixed download spends several loss/jitter-only seconds near3--5 Mbps before
recovering. Its mean hides that sustained slowdown. No scalar metric is a
substitute for ordered progress, confirmation gaps and loaded latency.

The exact mailbox wake defect remains proved. It does not follow that this
composition is a practical improvement, or that the endpoint interaction
causing the worse upload result has been attributed. Next isolate the endpoint
change with the same saved binaries: old client/new server, then new client/old
server. The local runner only gains per-endpoint binary selection; wire,
configuration, impairment schedule and workload are unchanged. Do not tune a
buffer or deadline, or waive the comparison because the component tests pass.

### Endpoint isolation and excluded leads

The endpoint-only pair gives147.976 Mbps with old client/new server and
295.989 Mbps with new client/old server. Their maximum confirmation gaps are
6.746 and1.195 seconds respectively. This narrows the observed interaction to
the changed server in this mixed-upload workload, without proving which
carrier or receive/feedback event causes it. Both transfers complete with
exact final target-confirmed accounting. The full ordinary cases remain
archived, including the less favorable runs.

Static review excludes two tempting but unsupported fixes:

- The legacy frame selector prefers an ingress hint for ACK/MAX_DATA, but
  current cumulative ACK and shared-credit publication use the separate
  per-attachment broadcast in `response/attachment.rs`. Do not remove that
  preference and claim to have fixed this modern publication path.
- A pending ACK capacity subscription is an independent select branch; it
  does not guard the main server Product input branch. It is not evidence of
  an all-attachments publication barrier stopping ordinary input.

Next use existing opt-in diagnostics to join client original/repair assignment,
server receive holes and delivered prefixes, client Data ACK progress, and
TCP writer/observation state. The candidate feature build changes no runtime
model and adds no diagnostic source patch. Use old client/new server versus
old client/old server for this attribution; diagnostic throughput is not a new
ordinary acceptance number. No speculative ACK or allocator correction is
authorized by endpoint localization alone.
