# Request recovery without a new target

Updated: 2026-09-06 21:38 UTC. Component correction; not release acceptance.

## Observed defect and provenance

The ordinary mixed upload in REVIEW_COMBINED_COHORT_20260906 stops at 4.996s,
before the configured QoS step or UDP blackhole. A failure-only diagnostic
reproduces at 26.512s with four attachments still registered. At
Unix1788728959080, request planning reports OutputUnavailable for an unbound
StalePathReinjection. No prior native error is present. Request emission maps
this to ReliablePathSessionClosed; queued dispatch requests a new attachment,
and the relay aborts when no additional attachment opens. Six earlier bound
repair cancellations do not abort. The later server H3_NO_ERROR is teardown.
Exact probe and events: REQUEST_NO_TARGET_RESET_EVIDENCE_20260906.json.

The current producer's no-target fallback is in checkpoint 3a6d0ea. It kept a
generic unbound repair path alongside the new exact-target path. Its apparent
purpose was to retain work when no selected target existed. Retained source
bytes and flight records already preserve that obligation, however. Unbound
dispatch both loses the exact-target cancellation semantics and permits the
older prefer-avoiding fallback to reuse a current copy owner. This is a code
mismatch with the existing RFC recovery predicates, not evidence that native
BBR closed the tunnel or that those predicates should be weakened.

## Minimal state proof

For a missing retained range r, let C(r) be its current exact copy owners and
T(r) the eligible targets after owner/slot exclusion and actual target-service
checks. An expired accepted-copy deadline makes another target examinable; it
does not remove an owner from C(r), free its slot, or acknowledge bytes.

When T(r) is empty:

- The source/flight ledger still owns r. No new queue command is necessary to
  remember it, and no current target authorizes a new publication.
- Session membership can remain nonempty and native delivery can continue.
  Therefore empty T(r) does not imply a closed logical stream.
- A retry that merely reuses C(r) violates the existing distinct-owner and
  vacant-slot requirements. Raising a limit would not correct that error.

The candidate returns pending service instead of enqueuing unbound work.
Existing membership generations, Product-model publications, carrier capacity
wakes and Data ACK events revisit recovery. Source data and accepted-copy
ownership remain unchanged. No expired retry timer is rearmed as a busy loop.
All-carriers-absent handling remains the existing attachment/membership owner;
there is no fabricated path or new timer. Already bound commands retain exact
target cancellation and re-selection. Genuine terminal errors are unchanged.

The RFC already states E(s,r,t), V(s,r,d(t)), and that queueing target-unbound
work does not confer target authority. It permits an unbound intent but does
not require this producer to create one. No new RFC model or parameter is
needed; unrelated tail intents and their rules remain intact.

## Deterministic production-sender evidence

Two variants use one shared fixture, not a simulated replacement scheduler:
retain 4096 source bytes, record their original and accepted alternate copy,
wait until the exact stored copy deadline, then call drive_request_path_recovery.
Both current attachments already own the range, so no new target exists.

Before the correction:

1. With the alternate still eligible, the producer queues an unbound repair
   and production dispatch accepts another 4096 bytes on a current owner.
2. With that alternate becoming stale between queueing and dispatch, the same
   work yields PathAttachmentRequired(ReliablePathSessionClosed), matching the
   captured reset branch.

These are RED tests of the same producer defect. Marking all paths stale
before recovery is not a valid enqueue reproducer: path_recovery_state then
returns no ranges. The capture identifies the unbound-command failure, not
which precise no-target predicate first held; the fixture proves a reachable
case and the correction covers every absence of selected target authority.

After the correction, require no queued command, unchanged retained source
and accepted-copy debt, no expired timer, and resumed exact dispatch after a
fresh membership generation introduces a distinct target. Existing sender,
bound-target cancellation, capacity and lifecycle tests are controls. Both new
tests pass after the deletion. The complete request-sender module passes143
tests; relay246 and stream253 pass (642 total). Strict all-target/all-feature
Clippy passes with warnings denied. Removing the production fallback leaves
the old cross-target queue sum used only by tests; it is now test-only rather
than retained with a dead-code exemption. Ordinary affected-direction
comparisons are in progress. No arbitrary sleep threshold is
part of the model: the fixture awaits its recorded absolute copy deadline.

The first ordinary mixed combined upload completes:1,086,980,096 bytes accepted
locally and confirmed by the target,42.536s including final drain,204.435Mbps,
first confirmation0.504s, maximum positive-confirmation gap4.213s. The latter
measures returned sink confirmations, not a direct clock of every target read.
No client warning/reset appears. This contrasts with the prior ordinary early
reset, but is only one sample. Complete interval series retain zero bins and
confirmation bursts above500Mbps; those bursts are queued acknowledgement
release, not physical service exceeding the500Mbps routed cut. Repeat and
download controls are pending. Timing is not accepted by this bulk mean.

Five ordinary cases and complete one-second observations are archived in
REQUEST_NO_TARGET_ORDINARY_20260906.json. Second mixed upload confirms all
1,008,926,720 bytes at185.927Mbps,43.412s including drain,first0.722s and maximum
confirmation gap3.305s. Neither upload resets. Mixed download gives66.271Mbps
but has a5.319s read gap and one interactive timeout after32 successful echoes;
36 subsequent unavailable slots are not36 independent timeout attempts. Its
bulk request is deliberately duration-limited, not a full8GiB completion.

The TCP upload control is important: candidate confirms563,281,920 bytes at
76.490Mbps over58.913s, maximum confirmation gap0.678s. A fresh unchanged
control confirms363,462,656 bytes at51.627Mbps over56.322s, gap0.553s. Both are
far below the earlier unchanged214.232Mbps sample. Therefore that older high
sample cannot establish a candidate regression, and the fresh lower control
cannot establish a general candidate performance gain. All samples stay in
the record. The tests prove removal of the invalid producer branch; ordinary
runs support upload survival but do not close TCP or mixed timing stability.

The event owners used by the fix also exist in committed HEAD, not just the
held actor overlay: membership generation, armed carrier capacity/model
publication and Data ACK recovery dirtiness. The path-failure and stale-path
producer share this target-binding decision. Adjacent request completion-tail
and response persistent-gap producers already return without enqueueing when
their target is absent. Their distinct authority is not rewritten. No temporary
origin logging remains in source.

## Tradeoff and acceptance boundary

The correction removes illegal duplicate publication and a spurious stream
abort. It cannot make a physically unavailable target deliver faster. If all
slots already own a copy, native delivery, Product acknowledgement or exact
membership change must make progress. That wait is already required by the
RFC; resetting the application or sending another same-carrier copy is not
a valid performance optimization.

No congestion, pacing, loss, qualification, queue, flight, recovery-interval,
or path-preference parameter changes. The held native/actor/paired-repair
composition remains separately unaccepted. Closing this component does not
close the known mixed allocation and QoS timing gaps, browser, independent
aggregation, or sustainability gates.
