# Server terminal reconciliation before parking

Date: 2026-09-07. Scope: the already-observed residual terminal owners in the
repair half-close candidate. This is not a performance-controller change or a
complete attribution of the uncaptured deployed RAM/CPU incident.

## Evidence and exact failure

`REPAIR_TERMINAL_CHURN_TRACE_20260907.json` records 1,934 successful requests,
zero remaining client owners, and thirteen server owners after the final quiet
interval. Eleven have a carrierless diagnostic snapshot. Their response sender
is empty, response bytes are fully Data-ACKed, and the local FIN has been sent
and replayed, but the output membership generation is newer than the relay's
observed generation. Some snapshots also have the request half closed. These
are transition snapshots, not an assertion that every retained owner has the
same final internal state; two owners do not hit that diagnostic hook.

The server relay formerly checked graceful completion near the beginning of
its serialized turn. It reconciled output membership and pending cumulative
Data ACK publication later in that same turn. Thus a legal final wake could:

1. find both Product halves closed, empty sender/reinjection state, but an old
   observed membership generation or a still-pending final ACK;
2. fail the early completion check;
3. consume the new membership or publish the final ACK successfully;
4. park without checking completion again.

No new Product input or capacity event is owed after that transition. A
carrierless owner can then wait for its existing retention expiry; a live
output with completed ACK publication can likewise remain parked without
useful work. This is unnecessary post-terminal retention, not preservation of
a still-open half or an unfinished final ACK.

## Origin and intended protection

Commit `5e1ace67` introduced the membership/ACK publication fence and the later
reconciliation in the per-attachment feedback/recovery model. The fence is
necessary: a changed recipient set must receive the retained latest cumulative
ACK, and one recipient's acceptance cannot discharge another live recipient's
obligation. Commit `3a6d0ea9` subsequently added the exact requalification-ACK
completion guard. Neither guard should be removed.

The defect is ordering, not the obligations themselves. A guard that becomes
satisfied during the current serialized turn must be reconsidered before the
actor parks. RFC Sections 8.3 and 8.5 already separate pending per-attachment
feedback, open Product halves, and terminal ownership; no new timeout, weaker
delivery authority, or RFC model change is needed.

## Bounded correction and tradeoff

Move the existing graceful-completion predicate immediately before the main
wait, after membership/ACK reconciliation, exact requalification-ACK retry, and
sender cleanup. Read live-output state again there; the earlier snapshot is
not the completion authority. Retain the exact membership fence, both-half
checks, final-control/sender drain, reinjection drain, and pending ACK guards.

This may execute the existing synchronous reconciliation work once on a turn
that previously could exit early. It does not add a timer, extra polling,
duplicate FIN replay, transport cancellation, or a new background task. An
open request or response half still retains the owner. A live output with a
blocked final ACK still retains the owner. Ordered final carrier cleanup in
the outer relay wrapper is unchanged.

## Focused verification

`server_terminal_reconciliation` exercises the actual server relay body with
the existing duplex and exact carrier queues. It observes the response FIN and
its replay before the final event; neither a clock nor attachment loss stands
in for Product FIN.

- FIN before last detach: a full live control queue retains the final ACK and
  the owner; the final detach must reconcile and complete without another wake.
- FIN after last detach: detachment alone must preserve the open request half;
  its later ordered FIN permits completion.
- Final ACK capacity release: the same live blocked queue must retain the
  owner, then its capacity wake must publish the ACK and complete in that turn.

At 02:56 UTC, the shared release-mode test executable ran all three tests
against the unchanged server implementation. FIN-after-detach passed. Both
FIN-before-last-detach and final-ACK-capacity failed at the final completion
assertion with `Elapsed(())`; the preceding exact-queue and half-close checks
passed. Total test time was 0.10 s. The assertion deadline is 100 ms while the
fixture's attachment retention interval is 60 s: expiry cannot make this test
pass. Build: `cargo test --release --locked --lib product_recipient -j 3 --
--nocapture`; server filter on that same executable:
`server_terminal_reconciliation --nocapture`.

Exact pre-correction output is currently retained at
`./.tmp/reflection/TERMINAL_RETIREMENT_RED_20260907.json`. After the predicate
relocation, the same release-mode filter passed all three tests in 0.00 s
(shared incremental build: 3 m 12 s). Thus the final membership/ACK wake now
completes the actor, while FIN-after-detach and the live blocked-ACK assertions
continue to preserve unfinished obligations.

The final cleaned executable passes30 selected checks, including the broader
half-close, live ACK publication and exact requalification-capacity guards;
TERMINAL_RETIREMENT_CHECKS_20260907.json retains exact outputs and commands.

The joint ordinary repeat also passes:958 then983 requests complete on one
unchanged mixed client/server pair. Both endpoints reach zero logical/admission
owners after each cycle and remain zero after68.0s final quiet; RSS stays flat
during that final interval. Final TCP-only and QUIC-only32-request controls also
complete and reclaim. See TERMINAL_RETIREMENT_ORDINARY_CHURN_20260907.json.
This accepts the demonstrated terminal-reconciliation correction with the
separate recipient/repair lifetime corrections, not a claim that every one of
the thirteen earlier traced owners or the uncaptured deployed incident has
this one cause. Wider timing and release gates remain open.
