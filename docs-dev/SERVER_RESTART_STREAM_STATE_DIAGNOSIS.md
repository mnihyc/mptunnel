# Server restart: lost stream state poisons recovery

Date: 2026-09-05. Examined production revision: `d1a99ad` (v0.4.8).
Status: original ORDINARY correction committed in `afbb75a`; R1 blocked-write
and R2 late-STARTUP branches corrected and verified locally, using wire 11.
The current root library suite passes 2,308 tests. Historical RED observations
below explain the original failure and why the earlier correction was incomplete.

## Cause and failure sequence

1. Restart removes the server's stream registry and outbound target sockets.
   The client retains its logical streams and session ID. Fresh carrier
   authentication can succeed with that ID; `SESSION_READY` does not report a
   server incarnation or whether an earlier logical stream survived.
2. Once startup enrollment has completed, recovery sends an `ORDINARY`
   `OPEN_STREAM` with the original stream ID and return-plan signature.
3. `ServerReliableStreamRegistry::open_or_attach_with_ingress` cannot find
   the old stream and enters its new-stream constructor. That constructor
   requires `STARTUP` enrollment and returns the exact reported error:
   `initial stream attachment must enroll in its return plan`.
4. The TCP stream dispatcher propagates this error through `?` to the shared
   carrier actor. The actor exits and retires the carrier. A fresh stream
   already accepted on that carrier consequently loses its transport too.
5. QUIC invokes the same registry, but the error terminates the native HTTP/3
   request stream. It does not by itself prove the entire QUIC carrier failed.
6. The client's retained logical streams keep attempting recovery. A client
   restart removes them, explaining the immediate apparent cure. Carrierless
   retention defaults to 300 seconds, so the demonstrated mechanism is a
   prolonged recovery loop, not proof of an infinite retry lifetime.

An additional error-boundary defect makes a server-only correction incomplete:
`try_handle_additional_path_open_result` returns `Ok(None)` even for an
authenticated `RemoteReset`. Both TCP and QUIC translate `STREAM_RESET` into
that error; the result handler then treats it like an unsuccessful optional
attachment. TCP also rewrites `FailedAfterOpen` to `ReliablePathRetired` when
its slot has changed before the waiting task resumes, without preserving
explicit stream-terminal authority.

## Reproduction and controls

The TCP tests use the existing real encrypted-carrier fixture and execute the
actual server actor. A fresh server context represents the state after a
process restart; no production branch is patched or bypassed.

| Diagnostic | Released behavior observed |
| --- | --- |
| Missing stream + `ORDINARY` recovery | Exact reported error; carrier removed; no stream-local terminal response |
| New `STARTUP` sibling followed by old `ORDINARY` | New sibling first receives zero-credit admission; stale recovery then removes their shared carrier |
| Retained stream + `ORDINARY` on another carrier | Existing stream attaches successfully; one target owner remains |
| Additional-open result receives `RemoteReset` | TCP and QUIC both return `Ok(None)` instead of terminating the lost logical stream |

Four diagnostics pass by asserting the existing failure and its retained-state
control. They are evidence of the defect, not acceptance tests for a fix.

```sh
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
  cargo test --locked --lib diagnosis_restart_ -j 4 -- --nocapture
```

Observed result: 4/4, 0.05 seconds after compilation.

## Why it was introduced and why acceptance missed it

The v10 startup constructor entered `3a6d0ea` and is present in v0.4.7 and
v0.4.8. Its purpose is valid: preserve a frozen, one-shot response topology and
prevent ordinary recovery from enrolling a new startup plan. The v0.4.8
frontier correction in `bfac5b8` did not introduce this failure.

The missing case is an otherwise valid recovery attachment whose target state
no longer exists. Startup validation and retained-stream reattachment were
tested independently; a restarted empty registry was not covered. RFC Section
8.1 describes valid startup and attachment to retained state but does not give
this missing-state transition an explicit stream-terminal outcome.

Additional-open error handling descended from best-effort optional attachment
work and was reused for disconnected recovery. The catch-all branch therefore
discards errors with stronger authority than an optional path refusal.

## Bounded correction required

- Model absent `ORDINARY` recovery as terminal loss of that logical stream.
  Report the existing stream-reset semantics while leaving the authenticated
  carrier available for fresh streams. Record the terminal identity through
  the existing stream lifecycle so delayed startup cannot recreate it.
- Propagate authenticated terminal errors through additional-open settlement,
  including when a TCP carrier slot changes concurrently. Preserve retries
  for actual transient attachment/carrier failures.
- Preserve the existing startup enrollment checks and retained-state recovery.
  Changing `ORDINARY` into `STARTUP` would reconnect a new target socket with
  old offsets and could corrupt application protocol continuity.
- Extend RFC Section 8 with the missing transition when the correction is
  implemented; verify both carriers and fresh-sibling survival.

A process restart cannot preserve an existing end-to-end target connection
whose server-side socket and byte state were destroyed. Correct recovery
terminates that lost logical stream promptly and lets the application open a
new one without restarting the MPP client.

The failed constructor's temporary session/load registrations have RAII
cleanup. This reproduction does not establish a memory leak from that error;
the separately reproduced QUIC journal issue has its own causal chain.

## Accepted correction — 2026-09-05T14:56:00Z

Absent valid ORDINARY and retained terminal IDs now return the typed
`Terminal(RemoteClosed)` outcome. TCP sends STREAM_RESET without attaching or
retiring the carrier; QUIC sends the same reset and finishes only that request.
An absent ID is inserted in the existing bounded closed-ID cache under the
stream-membership lock. This prevents delayed STARTUP resurrection while the
identity is retained; it is not an unbounded replay-protection claim.

Authenticated reset is preserved before optional-open error swallowing,
obsolete task-generation filtering, and TCP slot-replacement error rewriting.
Transient refusals and physical retirement remain attachment-local. No startup
enrollment rule, native congestion parameter, resource default, or wire frame
format changes as part of this correction. RFC Section 8 specifies the missing
transition and acknowledges that destroyed target sockets cannot be resumed.

Targeted acceptance: server TCP session tests 22/22; real QUIC request-handler
tests 17/17; relay lifecycle tests 30/30; stream registry tests 40/40; TCP
slot-replacement terminal/transient control 1/1. The new cases cover absent
state, delayed STARTUP, malformed ORDINARY, retained-state recovery, fresh
sibling survival (including QUIC payload/ACK exchange), obsolete open
generation, and terminal reset arriving after carrier replacement. Existing
duplicate refusal, close ordering, admission, and carrier retirement controls
remain green. These tests establish recovery/error scope, not a throughput claim.

## Closure audit — 2026-09-05T16:20:00Z

Examined HEAD: `6636091` (restart correction `afbb75a`). No further production
behavior has been changed. Two diagnostic counterexamples pass by asserting
the remaining defects; they are not acceptance tests for corrected behavior.

### Terminal reset behind blocked Product output

`control.rs` has a third additional-open receive site inside the local-write
wait. Its non-STARTUP branch bypasses the common terminal-aware settler:
matching results are deferred until the write finishes; obsolete-generation
errors are discarded. The latter subcase is established by branch inspection,
not a separate runtime reproduction.

`diagnosis_restart_reset_waits_for_blocked_product_write` executes the actual
relay actor and its nested select. A test-only ingress seam supplies an ordinary
recovery result after the local sink blocks. Reserving the result channel's
entire capacity proves the actor consumed the authenticated reset. The relay
still cannot finish; releasing the sink immediately yields that same reset.
There is no mock of the failing receive/settlement branch and no network delay.

The branch originated in `444fb38` to let response-startup control progress
while application delivery blocks. Deferring successful ordinary attachments
has legitimate post-write ordering requirements; deferring terminal logical
authority does not. The previous correction updated shared settlers but missed
this bypass. Bounded correction: classify terminal results before either
deferral or generation filtering at every ingress, preserving successful-open
ordering and transient-error behavior. No new timeout is needed.

### STARTUP enrollment reaches the restarted server first

An accepted stream below its response trigger can still contain an unbound
configured return candidate. After server state is lost, disconnected recovery
can bind that candidate to a newly ready carrier and send STARTUP. An empty
registry accepts it as `New`; the client's instance fence accepts the candidate
too. No ORDINARY has arrived to install the previous fix's terminal-ID fence.

`diagnosis_restart_unbound_startup_candidate_recreates_missing_stream` composes
the real client plan transitions with the real empty server registry, for TCP
and QUIC candidates. It reproduces a new server owner for an already accepted
client stream. This is a component/model reproduction, not a full process-
restart transfer. A new owner cannot restore destroyed target sockets or
already-acknowledged bytes retained only by the old server.

The v10 startup model (`3a6d0ea`) conflates permission to create a logical stream
with enrollment of another return path. Freezing known instances correctly
rejects their successors, but deliberately unbound slots are a legal branch.
The earlier test exercised ORDINARY before delayed STARTUP; the opposite order
was not covered. Rejecting every nonzero ordinal is invalid: ranking can select
a nonzero ordinal for the genuine first open.

Bounded model correction proposed, not implemented: make initial creation and
later STARTUP enrollment distinguishable. Only an initial-creation operation
may allocate absent target state; all attachments of an already accepted stream
require retained state and otherwise return a stream-local terminal reset.
Keep the frozen plan, ordinal settlement and FINAL semantics. This needs an
explicit wire/RFC decision; it must not be implemented as an ordinal heuristic,
blind stream-ID reset or an implicit conversion from recovery to creation.

### Verification and remaining authority

```sh
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
  cargo test --locked --lib diagnosis_restart_ -j 4 -- --nocapture
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
  cargo test --locked --lib restart_ -j 4
```

New diagnostics: 2/2, including both underlays in the unbound-slot case.
The broader name-filter control run passes 11 tests (including two app restart
tests, the two new defect assertions, and seven existing recovery controls).
Passing defect assertions do not turn the two gaps green. No new runtime fix,
wire change, commit, push or release is implied by this audit.

## R1 correction — 2026-09-05T17:26Z

The blocked-output ingress now classifies authenticated stream-terminal
outcomes before either deferred-success ordering or task-generation checks.
The exact actor test was changed to require termination while its sink stays
blocked: it failed before the correction, then passed for both current and
obsolete task generations. It still proves only one byte reached the sink.
No timeout, gain, stream credit or successful attachment ordering changed.

All28 relay-control tests and31 lifecycle tests pass. This includes blocked
startup ACK/OPEN/FINAL progress and normal generation/suppression controls.
The R2 diagnostic is still a defect assertion, not fixed behavior; R1's green
result does not close R2 or the entire restart incident. Not yet committed.
# R2 closure — 2026-09-05T17:52Z

The unbound-candidate counterexample first failed the acceptance assertion for
both carrier profiles: a later STARTUP could allocate a new server stream.
Version 11 separates CREATE (initial attempts before acceptance) from STARTUP
(enrollment after acceptance) and ORDINARY. The registry alone owns absent-state
creation permission; the return-plan component owns ordinal membership only.
This corrects v10's conflation, not its finite prefix or finalization model.

Targeted controls cover missing STARTUP on either ordinal and either carrier,
retained STARTUP, CREATE on either ordinal, terminal ID retention, and client
phase construction. The complete root library suite passed 2,303 tests before
the last registry control was added; the six creation controls then passed.
Authentication vectors were independently recomputed from the documented v11
contexts; the TCP carrier prelude remains its independent v1 protocol.

Tradeoff: this is an intentional wire break requiring matching endpoints. It
does not claim durable pre-acceptance CREATE deduplication across server state
loss. No lost target socket or byte-offset state is silently reconstructed.
