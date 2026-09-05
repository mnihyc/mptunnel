# Server restart: lost stream state poisons recovery

Date: 2026-09-05. Examined production revision: `d1a99ad` (v0.4.8).
Status: bounded correction accepted locally; targeted transport and lifecycle tests pass.

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
