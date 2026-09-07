# Delayed startup enrollment after FINAL

Date: 2026-09-07. Category: already-observed stream lifecycle / error scope.
Status: source-reachable ordering and transport RED failures confirmed;
bounded correction and targeted GREEN verification complete, awaiting commit review.

## Original purpose and failure

Commit `3a6d0ea9` introduced the frozen response startup plan and the
post-FINAL enrollment rejection in `src/runtime/stream/response/startup.rs`.
The purpose was sound: a finite startup prefix and one immutable retained set
must not become a repeatedly refillable enrollment opportunity. Commit
`89037a89` later distinguished CREATE from STARTUP and extended the same guard
to non-ORDINARY phases; it did not introduce a cross-carrier completion fence.

The missed distinction was local operation completion versus remote receipt.
`open_client_tcp_stream_on_connection` writes OPEN and the zero credit frame,
then flushes. The separate awaiting opener can expire before acceptance.
`ClientTcpOpenCancellation` and `expire_client_tcp_pending_opens` append an
ordered DETACH, but do not wait for a remote cancellation acknowledgement.
`settle_client_return_plan_open_result` marks that candidate Failed;
`ClientReliableReturnPlan::prepare_final` then omits it. FINAL publication uses
the remaining accepted attachments and cannot retract an already-written OPEN
on another carrier.

A legal two-carrier sequence is therefore:

1. CREATE for stream S is accepted on A; another stream T already uses B.
2. The requester writes S's correctly frozen STARTUP on B before its deadline.
3. B's peer consumption or forward delivery is delayed. The local attempt
   expires, settles Failed, and queues DETACH behind OPEN on B.
4. The requester publishes FINAL `{A}` on A after every candidate settles.
5. The server receives FINAL on A before the older OPEN on B.
6. The server must refuse obsolete enrollment without reopening FINAL.

There is no total order between A and B. No finite local timeout proves that
B's peer already consumed the OPEN or its later DETACH. The same ordering is
possible with an independent QUIC request stream. No forged phase, invalid
ordinal, target mismatch, or server restart is required.

The former server implementation returned `RuntimeError::Protocol` for step 6.
That escaped the registry and TCP `ServerTcpStreamState::open`, then
`ServerTcpPathSession::run_active`. Session teardown consequently retired B and
all its sibling attachments. The state was not corrupted: validation occurred
before output, incarnation, or evidence publication. The shared-carrier failure
was unnecessary, and conflicts with the existing RFC smallest-safe-scope and
operation-local refusal rules.

## Bounded correction and tradeoff

After ordinary shape and frozen-signature validation, an otherwise valid
CREATE/STARTUP against Finalized is a typed obsolete-enrollment refusal. The
binding publishes nothing; the registry uses its existing Rejected outcome;
TCP sends STREAM_DETACH and continues its actor. QUIC sends STREAM_DETACH and
finishes only the proposed native request stream before any output-detach guard
or repair binding is armed.

Do not promote the obsolete OPEN to ORDINARY. Do not mutate FINAL or its prefix
ceiling. Do not reset the logical stream, catch arbitrary Protocol errors,
weaken invalid shape/signature/ordinal checks, or alter absent-state restart
RESET behavior. A future legitimate ORDINARY attachment remains explicit.
Singleton behavior is unchanged.

The tradeoff is rejecting a stale attachment without using the stronger
carrier-wide penalty. It adds no timing parameter, rate heuristic, new probes,
or retry loop; it uses the existing attachment-refusal frame instead of
collateral carrier teardown. It prevents avoidable collateral retirement; it does
not claim to eliminate genuine QoS stalls or establish a throughput gain.

## Targeted verification

Command (existing local release cache, no profile overrides):

```sh
CARGO_BUILD_JOBS=3 cargo test --release --locked --features lab-diagnostics --lib late_startup_ -- --nocapture
```

- Requester settlement control uses the real frozen plan, Opening-to-Failed
  transition, and FINAL publication owner. The already-published phase remains
  STARTUP after FINAL is queued.
- TCP actor regression delivers that legal cross-carrier permutation; asserts
  attachment refusal, harmless trailing zero credit / DETACH, same-carrier
  sibling payload, later explicit ORDINARY acceptance, and retained owners.
- QUIC regression writes the native OPEN before FINAL and delays peer request
  consumption. It checks request-local refusal and EOF, sibling payload, later
  ORDINARY acceptance, and the same surviving native connection.

Before the correction, the exact filter matched three tests: the requester
ordering control passed, while TCP failed with peer TLS EOF (the actual actor
closed its carrier) and QUIC failed with the exact late-final Protocol error.
Result: **1 passed, 2 failed**, execution 0.05 s. No runtime changes preceded
that RED capture.

The correction adds a fourth guard covering CREATE and STARTUP on both an
existing same-channel output and an unpublished output: no model/membership
generation change, no incarnation consumption, invalid signature/ordinal still
errors, immutable FINAL, and later explicit ORDINARY admission.

Production files are limited to `startup.rs`, `attachment.rs`, and `registry.rs`
under `src/runtime/stream/`; the TCP/QUIC adapters reuse their existing refusal
and cleanup behavior unchanged. The RFC change is limited to the specific
post-FINAL arrival paragraph. An independent read-only review checked the
causal fixtures, both early-return branches, validation, restart RESET handling,
and lack of output/evidence publication; no issue was found.

After the correction, the optimized library test build completed in **3m 09s**.
The exact filter matched **4 tests, all passed in 0.04 s**. The existing startup
suite plus restart/reset, cancellation, and attachment-local refusal controls
matched **31 tests, all passed in 0.06 s** on the same executable. One new guard
overlaps the startup suite, so this is **34 distinct passing tests**, not 35.
The broad `restart_` selector also matched two existing application restart
controls; no new application test or behavior was added.

The control command was:

```sh
target/release/deps/mptunnel-3a813700b0d8f97b runtime::stream::response::startup::tests:: restart_ canceled_tcp_open_queues_generation_scoped_cancellation stale_tcp_open_cancellation_cannot_remove_current_generation tcp_detach_distinguishes_pending_refusal_from_live_retirement attachment_refusal_is_stream_local --nocapture
```

Exact command output is retained in
`LATE_STARTUP_FINALIZATION_SCOPE_TESTS_20260907.json`. Full-tree formatting and
`git diff --check` are clean. These tests establish scope and lifecycle behavior,
not a general performance acceptance milestone. No product binary was built and
no commit was made by this subtask.

## Attribution limit

The earlier ordinary mixed-run warning lacked logical stream identity, so this
legal sequence cannot be assigned retrospectively to its bulk or echo stream.
The later native diagnostic did not reproduce late STARTUP and still exhibited
a QoS read gap. This defect therefore cannot explain every observed slowdown.
