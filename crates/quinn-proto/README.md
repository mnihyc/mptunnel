# MPTUNNEL Quinn protocol patch

This core crate contains the crates.io source for `quinn-proto` 0.11.16,
checksum `2f4bfc015262b9df63c8845072ce59068853ff5872180c2ce2f13038b970e560`,
plus MPTUNNEL's congestion-control and private-Initial extensions. The
upstream MIT and Apache-2.0 licenses are included unchanged.

The production congestion controller is BBR3, maintained from Quinn PR #2481
head `e19f9e25` and completed with the draft-06 lifecycle and recovery-scoped
spurious-loss undo proven in standalone candidate
`a41a12ac7464f2b1165d76997881e64ca7dac002`. The older BBR and `min_max`
modules remain only for API compatibility and their existing tests; MPTUNNEL's
initial and fresh-network-path constructors select BBR3. The semantic
deviations from upstream 0.11.16 are:

- each ack-eliciting packet records packet-space and controller-epoch
  provenance, and the connection forwards per-packet send, ACK, loss, ECN,
  ACK-batch, ACK-frequency, cwnd-limited, and spurious-loss callbacks;
- controller-owned loss evidence is retained for two PTOs with an opaque,
  recovery-scoped undo identity. Expiry precedes late-ACK matching, completion
  spans all packet-number spaces, and only the exact still-current transaction
  can restore the model. Expired evidence or transport-valid CE abandons that
  undo identity, and a fresh controller cannot consume feedback from the prior
  network path;
- actual on-wire ECT provenance is retained with lost packets. Transport ECN
  counters still advance on ambiguous cumulative feedback, while a controller
  CE callback requires an exact, wholly current-epoch cohort and a live packet
  identity;
- BBR3 publishes byte-per-second pacing and bandwidth metrics plus its send
  quantum. Quinn's single token-bucket pacer preserves sub-byte elapsed credit,
  and the connection bounds the actual GSO batch by that quantum;
- an opt-in controller hook lets a genuinely new network path start fresh
  congestion state while retaining a connection-scoped instrumentation owner;
  controllers that do not implement the hook retain Quinn's factory-reset
  behavior, and NAT-port rebinding still clones the existing path state;
- BBR3 startup, ProbeBW, recovery, ACK aggregation, congestion window, and
  packet-feasibility transitions follow the complete draft-06 model carried by
  the tested candidate; and
- MPTUNNEL keeps ACK-derived delivery freshness and path telemetry separate
  from BBR3's model bandwidth estimate. The product endpoint is the sole
  byte-per-second to bit-per-second conversion boundary.

The private-Initial extension keeps QUIC packet framing and TLS unchanged while
allowing both endpoints to supply the same 32-byte input to Initial key
derivation. An endpoint using private Initial keys discards packets that do not
authenticate under those keys before sending Version Negotiation, Retry,
stateless reset, or a TLS certificate flight. The maintained surface is:

- the opt-in crypto-session capability in `src/crypto.rs`;
- rustls client/server configuration and Initial derivation in
  `src/crypto/rustls.rs`;
- pre-response endpoint admission in `src/endpoint.rs`; and
- packet-shape and wrong-key coverage in `src/packet.rs`.

The added BBR3, pacer, callback-ownership, late-ACK, path-lifecycle,
endpoint-admission, and packet tests cover these changes. No unrelated upstream
source file differs semantically from 0.11.16. Preserve the local private
Initial, `SpaceId` and per-packet delivery hooks, and the matching `quinn`/H3
surface when refreshing the controller source.

The exact full-source mirror is deliberate: the required delivery, ACK/loss,
pacing, and network-path hooks cross private Quinn internals and cannot be
maintained as a wrapper crate. Keep the delta limited to the files listed
above, and remove the local patch only when an upstream `quinn-proto` release
includes equivalent hooks and matched regression evidence preserves the
current delivery-sampling, pacing, migration, and minimum-RTT behavior.
References:

- <https://datatracker.ietf.org/doc/draft-ietf-ccwg-bbr/>
- <https://quiche.googlesource.com/quiche/+/refs/heads/main/quiche/quic/core/congestion_control/bbr_sender.cc>

## Updating the upstream baseline

Treat an upstream refresh as a performance-sensitive transport change, never
dependency housekeeping. Use a clean worktree and one exact candidate version:

1. Pin one Quinn release baseline and one reviewed PR #2481 head. Diff both
   untouched sources against this directory and classify every hunk as
   upstream, BBR3 model, transport adapter, or retained MPTUNNEL extension.
2. Port BBR3 and its configuration without tuning gains, thresholds, or
   timers. Reconcile the complete draft-06 lifecycle and recovery-scoped undo
   as one model rather than layering edge-case patches.
3. Port the granular controller callbacks, controller-epoch and actual-ECT
   packet provenance, recovery-transaction ownership, two-PTO late-loss
   retention, metrics-driven pacer, and send-quantum GSO bound. Then preserve
   private Initial, H3 compatibility, `SpaceId`, legacy BBR compatibility, and
   ACK-derived MPTUNNEL telemetry.
4. Run formatting and a source-diff review. The BBR3 model must pass its
   focused lifecycle, A.15, precautionary-transition, packet-feasibility, and
   spurious-recovery tests, followed by the full BBR3 controller suite.
5. Run focused connection tests for partial/final/expired and cross-space late
   ACKs, recovery-transaction completion and CE/expiry taint, controller/path
   ownership, ACK-only loss, ECN attribution, pacing-only blocking, and the
   actual send-quantum GSO bound.
6. With one external target directory, run the full `quinn-proto` suite,
   MPTUNNEL's focused QUIC wrapper tests, and the workspace check. Compiling
   without the protected tests is not an update.
7. Only after those gates pass, run the preregistered same-seed sustained
   collapse gate and the matched product baseline matrix. Accept no unexplained
   regression.

Useful review commands from the repository root:

```bash
CARGO_TARGET_DIR=<external-target> cargo fmt --all -- --check
CARGO_TARGET_DIR=<external-target> cargo test --locked --manifest-path crates/quinn-proto/Cargo.toml
cargo tree --locked -i quinn-proto
```

The update is complete only when `cargo tree` resolves this exact local path,
the protected tests pass, and matched evidence shows that the local behavior
survived the upstream port.
