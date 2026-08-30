# MPTUNNEL Quinn protocol patch

This core crate contains the crates.io source for `quinn-proto` 0.11.17,
checksum `04759210543be93709136e28212294a659ef5001836ff4eab4d663e4529bba83`,
plus MPTUNNEL's congestion-control and private-Initial extensions. The
upstream MIT and Apache-2.0 licenses are included unchanged.

The release baseline is official tag `quinn-proto-0.11.17`, commit
`0343120eb7ccdd067a7e975613b96190c8562bf7`. Its release lineage carried the
datagram-buffer refactor without the earlier main-line pruning chain, and the
tag therefore decrements `payload_bytes` twice after `pop_front()` while also
testing capacity before accounting for the new datagram payload. The public-API
`datagram_drop_pruning_preserves_accounting` reproduction against the exact tag
deterministically panics from that accounting error.
MPTUNNEL retains the bounded official-main correction chain
`7b497af3515a138c9a26a455d4b7084c77ed668f`,
`8da45f49af1866559194f78db53bc683a6651eb2`,
`107b6ef5e0b2205e8ae0e2a0077e98886307f610`,
`88c4e96d119e1ada071356986415de8294a89d65`, and
`c50f83bc4f5df16aa71d05e2d20e8f2b04ae4f62`. These commits make pruning a
single-owner operation, account for the new datagram payload before pruning,
perform overflow-safe capacity checks, and reject a datagram larger than its
configured send buffer. The 0.11.17 tag is not an ancestor of current Quinn
main, so this is an explicit retained upstream-main correction rather than an
unrecorded local transport tweak.

One bounded local correction completes that same memory invariant beyond the
five-commit official-main chain. Main commit
`35fe3379205ed2ace0e6a858f60f3a8a2ff6510e` intended the configured send
buffer to bound payload plus queued `Datagram` metadata, and
`send_buffer_space()` subtracts the next entry's metadata accordingly. Its
admission helper added only the new payload to current memory, however, so an
empty one-byte buffer accepted a one-byte datagram in both drop and non-drop
modes and then held more than the configured bound; drop mode could exhaust
the queue and still push. Admission and capacity checks here use checked
arithmetic for both the new payload and its metadata. The peer-advertised
maximum remains payload-only. Separate public-API tests cover both modes.

The production congestion controller is BBR3, maintained from Quinn PR #2481
head `e19f9e25` and completed with the draft-06 lifecycle and recovery-scoped
spurious-loss undo proven in standalone candidate
`a41a12ac7464f2b1165d76997881e64ca7dac002`. The older BBR and `min_max`
modules remain only for API compatibility and their existing tests; MPTUNNEL's
initial and fresh-network-path constructors select BBR3. The semantic
deviations from upstream 0.11.17 are:

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
- BBR3 treats each received ACK as one draft-06 transaction: per-packet
  callbacks initialize and accumulate one ACK-local rate sample using the
  newest packet's send state, then ACK-batch completion generates the current
  rate and runs the model and controls exactly once. The completed sample is
  retained until the next ACK epoch for Quinn's post-ACK or timer-driven loss
  detection;
- Quinn's authoritative application-limited send indication is stamped before
  the packet snapshot and idle-restart decision. ACK processing expires an old
  application-limited interval and may re-arm the current interval in the same
  transaction, so stale idle/control traffic cannot suppress later bandwidth
  evidence;
- draft-06 section 5.5.9 ACK aggregation feeds every completed ACK into one
  dynamic windowed maximum atomically: its horizon is one packet-timed round
  before `full_bw_reached` and ten rounds afterwards;
- raw minimum RTT remains propagation and ProbeRTT evidence, while ordinary
  flight may use the packet-qualified, two-observation operational-RTT model
  documented in the MPTUNNEL RFC;
- a sender-local loss-compensation policy corrects aligned delivery-rate and
  delivered-volume evidence by at most the observed loss. It composes with,
  but does not replace, the draft's 2% residual congestion-loss objective;
  MPTUNNEL selects 10% by default and a path may explicitly select zero for
  draft behavior. Ordinary loss classification uses a deterministic
  three-operating-round carry-over envelope at completed packet-timed
  boundaries, so correlated placement cannot repeatedly turn a population
  allowance into per-round false congestion. Unknown evidence stays
  conservative, and ECN and persistent congestion retain their native response
  authority. In Startup, an aligned compensated crossing retains the high-loss
  exit only for an exact epoch that was and remains application-limited;
  backlogged acquisition uses the compensated full-bandwidth plateau, while
  raw signals, zero policy, and ProbeBW retain native behavior;
- an opt-in controller hook lets a genuinely new network path start fresh
  congestion state while retaining a connection-scoped instrumentation owner;
  controllers that do not implement the hook retain Quinn's factory-reset
  behavior, and NAT-port rebinding still clones the existing path state;
- BBR3 startup, ProbeBW, recovery, ACK aggregation, congestion window, and
  packet-feasibility transitions follow the complete draft-06 model carried by
  the tested candidate; and
- MPTUNNEL keeps ACK-derived delivery freshness and path telemetry separate
  from BBR3's model bandwidth estimate. The product endpoint is the sole
  byte-per-second to bit-per-second conversion boundary; and
- outbound datagram pruning uses the official-main correction chain recorded
  above instead of the defective 0.11.17-tag implementation, and completes
  its documented total-memory admission invariant for the new queue entry; and
- stream reassembly retains the 0.11.17 security bound of 1,024 buffered spans,
  but counts actual disjoint byte ranges rather than packet-backed buffers. The
  tag implementation closed a valid connection when 2,049 contiguous full-size
  STREAM frames accumulated behind one missing prefix. Buffer-allocation and
  heap-slot amplification remain independently bounded: metadata-dominant
  fragments are defragmented, full-size packet slices stay zero-copy, and an
  oversized heap is released after defragmentation or stream reuse.

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
endpoint-admission, assembler, and packet tests cover these changes. No
unrelated upstream source file differs semantically from 0.11.17 except for
the documented datagram and assembler corrections. Preserve the local private
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
- <https://github.com/quinn-rs/quinn/security/advisories/GHSA-4w2j-m93h-cj5j>
- <https://github.com/quinn-rs/quinn/pull/2694>
- <https://github.com/quinn-rs/quinn/issues/981>

## Updating the upstream baseline

Treat an upstream refresh as a performance-sensitive transport change, never
dependency housekeeping. Use a clean worktree and one exact candidate version:

1. Pin one Quinn release baseline and one reviewed PR #2481 head. Diff both
   untouched sources against this directory and classify every hunk as
   upstream, BBR3 model, transport adapter, or retained MPTUNNEL extension.
2. Port BBR3 and its configuration without silently tuning gains, thresholds,
   or timers. Reconcile the complete draft-06 lifecycle and recovery-scoped
   undo as one model, then reapply only the explicitly documented MPTUNNEL
   operational-RTT and loss-compensation policies.
3. Port the granular controller callbacks, controller-epoch and actual-ECT
   packet provenance, recovery-transaction ownership, two-PTO late-loss
   retention, metrics-driven pacer, and send-quantum GSO bound. Then preserve
   private Initial, H3 compatibility, `SpaceId`, legacy BBR compatibility, and
   ACK-derived MPTUNNEL telemetry.
4. Run formatting and a source-diff review. The BBR3 model must pass its
   focused lifecycle, stretched/reordered ACK-epoch, invalid-rate-sample,
   Startup ACK-aggregation window-boundary, A.15, precautionary-transition,
   packet-feasibility, application-limited, aligned loss-compensation,
   capacity-cut, policer, ECN, persistent-congestion, and spurious-recovery
   tests, followed by the full BBR3 controller suite.
5. Run focused connection tests for partial/final/expired and cross-space late
   ACKs, recovery-transaction completion and CE/expiry taint, controller/path
   ownership, Retry ACK closure, ACK/MTU callback serialization, ACK-only loss,
   ECN attribution, pacing-only blocking, and the actual send-quantum GSO
   bound.
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
