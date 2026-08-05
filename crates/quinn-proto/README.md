# MPTUNNEL Quinn protocol patch

This core crate contains the crates.io source for `quinn-proto` 0.11.16,
checksum `2f4bfc015262b9df63c8845072ce59068853ff5872180c2ce2f13038b970e560`,
plus MPTUNNEL's congestion-control and private-Initial extensions. The
upstream MIT and Apache-2.0 licenses are included unchanged.

The local patch keeps Quinn's public congestion-controller boundary while
aligning its BBR implementation with the delivery-rate and pacing
model used by the reference algorithm. The semantic deviations from upstream
0.11.16 are:

- each ack-eliciting packet records a compact controller delivery snapshot;
  Quinn's existing packet state supplies the packet number and
  application-limited flag at acknowledgement time;
- BBR derives delivery rate from the maximum of send and acknowledgement
  intervals, filters application-limited samples, and updates once per ACK
  batch;
- BBR publishes its gain-adjusted pacing rate to Quinn's token-bucket pacer,
  which preserves fractional refill time and bounds burst capacity;
- an opt-in controller hook lets a genuinely new network path start fresh
  congestion state while retaining a connection-scoped instrumentation owner;
  controllers that do not implement the hook retain Quinn's factory-reset
  behavior, and NAT-port rebinding still clones the existing path state;
- startup, round, recovery, ACK-aggregation, and congestion-window updates use
  the corrected delivery samples and packet identities; and
- an absent minimum-RTT timestamp is not expired, and the timestamp starts with
  the first RTT sample instead of entering ProbeRTT on the first useful ACK.

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

The added BBR, bandwidth-estimator, pacer, path-lifecycle, endpoint-admission,
and packet tests cover these changes. No unrelated upstream source file differs
semantically from 0.11.16. Two BBR test modules use the repository-wide
`tests_[module].rs` naming; normalize those path-only renames before reviewing
an upstream diff. The 0.11.16 refresh first applied the upstream rand 0.10 and
dependency baseline, then ported the MPTUNNEL delta across the overlapping BBR,
connection, crypto, endpoint, and packet files.

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

1. Download `quinn-proto-<version>.crate` from `static.crates.io`, record and
   verify its registry SHA-256, and extract it under ignored
   `.tmp/upstream/quinn-proto-<version>/`.
2. Diff that untouched source against this directory. Separate upstream
   changes from the MPTUNNEL delta; do not copy this directory over a newer
   release or resolve conflicts by accepting either tree wholesale.
3. Port and review the delivery snapshot in `src/congestion.rs`, the
   send/ACK/loss plumbing in `src/connection/{mod,packet_builder,spaces}.rs`,
   the path-reset hook in `src/connection/paths.rs`, the actual pacing hook in
   `src/connection/pacing.rs`, and the BBR delivery model in
   `src/congestion/bbr/`.
4. Port the BBR, bandwidth-estimator, and pacer regression tests with the
   behavior. Port the private-Initial crypto, pre-response admission, and
   packet tests with their extension. Compiling without those tests is not an
   update.
5. Update this document's version/checksum, the package manifest inherited
   from upstream, the root exact `quinn-proto` requirement, and the root,
   benchmark, and standalone Quinn lockfiles in the same change.
6. Run the standalone Quinn suite, the complete MPTUNNEL suite, and the
   preregistered QUIC single-path, aggregation, failover, latency, CPU, memory,
   and wire-overhead matrix. Accept no unexplained regression.

Useful review commands from the repository root:

```bash
git diff --no-index -- .tmp/upstream/quinn-proto-<version> crates/quinn-proto
cargo test --locked --manifest-path crates/quinn-proto/Cargo.toml
cargo tree --locked -i quinn-proto
```

The update is complete only when `cargo tree` resolves this exact local path,
the protected tests pass, and matched evidence shows that the local behavior
survived the upstream port.
