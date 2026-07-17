# Release milestones

## 2026-07-17: Architecture, dashboard, and multipath performance

This milestone establishes the first release-ready MPP v2 implementation under
the repository's representative Linux lab and Windows cross-target scope.

### Commits

- `f4206d0`: finalized runtime ownership, the authenticated management API,
  browser dashboard, and symmetric peer diagnostics.
- `3f40ca9`: finalized transport-owned TCP/QUIC scheduling, exact session send
  buffering, multipath aggregation, and conservative Data ACK recovery.

### Performance evidence

All values below are receiver-delivered release-profile goodput from matched
lab rows. They are evidence for this milestone, not hard-coded product targets.

| Representative case | MPTUN | Matched baseline |
| --- | ---: | ---: |
| Single TCP download | 116.679 Mbps | VMess 44.285 Mbps |
| Single QUIC download | 93.442 Mbps | Hysteria2 40.141 Mbps |
| One-flow TCP multipath upload | 283.853 Mbps | MPTCP 155.830 Mbps |
| Two-flow TCP multipath download | 249.987 Mbps | MPTCP 201.068 Mbps |
| Two-flow TCP multipath upload | 259.437 Mbps | MPTCP 266.137 Mbps |
| Two-flow QUIC multipath download/upload | 273.020 / 413.265 Mbps | n/a |
| Two-flow mixed multipath download/upload | 306.734 / 374.396 Mbps | n/a |

The final aggregation guards reached 360.014 Mbps for TCP upload, 469.060 Mbps
for QUIC upload, 394.024 Mbps for mixed upload, and 397.933 Mbps for mixed
download. The representative mixed failover row retained a 0.974-second maximum
bulk gap with zero UDP loss. A deliberately adversarial data-owning TCP
blackhole recovered conservatively in 2.54 seconds.

### Retained model

- MPP owns connection Data Sequence numbers, Data ACKs, receive windows, exact
  range retention, and bounded cross-path reinjection.
- Kernel TCP and Quinn QUIC independently own congestion control, pacing, loss
  recovery, RTO/PTO, and native send credit.
- Path validation is transport-neutral above that boundary. Placement remains
  driven by current completion, queue, flight, receive-window, and reorder
  evidence rather than fixed link roles.
- Persistent Data ACK reinjection waits three recovery intervals of the owner
  carrier. Request-path staleness uses four TCP RTOs or three QUIC PTOs while
  native recovery continues.

Live-owner reinjection, one-interval Data ACK reinjection, speculative final
tail duplication, and private QUIC calibration were rejected and removed after
matched labs showed regressions or no causal benefit.

### Verification

- `cargo test --all-targets`: 875 passed.
- `cargo clippy --all-targets -- -D warnings`: passed.
- Lab Python tests: 133 passed.
- Windows GNU all-target check: passed.
- Windows GNU optimized release link: passed and produced an x86-64 PE binary.
- Wine 10 smoke: Windows platform reporting, mixed TCP+QUIC client config,
  SOCKS listener, management listener, static dashboard, authenticated status
  API, and unauthenticated `401` behavior passed.
- Optimized release build, formatting, and diff checks: passed.

Linux and Windows userspace are release targets for this milestone. Native
Windows Wintun operation, Windows kernel-network throughput/failover, MSVC and
ARM64 packaging still require their native CI or host. macOS and Android remain
best-effort CI targets because native Apple and Android NDK toolchains were not
available in the verification VM.
