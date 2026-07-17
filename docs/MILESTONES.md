# Release milestones

## 2026-07-18: v0.1.0 public release (frozen)

Runtime source `1018992` is the release candidate. It adds five-minute logical
session retention across complete carrier loss, the completion-driven
1 s/5 s/30 s/manual dashboard refresh for local and authorized peer status,
and a capability-gated basic-UDP QUIC adapter for limited Windows environments.
TCP and QUIC recovery, congestion control, metrics, and MPP policy remain at
their documented independent boundaries. It also fixes a pre-existing QUIC
stream-half ownership race: native FIN is distinct from product FIN and frame
truncation, and the independently writable half remains available for final
Data ACK and attachment teardown.

The clean final guard completed native TCP and QUIC plus Wine TCP, basic-UDP
QUIC, aggregation, and balanced-path blackhole cases in both directions. Native
QUIC measured 249.664 Mbps download and 229.762/259.757 Mbps across two upload
observations on one high-delay path, then 389.138/433.475 Mbps on five. Wine
basic-UDP QUIC measured 75.418/124.621 Mbps on one and 112.579/159.564 Mbps on
five. The matched Wine balanced-path blackhole completed at 51.591/153.034
Mbps with 2.526/2.096 s receiver progress gaps. All uploads were
target-confirmed and exact.

Local verification passes 912 all-feature Rust tests, warnings-denied clippy,
Windows-target end-to-end QUIC execution under Wine, formatting, lab contracts,
shell syntax, and diff integrity. Release Check run `29611283125` passed the
exact runtime commit across Linux musl x64/ARM64, Windows MSVC x64/ARM64 with
native tests and static CRT, macOS x64/ARM64, and Android ARM64. Final Release
Check run `29612375287` passed the same exact inventory for release-doc commit
`965f282`.

Annotated tag `v0.1.0` resolves to
`965f28280313c3b3cc8ca4ece6339d2a8c2c46bb`. Release run `29612988937`
published the non-draft [mptunnel 0.1.0 release](https://github.com/mnihyc/mptunnel/releases/tag/v0.1.0)
with seven platform archives, seven adjacent checksum files, `SHA256SUMS`, and
`version.json`. A fresh download passed every checksum, and `version.json`
records the exact tag, commit, version, and Rust 1.96.0 toolchain. This tag and
its evidence are immutable; later development belongs to a later release.

## 2026-07-17: matched reference cohort

The adjacent direct, VMess, Hysteria2, MPTCP, and MPP comparison is preserved
in [`docs/PERFORMANCE.md`](PERFORMANCE.md) with exact source and binary hashes,
topology, completion status, target-confirmed upload accounting, and claim
limits. Its measured runtime source is `c196e22`; it is historical reference
evidence and is not presented as the final v0.1.0 binary.

The reference high-delay rows establish TCP and QUIC aggregation in both
directions for that binary. They also preserve the non-ideal single-path facts:
MPP/TCP was below the adjacent VMess download control, and the Hysteria2 upload
control remained a receiver-confirmed lower bound. An isolated Rust 1.96.0
rebuild reproduced the recorded Linux SHA-256 exactly.

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

## 2026-07-17: Platform TCP telemetry and portable Windows capacity

This milestone adds capability-isolated native TCP telemetry for every stated
host family and removes the reproduced portable TCP multipath capacity
regression without changing native TCP or QUIC congestion control.

### Retained model

- Linux and Android consume the stable `TCP_INFO` prefix, macOS consumes the
  stable `TCP_CONNECTION_INFO` prefix, and Windows uses `SIO_TCP_INFO` version
  0. Missing fields remain unknown; partial RTT/window shape remains useful.
- Native telemetry is optional and never grants eligibility. Its absence emits
  one explicit warning and selects a capability rule, not an OS-specific rule.
- Partial native window shape never proves an idle TCP carrier. Drain-based
  reinjection requires exact flight and unsent-queue counters from one snapshot
  and otherwise uses exact MPP product flight.
- A portable TCP path receives one bounded startup flight. After durable
  original Data ACK progress, its product service uses the configured resource
  envelope plus shared stream/reorder limits and socket backpressure. The Data
  ACK rate ranks completion but is not a replacement congestion window.

The alternative attachment-order patch was rejected. Linux native reached
263.187 Mbps with the same initial first-path debt that appeared in the failed
Wine runs; disabling Linux native telemetry reproduced Wine at 34.757 Mbps.
The causal difference was the persistent 512 KiB product feedback window.

### Matched evidence

All rows use the same five 500 Mbps, 180 ms plus jitter paths and a 10-second
single-flow release-profile load. Upload accounting is target-confirmed and
exact.

| Case | Before | Retained result |
| --- | ---: | ---: |
| Wine portable five-TCP upload, cold | 33.791 / 34.809 Mbps | 267.187 / 272.654 Mbps |
| Wine portable one-TCP upload, cold | 158.335 Mbps | 182.160 Mbps |
| Wine portable five-TCP download, cold | 241.031 Mbps | 258.692 Mbps |
| Wine portable one-TCP download, cold | n/a | 137.627 Mbps |
| Linux native five-TCP upload, cold | 263.187 Mbps | 286.313 Mbps |

The retained cold Wine upload used all five interfaces, with 58-83 MB sent per
path, completed without a recovery gap, and exceeded its matched single path.
A second transfer on the established five-path session reached 589.635 Mbps;
it is recorded as steady-session evidence, not substituted for the cold result.

### Verification and limits

- The original generated artifact directory was retired after the release-facing
  evidence and its limitations were retained in `docs/PERFORMANCE.md`.
- `cargo test --all-targets`: 882 passed.
- `cargo clippy --all-targets -- -D warnings`: passed.
- Windows GNU all-target check and optimized release link: passed.
- Linux optimized release build: passed.
- macOS and Android telemetry sources compiled against their target Rust
  standard libraries; native loopback FFI tests are target-gated for CI.
- Wine rejects `SIO_TCP_INFO` with Winsock error 10045. It therefore proves
  portable Windows executable behavior, both-direction TCP aggregation, exact
  delivery, and the explicit warning, but cannot prove the native Windows
  socket API. Native Windows CI owns that loopback test.
