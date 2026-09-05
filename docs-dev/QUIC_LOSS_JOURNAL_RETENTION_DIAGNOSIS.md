# QUIC loss journal: rolling retention grows RAM and CPU

Date: 2026-09-05. Examined production revision: `d1a99ad` (v0.4.8).
Status: correction and resource boundary tests pass; independent review accepted.

## Cause

MPP's BBR3 loss-compensation extension stores canonical loss records and
packet-round epochs so later ACKs can reclassify an earlier loss exactly.
`maybe_compact_loss_budget_journal` returns immediately if **any** recovery
transaction, open loss cohort, or current callback batch remains.

Quinn expires each transaction's retained packet evidence after two PTOs.
That does not imply a finite lifetime for their union. On a continuously lossy
path, new transactions can start before older transactions expire. When an
older transaction expires, a younger one still prevents all journal cleanup.
Finalized old records and epochs therefore grow with connection lifetime.

Formally, let transaction starts be `t_k`, with spacing `d`, and finite
retention `R > d`. Rolling intervals `[t_k, t_k + R]` can cover the entire
connection lifetime. The number of live transactions remains approximately
`R/d`, while a collector requiring their union to be empty never runs.
With one recorded loss per interval, journal storage is `O(k)` even though
only `O(R/d)` records still need late-ACK authority.

This requires no malformed packet, restart, race, or oversized Product queue.
MPP enables the extension by default with 10 percent loss compensation; the
production wrapper forwards native transaction-expiry callbacks directly.

## Executed reproduction

The test `loss_budget_journal_compacts_expired_prefix_with_live_suffix` uses
actual `Controller` send, ACK, ACK-batch-end, loss, congestion-event, and
transaction-abandon callbacks in Quinn's order. It does not mutate private
congestion state. It respects the native congestion window, sends ten
100-byte packets per round, and drops one. Packet numbers advance normally.

Observed RTT alternates between 80 and 120 ms. The real RTT estimator updates
after the ACK batch, as in Quinn. Old transaction evidence expires from packet
send time using the current measured two-PTO window, which settles near
368 ms. This avoids assuming that initial RTT variance remains fixed.

```sh
MPP_DIAG_LOSS_ROUNDS=8192 CARGO_TARGET_DIR=./target \
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
  cargo test --locked --manifest-path crates/quinn-proto/Cargo.toml \
  --lib loss_budget_journal_compacts_expired_prefix_with_live_suffix \
  -j 4 -- --nocapture
```

The expected storage-bound assertion fails on the released implementation:
`expired immutable prefix retained: 8192 records for 3 live transactions`.

| Completed rounds | Live transactions | Retained records | Retained epochs | Mean callback work per round in preceding block | One controller clone |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 3 | 64 | 63 | 11 us | 21 us |
| 256 | 3 | 256 | 255 | 19 us | 61 us |
| 1,024 | 3 | 1,024 | 1,023 | 54 us | 233 us |
| 4,096 | 3 | 4,096 | 4,095 | 161 us | 922 us |
| 8,192 | 3 | 8,192 | 8,191 | 362 us | 2,038 us |

This diagnostic completed in 2.04 seconds using simulated network time and an
unoptimized test build. Timings show increasing work; they are not production
throughput, host-saturation measurements, or wall-clock network delays.
The default 256-round run is sufficient to expose the same storage failure.

On this build a loss record occupies 88 bytes and an epoch occupies 48 bytes,
plus each epoch's allocated record-ID vector. After expiring all remaining
transactions and delivering a clean ACK, both journal lengths become zero,
but both deque capacities remain 8,192. Thus even eventual quiescence retains
the allocations until the controller is destroyed or explicitly shrunk.

As an illustrative exposure calculation, 300 Mbit/s of transmitted traffic,
1,200-byte packets, and 10 percent loss produce about 3,125 loss records per
second. At 88 bytes plus an 8-byte epoch reference per record, that alone is
about 300,000 bytes/s, or 1 GiB/hour, before epoch structures, allocator slack,
and temporary copies. This is a calculation, not measured RSS from the user.

## Why CPU rises with RAM

- `advance_compensated_loss_budget` scans retained history to select pending
  records and again to mark consumption. Other loss-round and replay paths
  also scan the historical collection.
- `Bbr3::clone_box` uses derived `Clone`, including the full record deque,
  epoch deque, and each epoch's record-ID allocation.
- MPP's `UdpPathConnection::tx_metrics` obtains a scheduling shape and a
  congestion snapshot, each through a deep controller clone. Both Quinn
  snapshot APIs perform the clone while holding the connection-state lock.
- Active server metric polling uses `SRTT / 2`. At 100 ms RTT this path alone
  can perform about 40 full-journal copies per second, in addition to native
  authority update snapshots. Increasing history therefore increases copying,
  temporary allocation, and time during which packet processing cannot acquire
  that lock.

Periodic loss-free gaps may accidentally allow complete cleanup. Lower RTT
variance may shorten overlapping lifetimes. These conditions explain why the
symptom can appear intermittently rather than on every connection.

## History, model mismatch, and missed acceptance

Commit `61c2059` (2026-08-31) introduced this journal to fix loss compensation
under random loss and exact late-ACK correction. Its purpose remains valid:
adding back lost-byte credit is not an exact refund after the credit bucket
has clamped or its capacity has changed. A checkpoint and replay of remaining
mutable history preserve those semantics.

The incorrect assumption was that finite individual transaction lifetimes
make the entire history bounded. Existing tests covered isolated transactions,
two-transaction replay arithmetic, and eventual complete cleanup. They did
not cover rolling overlap with no globally empty moment.

RFC Section 17.2 already requires maximal immutable-prefix folding even while
newer records remain mutable, finite byte/item journal authority, and an
explicit `RawOnly` transition on exhaustion. Those stronger requirements
entered `3a6d0ea` on 2026-09-03. The production compactor still implements the
older all-empty rule from August 31. Earlier claims of boundedness and later
release gates therefore did not establish that the required model was
implemented. This is a code/RFC mismatch in MPP's extension, not evidence of
an upstream Quinn or generic BBR3 algorithm defect.

## Bounded correction required

1. Fold every finalized chronological prefix into the exact replay checkpoint
   even while later transactions remain eligible for late-ACK reclassification.
   Delete only records and epochs no future valid event can change.
2. Enforce the already documented journal byte/item authorities and exhaustion
   transition. Preserve exact late-ACK, ECN, persistent-congestion, and zero-
   compensation behavior; deleting mutable history would break those semantics.
3. Check sustained overlapping loss, loss-free cleanup, and retained allocation
   size. Reuse existing replay and congestion tests for semantic controls.
4. Preserve coherent controller/path snapshots while ensuring observation cost
   does not scale with lifetime history. The journal bound is required first;
   any narrower scalar snapshot change needs its own focused justification.

No rate, loss threshold, initial window, or congestion algorithm needs to be
changed to diagnose or correct this retention invariant.

The running process from the user's incident was not captured. This proves a
reachable defect matching simultaneous RAM/CPU growth on the released code;
it does not establish that no additional factor contributed to that instance.
The server-restart attachment failure is documented separately.

## Correction model and evidence

The collector folds finalized epochs in chronological order through the same
`advance_loss_budget_state` recurrence and keeps only the mutable suffix. An
old transaction ID is not itself live authority: membership in the retained
native transaction set is. Open loss cohorts and the current callback batch
remain mutable even if native late-ACK ownership has expired. Only a
contiguous consumed record prefix is removed, preserving record-ID indexing.

For epoch transition functions `F_i`, split finalized prefix `I` from retained
suffix `U`. Storing `S_I = F_I(S_0)` and replaying `F_U(S_I)` is identical to
`F_U(F_I(S_0))`. The collector preserves the original floating-point operation
order; it does not replace nonlinear clamping/rebasing with a summed refund.
Current checkpoint operands are `(C,B)`; lifetime counter frontiers and raw
authority generation remain live bookkeeping and are never rewound. Empty
journals release their allocations while retaining the current budget state.

The 8192-round test now retains three records and two epochs for three live
transactions; both deque capacities are four and become zero after final
cleanup. Test-build round work remains approximately 11 us, and clone work
approximately 6–11 us. The uncollected version reached 362 us/round and
2038 us/clone. These remain diagnostic timings, not throughput claims.
An uncompacted replay oracle confirms exact C/B bits after late-ACK
reclassification across both credit clamp and capacity rebase. Existing
spurious-loss controls (18) and loss-floor controls (13) pass the prefix layer.

The independent `resources.max_quic_loss_journal_bytes` authority defaults to
64 MiB per native path, with no preallocation. This is an explicit resource
policy, not an inferred bandwidth cap or another loss threshold. As a scale
calculation, 500 Mbit/s / 1200-byte packets at 10 percent loss produces about
5208 records/s, or 0.5 MB/s of record/reference history before epoch overhead.
Normal finite late-ACK retention should occupy a small part of the ceiling
after prefix folding; the ceiling covers exceptionally long mutable history.
It is independent of payload flight and configurable, including zero.

Charged allocation includes spare record/epoch/transaction/batch capacity and
each epoch's ID array. Every item has nonzero size, so a finite byte ceiling
also implies a finite item bound. Allocator bookkeeping, native packet buffers,
and temporary coherent snapshots are not part of this retained-journal limit;
it must not be advertised as a total process-RSS ceiling.

Exhaustion follows the existing RFC's absorbing RawOnly transition: discard
replay and native-undo authority, preserve current native controller state,
and continue without loss compensation for that native path epoch. A real
unresolved loss cohort remains subject to native handling once; exhaustion
without such a cohort cannot synthesize congestion. A fresh native path starts
with the configured policy again. No native gain, startup geometry, or loss
threshold is tuned. Boundary/undo acceptance is recorded below.

## Final targeted acceptance — 2026-09-05

The combined prefix/resource correction passes five focused boundary tests:

- pinned history reaches the configured allocation ceiling before a new loss;
- journal-only exhaustion during a clean epoch creates no native congestion;
- exhaustion during a second missing-snapshot loss does not reopen a consumed
  callback batch (found in independent review and corrected before acceptance);
- native rate/window and pending bandwidth advancement survive the transition,
  while neither old nor future recovery transactions can resurrect undo;
- zero budget, record-ID exhaustion, authority-generation exhaustion and invalid
  compensation frontier enter RawOnly without panic or fabricated loss.

Three prefix/oracle/rolling cases, 18 existing spurious-loss controls and 13
existing loss-floor controls also pass on the combined source (39 targeted
tests). The final 8192-round run still retains three records/two epochs and
releases empty capacities to zero; additional capacity bookkeeping keeps
diagnostic round work flat at approximately 15–17 us. This supersedes the
prefix-only callback timing above and remains far below the released growing
history's 362 us final-block timing. No production throughput claim is made.

Independent review accepts allocation accounting, exact replay, once-per-cohort
handling and absorbing undo invalidation. Root integration verifies the
independent TOML/default/MuxLimits value and identical resource policy for
initial, cloned and fresh native controllers. The mutable-history limit is
documented in shipped examples and the full reference; RFC Section 17.2 and
the maintained Quinn delta describe its model and resource scope.

Project `cargo fmt --all -- --check` and
`cargo clippy --locked --lib --tests --all-features -- -D warnings` pass.
The extra standalone Quinn `--lib -D warnings` check reports two pre-existing
style warnings (`use_self` in assembler and `collapsible_if` in the existing
BBR raw-response condition). Both source forms are unchanged by this work.
The standalone warnings-denied check therefore remains non-green on this
pre-existing lint baseline; no unrelated source cleanup is included. This is
separate from the passing project Clippy gate and targeted runtime/model tests.
