# MPTUNNEL Tokio fork

This directory is a local source fork of Tokio 1.53.1. It exists to carry a
small multi-thread scheduler service change while keeping the dependency's
public API unchanged. The upstream README is retained as `README.md`.

## Provenance

- Upstream package: Tokio 1.53.1, repository `https://github.com/tokio-rs/tokio`.
- The crate's `.cargo_vcs_info.json` records upstream commit
  `75fef53d0a8590c2d1dbb63672aa7b7d1ef51155` and the repository path `tokio`.
- Before the path override, the root `Cargo.lock` pinned registry Tokio 1.53.1
  with checksum
  `202caea871b69668250d242070849eb495be178ed697a3e98aebce5bc81a0bed`.
  The current lock uses this local path package, so its Tokio entry has no
  registry checksum. The source package retains upstream license and metadata.
- The scheduling changes are limited to
  `src/runtime/scheduler/multi_thread/stats.rs` and
  `src/runtime/scheduler/multi_thread/worker.rs`. The added
  `tests/rt_service_budget.rs` exercises the fork. `Cargo.toml` registers that
  integration test; the root `Cargo.toml` patches crates.io Tokio to this path.
- The standalone test lockfile pins Mio 1.2.2, matching the application's
  selected event backend. Other standalone test dependencies retain the
  upstream lockfile's versions.

## Change and scheduling semantics

The multi-thread worker keeps Tokio's existing `event_interval` tick trigger
and adds a second trigger when a worker's current scheduled-work batch has run
for at least one millisecond. The check is in `Context::maintenance`, before
the next scheduled task is obtained. A due check ends the stats batch, calls
the existing `park_yield` path with a zero timeout, runs ordinary core
maintenance, and starts a new stats batch.

This is cooperative. It cannot interrupt a future poll already executing,
preempt a LIFO chain, or run while the worker is descheduled. Tokio's
`run_task` can process up to three LIFO-slot tasks before returning to the
maintenance check. The measured elapsed time uses `std::time::Instant`; Tokio's
test clock pause/advance does not advance this clock.

The elapsed timestamp is reset when a scheduled-work batch starts. Tokio ends
the batch before work stealing, starts another after a successful steal, and
starts another after park returns. Search and parked time are therefore not
included. The guard bounds one continuous batch, not the interval between all
driver calls. The original tick trigger remains necessary, including across
successive short batches. `event_interval(0)` continues to panic as before.

The zero-timeout park reuses the existing driver and wake machinery; it does
not sleep. The IO driver releases pending registrations, polls Mio, maps
returned readiness to Tokio readiness, and wakes tasks. The time driver also
processes due timers, and Tokio drains deferred wakes after returning from the
park path. On a multi-worker runtime the driver is shared and guarded by a
try-lock. A worker that cannot acquire it during a zero-timeout attempt returns
without polling it; this is an opportunity to service runtime events, not a
per-worker guarantee that a native poll occurred.

Ending batches more often also updates `Stats::task_poll_time_ewma`. That EWMA
feeds Tokio's tuned global-queue interval and runtime metrics. Changes in batch
length, wall-clock descheduling, or task-cost mix can therefore affect queue
polling and reported batch statistics. This is the main scheduler side effect
to watch in performance tests.

The change is only in the multi-thread scheduler. The current-thread scheduler
retains its existing event-interval loop. The MPTUNNEL application uses a
multi-thread runtime and may configure one worker, so the one-worker
multi-thread case is relevant; a current-thread test does not cover this
change.

## Why one millisecond

The turn count bounds the number of scheduled tasks, not their cumulative
processing time. A busy batch containing expensive polls can postpone feedback
and timer service even when no individual poll is unusually long. Earlier
maintenance lets the application observe that feedback and release retained
ownership, rather than compensating with smaller byte windows.

One millisecond is a measured batching policy, not a universal optimum. It
allows cheap tasks to retain normal turn-count batching and brings maintenance
forward for expensive continuous batches. Unconditional polling every turn
adds unnecessary driver work; reusing global-queue tuning would couple two
different scheduling decisions. The additional clock checks and maintenance
cost must be evaluated with both expensive mixed traffic and cheap-task/single-
carrier controls when updating this fork. The application retains its original
buffer capacities, transport settings, and recovery authority.

The value is not a user-facing tuning knob or a deadline. It is a fixed,
cooperative threshold that prompts the existing maintenance path after a busy
batch crosses the profile. A single poll/LIFO chain, descheduling, driver-lock
contention, event-buffer limits, or later task scheduling can all make actual
event-to-task service take longer.

## Regression test

Run the focused scheduler integration test from the repository root:

```sh
cargo test --locked --manifest-path crates/tokio/Cargo.toml \
  --features full,test-util --test rt_service_budget
```

The test keeps self-waking tasks busy with deliberately longer-than-one-
millisecond polls, raises `event_interval` to isolate elapsed service, and
checks timer completion with one and two workers. Its network-enabled case
checks socket readiness after `block_in_place` core handoff at both worker
counts. It asserts service before a generous scheduler-poll limit and uses an
outer watchdog; it does not assert a hard one-millisecond latency bound. The
CI invokes this test from the Linux release-quality job and for the Windows
and macOS native matrix targets. A passing integration test is useful runtime
evidence, not a formal proof of all interleavings or platform behavior.

The source repository carries this directory and the root path override, so
source builds compile the fork. Deterministic binary release archives use an
explicit runtime-file manifest and do not package crate source; the fork is
included in the resulting executable. MPTUNNEL itself is marked
`publish = false`, so this is not a separately published Tokio crate.

For source-level review, the main anchors are `stats.rs` batch timestamp and
EWMA update, `worker.rs` `Context::run`, `Context::maintenance`,
`park_yield`/`park_internal`, and `builder.rs` `event_interval` contract.
