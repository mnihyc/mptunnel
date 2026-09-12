//! Passive source-poll observations in the existing server classifier window.

use std::{
    cell::Cell,
    fmt,
    future::{Future, poll_fn},
    sync::atomic::{AtomicU64, Ordering},
    task::Poll,
};

use crate::{Duration, Instant};
use proto::NativeSourceDriverContext;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_EVENT: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone, Copy)]
struct Context {
    driver: NativeSourceDriverContext,
    source: u64,
    poll: u64,
    await_id: Option<u64>,
    parent_await: Option<u64>,
    depth: usize,
}

thread_local! {
    static CURRENT: Cell<Option<Context>> = const { Cell::new(None) };
}

fn window() -> Option<Duration> {
    proto::native_source_window_at(Instant::now())
}

fn emit(event: &str, context: Context, elapsed: Duration, fields: fmt::Arguments<'_>) {
    let unix_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_micros());
    proto::emit_native_trace(format_args!(
        "{event} role=server window=first_native_poll_10_11 event={} driver_connection={} driver_turn={} source_pass={} source_id={} source_poll={} await_id={:?} parent_await={:?} depth={} elapsed_ns={} unix_us={:?} {}",
        NEXT_EVENT.fetch_add(1, Ordering::Relaxed),
        context.driver.connection,
        context.driver.turn,
        context.driver.pass,
        context.source,
        context.poll,
        context.await_id,
        context.parent_await,
        context.depth,
        elapsed.as_nanos(),
        unix_us,
        fields,
    ));
}

/// Give an existing turn an identity only when its actual pass is in-window.
pub(crate) fn driver_context(
    connection: usize,
    turn: &mut Option<u64>,
    pass: u64,
) -> Option<NativeSourceDriverContext> {
    window()?;
    Some(NativeSourceDriverContext {
        connection,
        turn: *turn.get_or_insert_with(next_id),
        pass,
    })
}

pub(crate) struct SourcePoll {
    context: Context,
    previous: Option<Context>,
    budget_before: bool,
    started: Duration,
}

impl SourcePoll {
    pub(crate) fn begin(driver: Option<NativeSourceDriverContext>, source: u64) -> Option<Self> {
        let elapsed = window()?;
        let context = Context {
            driver: driver?,
            source,
            poll: next_id(),
            await_id: None,
            parent_await: None,
            depth: 0,
        };
        let budget_before = tokio::task::coop::has_budget_remaining();
        let previous = CURRENT.replace(Some(context));
        emit(
            "native_source_poll",
            context,
            elapsed,
            format_args!("edge=begin budget_before={budget_before}"),
        );
        Some(Self {
            context,
            previous,
            budget_before,
            started: elapsed,
        })
    }

    pub(crate) fn finish(&self, ready: bool, source_ready: impl FnOnce() -> bool) {
        let Some(elapsed) = window() else {
            return;
        };
        let budget_after = tokio::task::coop::has_budget_remaining();
        let ready_after = source_ready();
        emit(
            "native_source_poll",
            self.context,
            elapsed,
            format_args!(
                "edge=end result={} budget_before={} budget_after={} ready_after={} start_elapsed_ns={}",
                if ready { "Ready" } else { "Pending" },
                self.budget_before,
                budget_after,
                ready_after,
                self.started.as_nanos(),
            ),
        );
    }
}

impl Drop for SourcePoll {
    fn drop(&mut self) {
        CURRENT.set(self.previous);
    }
}

pub(crate) fn skipped(driver: Option<NativeSourceDriverContext>, source: u64) {
    let Some(elapsed) = window() else {
        return;
    };
    let Some(driver) = driver else {
        return;
    };
    emit(
        "native_source_poll",
        Context {
            driver,
            source,
            poll: 0,
            await_id: None,
            parent_await: None,
            depth: 0,
        },
        elapsed,
        format_args!(
            "edge=skip result=NotPolled ready_at_check=false budget={}",
            tokio::task::coop::has_budget_remaining()
        ),
    );
}

struct AwaitPoll {
    context: Context,
    previous: Context,
    phase: &'static str,
    budget_before: bool,
    started: Duration,
}

impl AwaitPoll {
    fn begin(phase: &'static str) -> Option<Self> {
        let previous = CURRENT.get()?;
        let elapsed = window()?;
        let context = Context {
            await_id: Some(next_id()),
            parent_await: previous.await_id,
            depth: previous.depth + 1,
            ..previous
        };
        let budget_before = tokio::task::coop::has_budget_remaining();
        CURRENT.set(Some(context));
        emit(
            "native_source_await",
            context,
            elapsed,
            format_args!("edge=begin phase={phase} budget_before={budget_before}"),
        );
        Some(Self {
            context,
            previous,
            phase,
            budget_before,
            started: elapsed,
        })
    }

    fn finish(&self, ready: bool) {
        let Some(elapsed) = window() else {
            return;
        };
        emit(
            "native_source_await",
            self.context,
            elapsed,
            format_args!(
                "edge=end phase={} result={} budget_before={} budget_after={} start_elapsed_ns={}",
                self.phase,
                if ready { "Ready" } else { "Pending" },
                self.budget_before,
                tokio::task::coop::has_budget_remaining(),
                self.started.as_nanos(),
            ),
        );
    }
}

impl Drop for AwaitPoll {
    fn drop(&mut self) {
        CURRENT.set(Some(self.previous));
    }
}

/// Observe each actual future poll, preserving its Context, Waker and result.
/// Outside an active server source-poll window this adds no records or queries.
pub async fn observe_source_future<F: Future>(phase: &'static str, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    poll_fn(|cx| {
        let trace = AwaitPoll::begin(phase);
        let result = future.as_mut().poll(cx);
        if let Some(trace) = trace {
            trace.finish(matches!(&result, Poll::Ready(_)));
        }
        result
    })
    .await
}

/// Annotate facts already observed by the current source poll, with no new poll.
/// Formatting and logging occur only in the existing server observation window.
pub fn note_source_state(kind: &'static str, fields: fmt::Arguments<'_>) {
    let Some(context) = CURRENT.get() else {
        return;
    };
    let Some(elapsed) = window() else {
        return;
    };
    emit(
        "native_source_state",
        context,
        elapsed,
        format_args!("kind={kind} {fields}"),
    );
}
