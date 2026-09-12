//! Diagnostic-only, shared 10–11 second observation window. No policy authority.

use crate::{Duration, Instant};
use std::cell::Cell;
use std::sync::{
    OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

struct Window {
    role: String,
    first_poll: Instant,
    started: AtomicBool,
    ended: AtomicBool,
    polls: AtomicU64,
    empty_polls: AtomicU64,
    byte_full: AtomicU64,
    packet_blocked: AtomicU64,
    send_marks: AtomicU64,
    ack_marks: AtomicU64,
    missing_context: AtomicU64,
}

static WINDOW: OnceLock<Option<Window>> = OnceLock::new();

/// Passive view of the already initialized server observation window.
/// This does not initialize it, emit boundaries, or change classifier counters.
#[doc(hidden)]
pub fn native_source_window_at(now: Instant) -> Option<Duration> {
    let window = WINDOW.get()?.as_ref()?;
    if window.role != "server" {
        return None;
    }
    let elapsed = now.checked_duration_since(window.first_poll)?;
    (elapsed >= Duration::from_secs(10) && elapsed < Duration::from_secs(11))
        .then_some(elapsed)
}

/// Exact native-driver turn and source pass enclosing a transmit poll.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct NativeSourceDriverContext {
    /// Stable runtime connection identity, also present on source observations.
    pub connection: usize,
    /// Unique observed driver turn.
    pub turn: u64,
    /// Source pass within that turn; zero is never emitted by the driver.
    pub pass: u64,
}

thread_local! {
    static SOURCE_DRIVER: Cell<Option<NativeSourceDriverContext>> = const { Cell::new(None) };
}

/// Restores the preceding diagnostic context without affecting native state.
#[doc(hidden)]
pub struct NativeSourceDriverGuard(Option<NativeSourceDriverContext>);

impl Drop for NativeSourceDriverGuard {
    fn drop(&mut self) {
        SOURCE_DRIVER.set(self.0);
    }
}

/// Associate only the enclosed real transmit poll with its runtime source pass.
#[doc(hidden)]
pub fn enter_native_source_driver(
    context: Option<NativeSourceDriverContext>,
) -> NativeSourceDriverGuard {
    NativeSourceDriverGuard(SOURCE_DRIVER.replace(context))
}

#[derive(Clone, Copy)]
struct Context {
    connection: usize,
    path_epoch: u64,
}

thread_local! {
    static CONTEXT: Cell<Option<Context>> = const { Cell::new(None) };
}

pub(super) struct ContextGuard(Option<Context>);

impl Drop for ContextGuard {
    fn drop(&mut self) {
        CONTEXT.set(self.0);
    }
}

fn unix_us() -> Option<u128> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_micros())
}

fn active(now: Instant) -> Option<(&'static Window, u128)> {
    let window = WINDOW.get()?.as_ref()?;
    let elapsed = now.checked_duration_since(window.first_poll)?;
    if elapsed < Duration::from_secs(10) {
        return None;
    }
    if elapsed >= Duration::from_secs(11) {
        if !window.ended.swap(true, Ordering::Relaxed) {
            eprintln!(
                "native_classifier_window_end role={} window=first_native_poll_10_11 elapsed_us={} unix_us={:?} started={} polls={} empty_polls={} byte_full={} packet_blocked={} send_marks={} ack_marks={} missing_context={}",
                window.role,
                elapsed.as_micros(),
                unix_us(),
                window.started.load(Ordering::Relaxed),
                window.polls.load(Ordering::Relaxed),
                window.empty_polls.load(Ordering::Relaxed),
                window.byte_full.load(Ordering::Relaxed),
                window.packet_blocked.load(Ordering::Relaxed),
                window.send_marks.load(Ordering::Relaxed),
                window.ack_marks.load(Ordering::Relaxed),
                window.missing_context.load(Ordering::Relaxed)
            );
        }
        return None;
    }
    if !window.started.swap(true, Ordering::Relaxed) {
        eprintln!(
            "native_classifier_window_start role={} window=first_native_poll_10_11 elapsed_us={} unix_us={:?}",
            window.role,
            elapsed.as_micros(),
            unix_us()
        );
    }
    Some((window, elapsed.as_micros()))
}

pub(super) fn enter_poll(now: Instant, connection: usize, path_epoch: u64) -> Option<ContextGuard> {
    WINDOW.get_or_init(|| {
        std::env::var("MPTUNNEL_NATIVE_STATE_TRACE_ROLE")
            .ok()
            .filter(|role| matches!(role.as_str(), "server" | "client"))
            .map(|role| Window {
                role,
                first_poll: now,
                started: AtomicBool::new(false),
                ended: AtomicBool::new(false),
                polls: AtomicU64::new(0),
                empty_polls: AtomicU64::new(0),
                byte_full: AtomicU64::new(0),
                packet_blocked: AtomicU64::new(0),
                send_marks: AtomicU64::new(0),
                ack_marks: AtomicU64::new(0),
                missing_context: AtomicU64::new(0),
            })
    });
    let (window, _) = active(now)?;
    window.polls.fetch_add(1, Ordering::Relaxed);
    Some(ContextGuard(CONTEXT.replace(Some(Context {
        connection,
        path_epoch,
    }))))
}

pub(super) fn enter_ack(now: Instant, connection: usize, path_epoch: u64) -> Option<ContextGuard> {
    active(now)?;
    Some(ContextGuard(CONTEXT.replace(Some(Context {
        connection,
        path_epoch,
    }))))
}

pub(super) struct EmptyPoll {
    pub flag_before: bool,
    pub flag_after: bool,
    pub flight: u64,
    pub cwnd: u64,
    pub mtu: u16,
    pub send_blocked: bool,
    pub cwnd_blocked: bool,
    pub had_sendable_frames: bool,
}

pub(super) fn empty_poll(
    now: Instant,
    poll: EmptyPoll,
    observe_pacing: impl FnOnce() -> super::pacing::PacerDelayObservation,
) {
    let Some((window, elapsed)) = active(now) else {
        return;
    };
    // All hypothetical pacing reads/work are inside the existing exact window.
    let pacing = observe_pacing();
    let context = CONTEXT.get();
    let source_driver = SOURCE_DRIVER.get();
    let byte_full = poll.flight >= poll.cwnd;
    let packet_blocked = poll
        .flight
        .checked_add(u64::from(poll.mtu))
        .map(|flight| flight > poll.cwnd);
    let event = window.empty_polls.fetch_add(1, Ordering::Relaxed) + 1;
    window
        .byte_full
        .fetch_add(u64::from(byte_full), Ordering::Relaxed);
    window
        .packet_blocked
        .fetch_add(u64::from(packet_blocked == Some(true)), Ordering::Relaxed);
    window
        .missing_context
        .fetch_add(u64::from(context.is_none()), Ordering::Relaxed);
    eprintln!(
        "native_empty_poll role={} window=first_native_poll_10_11 event={} elapsed_us={} unix_us={:?} connection={:?} path_epoch={:?} flag_before={} flag_after={} flight={} cwnd={} mtu={} byte_full={} packet_blocked={:?} send_blocked={} cwnd_blocked={} had_sendable_frames={} pacer_capacity_before={} pacer_tokens_before={} pacer_previous_age_ns={} pacer_now_before_previous={} pacer_cached_window={:?} pacer_cached_mtu={:?} pacer_rtt_ns={} pacer_metric_window={} pacer_rate_bytes_per_s={:?} pacer_hypothetical_bytes={} pacer_hypothetical_mtu={} pacer_capacity_after={} pacer_tokens_after={} pacer_previous_after_age_ns={} pacer_delay_some={} pacer_due_gap_ns={:?} driver_connection={:?} driver_turn={:?} source_pass={:?}",
        window.role,
        event,
        elapsed,
        unix_us(),
        context.map(|value| value.connection),
        context.map(|value| value.path_epoch),
        poll.flag_before,
        poll.flag_after,
        poll.flight,
        poll.cwnd,
        poll.mtu,
        byte_full,
        packet_blocked,
        poll.send_blocked,
        poll.cwnd_blocked,
        poll.had_sendable_frames,
        pacing.capacity_before,
        pacing.tokens_before,
        pacing.previous_age_ns,
        pacing.now_before_previous,
        pacing.cached_window,
        pacing.cached_mtu,
        pacing.rtt_ns,
        pacing.metric_window,
        pacing.metric_rate,
        pacing.bytes_to_send,
        pacing.mtu,
        pacing.capacity_after,
        pacing.tokens_after,
        pacing.previous_after_age_ns,
        pacing.due.is_some(),
        pacing.due_gap_ns,
        source_driver.map(|driver| driver.connection),
        source_driver.map(|driver| driver.turn),
        source_driver.map(|driver| driver.pass),
    );
}

pub(crate) struct MarkUpdate {
    pub controller: usize,
    pub callback: &'static str,
    pub old: u64,
    pub after_expiry: u64,
    pub new: u64,
    pub delivered: u64,
    pub flight: u64,
    pub cwnd: u64,
    pub flag: bool,
}

pub(crate) fn mark_update(now: Instant, mark: MarkUpdate) {
    if mark.old == mark.new && mark.old == mark.after_expiry {
        return;
    }
    let Some((window, elapsed)) = active(now) else {
        return;
    };
    let context = CONTEXT.get();
    let count = if mark.callback == "send" {
        &window.send_marks
    } else {
        &window.ack_marks
    };
    let event = count.fetch_add(1, Ordering::Relaxed) + 1;
    window
        .missing_context
        .fetch_add(u64::from(context.is_none()), Ordering::Relaxed);
    eprintln!(
        "native_mark_update role={} window=first_native_poll_10_11 callback={} event={} elapsed_us={} unix_us={:?} connection={:?} path_epoch={:?} controller={:#x} old={} after_expiry={} new={} delivered={} flight={} cwnd={} flag={}",
        window.role,
        mark.callback,
        event,
        elapsed,
        unix_us(),
        context.map(|value| value.connection),
        context.map(|value| value.path_epoch),
        mark.controller,
        mark.old,
        mark.after_expiry,
        mark.new,
        mark.delivered,
        mark.flight,
        mark.cwnd,
        mark.flag
    );
}
