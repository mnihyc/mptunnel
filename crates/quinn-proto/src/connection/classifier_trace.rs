//! Diagnostic-only, shared 10–11 second observation window. No policy authority.

use crate::{Duration, Instant};
use std::cell::Cell;
use std::fmt::{self, Write as _};
use std::io::Write as _;
use std::sync::{
    Mutex, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

struct Window {
    role: String,
    first_poll: Instant,
    capture: Option<Mutex<Capture>>,
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

// Measurement memory only. Overflow invalidates the capture; it never limits
// transport work or falls back to synchronous output during observation.
const CAPTURE_BYTES: usize = 64 * 1024 * 1024;

struct Capture {
    bytes: String,
    records: u64,
    dropped: u64,
    flushed: bool,
}

struct CappedRecord<'a>(&'a mut String);

impl fmt::Write for CappedRecord<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if value.len() > CAPTURE_BYTES.saturating_sub(self.0.len()) {
            return Err(fmt::Error);
        }
        self.0.push_str(value);
        Ok(())
    }
}

/// One ordered sink for source, classifier and controller diagnostics.
#[doc(hidden)]
pub fn emit_native_trace(fields: fmt::Arguments<'_>) {
    let Some(window) = WINDOW.get().and_then(Option::as_ref) else {
        eprintln!("{fields}");
        return;
    };
    let Some(capture) = &window.capture else {
        eprintln!("{fields}");
        return;
    };
    let mut capture = capture.lock().expect("native diagnostic capture");
    if capture.flushed || Instant::now().duration_since(window.first_poll) < Duration::from_secs(10)
    {
        eprintln!("{fields}");
        return;
    }
    if capture.dropped != 0 {
        capture.dropped += 1;
        return;
    }
    let start = capture.bytes.len();
    if writeln!(CappedRecord(&mut capture.bytes), "{fields}").is_err() {
        capture.bytes.truncate(start);
        capture.dropped += 1;
    } else {
        capture.records += 1;
    }
}

/// Called only at an existing driver entry, before Native/source locks or polls.
#[doc(hidden)]
pub fn flush_native_trace_if_due() {
    let Some(window) = WINDOW.get().and_then(Option::as_ref) else {
        return;
    };
    let Some(capture) = &window.capture else {
        return;
    };
    if Instant::now().duration_since(window.first_poll) < Duration::from_secs(11) {
        return;
    }
    // Serialize the flush with all later diagnostic output, including callbacks
    // whose supplied time is older. No Native lock is acquired by this sink.
    let mut capture = capture.lock().expect("native diagnostic flush");
    if capture.flushed {
        return;
    }
    let started = Instant::now();
    let bytes = capture.bytes.len();
    let mut stderr = std::io::stderr().lock();
    let header_ok = writeln!(stderr,
        "native_capture_flush_begin role={} cap_bytes={} bytes={} records={} dropped={} elapsed_ns={} unix_us={:?}",
        window.role, CAPTURE_BYTES, bytes, capture.records, capture.dropped,
        started.duration_since(window.first_poll).as_nanos(), unix_us(),
    ).is_ok();
    let body_ok = stderr.write_all(capture.bytes.as_bytes()).is_ok();
    let ended = Instant::now();
    let _ = writeln!(
        stderr,
        "native_capture_flush_end role={} cap_bytes={} bytes={} records={} dropped={} write_ok={} valid={} elapsed_ns={} write_elapsed_us={} unix_us={:?}",
        window.role,
        CAPTURE_BYTES,
        bytes,
        capture.records,
        capture.dropped,
        header_ok && body_ok,
        header_ok && body_ok && capture.dropped == 0,
        ended.duration_since(window.first_poll).as_nanos(),
        ended.duration_since(started).as_micros(),
        unix_us(),
    );
    capture.flushed = true;
    capture.bytes = String::new();
}

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
            emit_native_trace(format_args!(
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
            ));
        }
        return None;
    }
    if !window.started.swap(true, Ordering::Relaxed) {
        emit_native_trace(format_args!(
            "native_classifier_window_start role={} window=first_native_poll_10_11 elapsed_us={} unix_us={:?}",
            window.role,
            elapsed.as_micros(),
            unix_us()
        ));
    }
    Some((window, elapsed.as_micros()))
}

pub(super) fn enter_poll(now: Instant, connection: usize, path_epoch: u64) -> Option<ContextGuard> {
    WINDOW.get_or_init(|| {
        std::env::var("MPTUNNEL_NATIVE_STATE_TRACE_ROLE")
            .ok()
            .filter(|role| matches!(role.as_str(), "server" | "client"))
            .map(|role| Window {
                capture: (role == "server").then(|| {
                    Mutex::new(Capture {
                        bytes: String::with_capacity(CAPTURE_BYTES),
                        records: 0,
                        dropped: 0,
                        flushed: false,
                    })
                }),
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
    emit_native_trace(format_args!(
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
    ));
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
    emit_native_trace(format_args!(
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
    ));
}
