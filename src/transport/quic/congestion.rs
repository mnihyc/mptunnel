//! Native QUIC congestion and ACK instrumentation.

use crate::transport::{LossPolicyPercent, PathMetadata, QuicStartupTarget};
use std::any::Any;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy)]
pub struct CongestionMetrics {
    /// Equality identity `I` of the current network-path controller lineage.
    ///
    /// Same-identity migration clones and rollback preserve this value. It is
    /// therefore not an activation fence; internal scheduling that needs one
    /// must use the activation-stamped native controller shape.
    pub path_epoch: u64,
    /// Path-lineage diagnostic identity of the non-app-limited ACK clock.
    pub delivery_clock_epoch: u64,
    /// Activation-local window from the exact active controller clone.
    pub congestion_window: u64,
    /// Activation-local flight from the exact active Quinn `PathData`.
    pub bytes_in_flight: Option<u64>,
    /// Carrier-wide write backlog, independent of controller activation.
    pub pending_bytes: u64,
    /// Activation-local controller-owned sustainable bandwidth, in bits/s.
    ///
    /// It is neither a pacing rate nor a detached delivery sample with a
    /// wall-clock freshness deadline.
    pub bandwidth_estimate_bps: Option<u64>,
    /// Activation-local controller pacing rate, in bits/s.
    pub pacing_rate_bps: Option<u64>,
    /// Path-lineage native loss diagnostic; it is not activation authority.
    pub loss_ppm: Option<u32>,
    /// Path-lineage bytes declared lost by Quinn's native recovery controller.
    pub lost_bytes: u64,
    /// Path-lineage ECN diagnostic, when available.
    pub ecn_ppm: Option<u32>,
    /// Path-lineage ACK diagnostic since the preceding consuming read.
    pub newly_acked_bytes: Option<u64>,
    /// Path-lineage non-app-limited ACK diagnostic since the preceding read.
    pub non_app_limited_acked_bytes: Option<u64>,
    /// Path-lineage timed ACK bytes within `delivery_clock_epoch`.
    ///
    /// Unlike the preceding per-snapshot counters, this is a current-epoch
    /// total. Consumers must subtract a cursor with the same epoch identity.
    pub timed_non_app_limited_acked_bytes: Option<u64>,
    /// Path-lineage ACK/send-clock duration within `delivery_clock_epoch`.
    pub non_app_limited_ack_elapsed: Option<Duration>,
    /// Path-lineage native delivery samples since the preceding read.
    pub delivery_sample_count: u64,
    /// Path-lineage non-app-limited samples since the preceding read.
    pub non_app_limited_delivery_sample_count: u64,
    /// Path-lineage timed ACK samples within `delivery_clock_epoch`.
    pub timed_non_app_limited_delivery_sample_count: u64,
    /// Activation-local application-limited state from active Quinn `PathData`.
    pub app_limited: bool,
}

/// Equality-only identity of the native controller lineage that owns one
/// coherent operational-rate observation.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct NativeControllerIdentity(NonZeroU64);

impl NativeControllerIdentity {
    /// Opaque equality projection for binding this controller lineage into a
    /// carrier-rate authority stamp.
    ///
    /// The value has no capacity meaning and no ordering meaning across
    /// carrier owners. It is only a stable equality identity within the
    /// instrumented controller lineage that issued it.
    pub(crate) fn opaque_serial(self) -> u64 {
        self.0.get()
    }
}

/// Classification of one coherent native-controller operational observation.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum NativeControllerObservationKind {
    /// No valid operational rate is present in this read. This is never an
    /// instruction to clear a previously accepted rate or restore a prior.
    Absent,
    Valid,
}

/// Coherent active native-controller snapshot used by the authority adapter.
///
/// All fields come from one `quinn::Connection::congestion_state()` clone.
/// Diagnostic ACK cursors are not consumed by this snapshot.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct NativeControllerAuthoritySnapshot {
    activation: quinn::congestion::ControllerActivation,
    controller: NativeControllerIdentity,
    kind: NativeControllerObservationKind,
    operational_rate_bps: Option<NonZeroU64>,
}

impl NativeControllerAuthoritySnapshot {
    pub(crate) fn activation(self) -> quinn::congestion::ControllerActivation {
        self.activation
    }

    pub(crate) fn controller(self) -> NativeControllerIdentity {
        self.controller
    }

    pub(crate) fn kind(self) -> NativeControllerObservationKind {
        self.kind
    }

    pub(crate) fn operational_rate_bps(self) -> Option<NonZeroU64> {
        self.operational_rate_bps
    }
}

/// Activation-coherent native scheduling shape for one exact active QUIC path.
///
/// Every field in this value belongs to the same installed controller
/// activation `A` and controller lineage `I`. RTT, RTT variation, flight, and
/// application-limited state come from the `PathData` that owns that exact
/// controller clone; window and rates come from the clone itself. Shared ACK
/// and loss telemetry is deliberately absent: it is path-lineage diagnostic
/// evidence and may include callbacks from another activation after a
/// same-identity migration clone and rollback.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct NativeControllerShapeSnapshot {
    activation: quinn::congestion::ControllerActivation,
    controller: NativeControllerIdentity,
    smoothed_rtt: Duration,
    rtt_variance: Duration,
    congestion_window: u64,
    bytes_in_flight: u64,
    current_mtu: u16,
    operational_rate_bps: Option<NonZeroU64>,
    pacing_rate_bps: Option<NonZeroU64>,
    app_limited: bool,
}

impl NativeControllerShapeSnapshot {
    pub(crate) fn activation(self) -> quinn::congestion::ControllerActivation {
        self.activation
    }

    pub(crate) fn controller(self) -> NativeControllerIdentity {
        self.controller
    }

    pub(crate) fn smoothed_rtt(self) -> Duration {
        self.smoothed_rtt
    }

    pub(crate) fn rtt_variance(self) -> Duration {
        self.rtt_variance
    }

    pub(crate) fn congestion_window(self) -> u64 {
        self.congestion_window
    }

    pub(crate) fn bytes_in_flight(self) -> u64 {
        self.bytes_in_flight
    }

    pub(crate) fn current_mtu(self) -> u16 {
        self.current_mtu
    }

    pub(crate) fn operational_rate_bps(self) -> Option<NonZeroU64> {
        self.operational_rate_bps
    }

    pub(crate) fn pacing_rate_bps(self) -> Option<NonZeroU64> {
        self.pacing_rate_bps
    }

    pub(crate) fn app_limited(self) -> bool {
        self.app_limited
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct InstrumentedBbrConfig {
    loss_compensation: LossPolicyPercent,
    startup_target: Option<QuicStartupTarget>,
}

impl InstrumentedBbrConfig {
    pub(super) fn for_path(metadata: &PathMetadata) -> Self {
        Self {
            loss_compensation: metadata.loss_compensation.unwrap_or_default(),
            startup_target: metadata
                .quic_startup_target()
                .expect("QUIC path startup target must be validated during configuration"),
        }
    }

    fn bbr3_config(
        loss_compensation: LossPolicyPercent,
        startup_target: Option<QuicStartupTarget>,
    ) -> quinn::congestion::Bbr3Config {
        let mut config = quinn::congestion::Bbr3Config::default();
        config.loss_compensation_floor(f64::from(loss_compensation.ppm()) / 1_000_000.0);
        if let Some(target) = startup_target {
            config.initial_window_and_pacing_rate(
                target.window_bytes,
                target.pacing_bytes_per_second,
            );
        }
        config
    }

    fn build_bbr3(
        loss_compensation: LossPolicyPercent,
        startup_target: Option<QuicStartupTarget>,
        now: Instant,
        current_mtu: u16,
    ) -> Box<dyn quinn::congestion::Controller> {
        quinn::congestion::ControllerFactory::build(
            Arc::new(Self::bbr3_config(loss_compensation, startup_target)),
            now,
            current_mtu,
        )
    }
}

#[derive(Debug)]
pub(super) struct QuicCarrierTelemetry {
    next_path_epoch: AtomicU64,
    current_path_epoch: AtomicU64,
    application_ready: AtomicBool,
    controller_activation_fence: quinn::congestion::ControllerActivationFence,
    native_authority_changed: Arc<Notify>,
    write_activity_started: Arc<Notify>,
}

impl Default for QuicCarrierTelemetry {
    fn default() -> Self {
        Self {
            next_path_epoch: AtomicU64::new(0),
            current_path_epoch: AtomicU64::new(0),
            application_ready: AtomicBool::new(false),
            controller_activation_fence: quinn::congestion::ControllerActivationFence::new(),
            native_authority_changed: Arc::new(Notify::new()),
            write_activity_started: Arc::new(Notify::new()),
        }
    }
}

#[derive(Debug, Default)]
struct QuicPathTelemetry {
    path_epoch: u64,
    bytes_in_flight: AtomicU64,
    bytes_in_flight_authoritative: AtomicBool,
    // Quinn invokes congestion callbacks through one mutable controller. The
    // sequence makes its cumulative ACK counters coherent for a concurrent
    // metrics reader without putting a lock on the packet ACK hot path.
    ack_snapshot_sequence: AtomicU64,
    delivery_clock_epoch: AtomicU64,
    newly_acked_bytes: AtomicU64,
    non_app_limited_acked_bytes: AtomicU64,
    timed_non_app_limited_acked_bytes: AtomicU64,
    non_app_limited_ack_elapsed_nanos: AtomicU64,
    delivery_sample_count: AtomicU64,
    non_app_limited_delivery_sample_count: AtomicU64,
    timed_non_app_limited_delivery_sample_count: AtomicU64,
    ack_snapshot_cursor: Mutex<QuicAckTelemetryTotals>,
    sent_bytes: AtomicU64,
    lost_bytes: AtomicU64,
    app_limited: AtomicBool,
}

#[derive(Debug, Default, Clone, Copy)]
struct QuicAckTelemetryTotals {
    delivery_clock_epoch: u64,
    acked_bytes: u64,
    non_app_limited_acked_bytes: u64,
    timed_non_app_limited_acked_bytes: u64,
    non_app_limited_ack_elapsed_nanos: u64,
    sample_count: u64,
    non_app_limited_sample_count: u64,
    timed_non_app_limited_sample_count: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct QuicCarrierTelemetrySnapshot {
    pub(super) path_epoch: u64,
    pub(super) delivery_clock_epoch: u64,
    pub(super) bytes_in_flight: Option<u64>,
    pub(super) newly_acked_bytes: Option<u64>,
    pub(super) non_app_limited_acked_bytes: Option<u64>,
    pub(super) timed_non_app_limited_acked_bytes: Option<u64>,
    pub(super) non_app_limited_ack_elapsed: Option<Duration>,
    pub(super) delivery_sample_count: u64,
    pub(super) non_app_limited_delivery_sample_count: u64,
    pub(super) timed_non_app_limited_delivery_sample_count: u64,
    pub(super) loss_ppm: Option<u32>,
    pub(super) lost_bytes: u64,
    pub(super) app_limited: bool,
}

pub(super) struct InstrumentedController {
    inner: Box<dyn quinn::congestion::Controller>,
    loss_compensation: LossPolicyPercent,
    startup_target: Option<QuicStartupTarget>,
    pub(super) telemetry: Arc<QuicCarrierTelemetry>,
    path_telemetry: Arc<QuicPathTelemetry>,
    native_activation: Option<quinn::congestion::ControllerActivation>,
    startup_authority: StartupAuthorityState,
    last_bandwidth_sample_revision: Option<NonZeroU64>,
    last_valid_operational_rate_bps: Option<u64>,
    ack_batch_acked_bytes: u64,
    ack_batch_non_app_limited_acked_bytes: u64,
    ack_batch_sample_count: u64,
    ack_batch_non_app_limited_sample_count: u64,
    ack_batch_earliest_non_app_limited_sent: Option<Instant>,
    ack_batch_latest_non_app_limited_sent: Option<Instant>,
    last_non_app_limited_ack: Option<Instant>,
    non_app_limited_sent_high_watermark: Option<Instant>,
    delivery_clock_epoch: u64,
    next_non_app_limited_ack_starts_epoch: bool,
}

/// Finite configured priors remain MPP scheduling authority until the exact
/// native controller has produced post-authentication evidence from two
/// distinct packet-timed rounds. This state never changes Quinn's own model.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum StartupAuthorityState {
    /// Unknown and Unlimited configuration retain the prior adapter behavior.
    Bypass,
    /// The controller exists, but the authenticated MPP carrier is not ready.
    PreReady,
    /// Readiness is established; the first subsequent Data packet defines F.
    AwaitFloor,
    /// F is fixed; no eligible native sample has been observed yet.
    AwaitFirst { floor_packet_number: u64 },
    /// One eligible sample exists, but only within one send-time BBR round.
    Armed {
        floor_packet_number: u64,
        first_source_round: u64,
    },
    /// Native operational authority is permanently qualified for this lineage.
    Operational,
}

impl StartupAuthorityState {
    fn projects_native(self) -> bool {
        matches!(self, Self::Bypass | Self::Operational)
    }
}

impl std::fmt::Debug for InstrumentedController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedController")
            .field("telemetry", &self.telemetry)
            .finish_non_exhaustive()
    }
}

impl QuicCarrierTelemetry {
    pub(super) fn current_path_epoch(&self) -> u64 {
        self.current_path_epoch.load(Ordering::Acquire)
    }

    pub(super) fn record_write_activity(&self) {
        // This is only a prompt for an idle metrics task to observe Quinn's
        // native state. It carries no byte count and cannot prove Product
        // delivery; exact Product completion belongs to MPP DataACK.
        self.write_activity_started.notify_waiters();
    }

    pub(super) fn write_activity_notify(&self) -> Arc<Notify> {
        self.write_activity_started.clone()
    }

    pub(super) fn controller_activation_fence(
        &self,
    ) -> quinn::congestion::ControllerActivationFence {
        self.controller_activation_fence.clone()
    }

    pub(super) fn native_authority_notify(&self) -> Arc<Notify> {
        self.native_authority_changed.clone()
    }

    fn publish_native_authority_change(&self) {
        // notify_one stores at most one permit when no task is waiting, making
        // this wake durable and naturally coalescing to the fence's current A.
        self.native_authority_changed.notify_one();
    }

    fn allocate_path_telemetry(&self) -> Arc<QuicPathTelemetry> {
        let previous = self
            .next_path_epoch
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |epoch| {
                epoch.checked_add(1)
            })
            .expect("QUIC path epoch exhausted");
        let path_epoch = previous + 1;
        Arc::new(QuicPathTelemetry {
            path_epoch,
            ..QuicPathTelemetry::default()
        })
    }
}

impl QuicPathTelemetry {
    pub(super) fn bytes_in_flight(&self) -> Option<u64> {
        self.bytes_in_flight_authoritative
            .load(Ordering::Acquire)
            .then(|| self.bytes_in_flight.load(Ordering::Relaxed))
            .filter(|_| {
                fence(Ordering::Acquire);
                self.bytes_in_flight_authoritative.load(Ordering::Relaxed)
            })
    }

    fn ack_totals(&self) -> QuicAckTelemetryTotals {
        loop {
            let before = self.ack_snapshot_sequence.load(Ordering::Acquire);
            if before & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let totals = QuicAckTelemetryTotals {
                delivery_clock_epoch: self.delivery_clock_epoch.load(Ordering::Relaxed),
                acked_bytes: self.newly_acked_bytes.load(Ordering::Relaxed),
                non_app_limited_acked_bytes: self
                    .non_app_limited_acked_bytes
                    .load(Ordering::Relaxed),
                timed_non_app_limited_acked_bytes: self
                    .timed_non_app_limited_acked_bytes
                    .load(Ordering::Relaxed),
                non_app_limited_ack_elapsed_nanos: self
                    .non_app_limited_ack_elapsed_nanos
                    .load(Ordering::Relaxed),
                sample_count: self.delivery_sample_count.load(Ordering::Relaxed),
                non_app_limited_sample_count: self
                    .non_app_limited_delivery_sample_count
                    .load(Ordering::Relaxed),
                timed_non_app_limited_sample_count: self
                    .timed_non_app_limited_delivery_sample_count
                    .load(Ordering::Relaxed),
            };
            fence(Ordering::Acquire);
            let after = self.ack_snapshot_sequence.load(Ordering::Relaxed);
            if before == after {
                return totals;
            }
        }
    }

    pub(super) fn snapshot(&self) -> QuicCarrierTelemetrySnapshot {
        let mut cursor = self
            .ack_snapshot_cursor
            .lock()
            .expect("QUIC ACK telemetry snapshot lock");
        let totals = self.ack_totals();
        let newly_acked_bytes = totals.acked_bytes.wrapping_sub(cursor.acked_bytes);
        let non_app_limited_acked_bytes = totals
            .non_app_limited_acked_bytes
            .wrapping_sub(cursor.non_app_limited_acked_bytes);
        // Timed fields are cumulative only within `delivery_clock_epoch`.
        // Returning the coherent current-epoch totals prevents a metrics poll
        // that spans an app-limited reset from joining two delivery clocks.
        let timed_non_app_limited_acked_bytes = totals.timed_non_app_limited_acked_bytes;
        let non_app_limited_ack_elapsed_nanos = totals.non_app_limited_ack_elapsed_nanos;
        let delivery_sample_count = totals.sample_count.wrapping_sub(cursor.sample_count);
        let non_app_limited_delivery_sample_count = totals
            .non_app_limited_sample_count
            .wrapping_sub(cursor.non_app_limited_sample_count);
        let timed_non_app_limited_delivery_sample_count = totals.timed_non_app_limited_sample_count;
        *cursor = totals;
        drop(cursor);
        let sent_bytes = self.sent_bytes.load(Ordering::Relaxed);
        let lost_bytes = self.lost_bytes.load(Ordering::Relaxed);
        let loss_ppm = (sent_bytes > 0).then(|| {
            let ratio = (lost_bytes as f64 / sent_bytes as f64).clamp(0.0, 1.0);
            (ratio * 1_000_000.0).round() as u32
        });
        let bytes_in_flight = self.bytes_in_flight();
        QuicCarrierTelemetrySnapshot {
            path_epoch: self.path_epoch,
            delivery_clock_epoch: totals.delivery_clock_epoch,
            bytes_in_flight,
            newly_acked_bytes: (newly_acked_bytes > 0).then_some(newly_acked_bytes),
            non_app_limited_acked_bytes: (non_app_limited_acked_bytes > 0)
                .then_some(non_app_limited_acked_bytes),
            timed_non_app_limited_acked_bytes: (timed_non_app_limited_acked_bytes > 0)
                .then_some(timed_non_app_limited_acked_bytes),
            non_app_limited_ack_elapsed: (non_app_limited_ack_elapsed_nanos > 0)
                .then(|| Duration::from_nanos(non_app_limited_ack_elapsed_nanos)),
            delivery_sample_count,
            non_app_limited_delivery_sample_count,
            timed_non_app_limited_delivery_sample_count,
            loss_ppm,
            lost_bytes,
            app_limited: self.app_limited.load(Ordering::Relaxed),
        }
    }
    fn add_sent(&self, bytes: u64) {
        // Quinn invokes on_sent for ACK-only datagrams too. Until on_end_acks
        // supplies its exact path flight, an additive estimate is not sound.
        // Invalidate that snapshot before the send watermark's release so a
        // reader cannot pair a new send with the preceding authoritative BIF.
        self.bytes_in_flight_authoritative
            .store(false, Ordering::Release);
        self.sent_bytes.fetch_add(bytes, Ordering::AcqRel);
    }

    fn publish_ack_batch(&self, totals: QuicAckTelemetryTotals, in_flight: u64, app_limited: bool) {
        if totals.sample_count > 0 {
            self.ack_snapshot_sequence.fetch_add(1, Ordering::AcqRel);
            let current_delivery_clock_epoch = self.delivery_clock_epoch.load(Ordering::Relaxed);
            if totals.delivery_clock_epoch != current_delivery_clock_epoch {
                debug_assert!(
                    totals.delivery_clock_epoch > current_delivery_clock_epoch,
                    "QUIC delivery-clock epochs must advance monotonically"
                );
                self.delivery_clock_epoch
                    .store(totals.delivery_clock_epoch, Ordering::Relaxed);
                self.timed_non_app_limited_acked_bytes
                    .store(0, Ordering::Relaxed);
                self.non_app_limited_ack_elapsed_nanos
                    .store(0, Ordering::Relaxed);
                self.timed_non_app_limited_delivery_sample_count
                    .store(0, Ordering::Relaxed);
            }
            self.newly_acked_bytes
                .fetch_add(totals.acked_bytes, Ordering::Relaxed);
            if totals.non_app_limited_sample_count > 0 {
                self.non_app_limited_acked_bytes
                    .fetch_add(totals.non_app_limited_acked_bytes, Ordering::Relaxed);
                self.non_app_limited_ack_elapsed_nanos
                    .fetch_add(totals.non_app_limited_ack_elapsed_nanos, Ordering::Relaxed);
                self.non_app_limited_delivery_sample_count
                    .fetch_add(totals.non_app_limited_sample_count, Ordering::Relaxed);
                if totals.timed_non_app_limited_sample_count > 0 {
                    self.timed_non_app_limited_acked_bytes
                        .fetch_add(totals.timed_non_app_limited_acked_bytes, Ordering::Relaxed);
                    self.timed_non_app_limited_delivery_sample_count
                        .fetch_add(totals.timed_non_app_limited_sample_count, Ordering::Relaxed);
                }
            }
            self.delivery_sample_count
                .fetch_add(totals.sample_count, Ordering::Relaxed);
            self.ack_snapshot_sequence.fetch_add(1, Ordering::Release);
        }
        self.bytes_in_flight.store(in_flight, Ordering::Relaxed);
        self.bytes_in_flight_authoritative
            .store(true, Ordering::Release);
        self.app_limited.store(app_limited, Ordering::Relaxed);
    }

    fn add_lost(&self, lost_bytes: u64) {
        if lost_bytes > 0 {
            self.lost_bytes.fetch_add(lost_bytes, Ordering::Relaxed);
            let _ = self.bytes_in_flight.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| Some(current.saturating_sub(lost_bytes)),
            );
        }
    }
}
impl InstrumentedController {
    #[cfg(test)]
    pub(super) fn new(
        inner: Box<dyn quinn::congestion::Controller>,
        telemetry: Arc<QuicCarrierTelemetry>,
    ) -> Self {
        let path_telemetry = telemetry.allocate_path_telemetry();
        Self::for_path(
            inner,
            LossPolicyPercent::default(),
            None,
            telemetry,
            path_telemetry,
        )
    }

    fn for_path(
        inner: Box<dyn quinn::congestion::Controller>,
        loss_compensation: LossPolicyPercent,
        startup_target: Option<QuicStartupTarget>,
        telemetry: Arc<QuicCarrierTelemetry>,
        path_telemetry: Arc<QuicPathTelemetry>,
    ) -> Self {
        let startup_authority = match startup_target {
            None => StartupAuthorityState::Bypass,
            Some(_) if telemetry.application_ready.load(Ordering::Acquire) => {
                StartupAuthorityState::AwaitFloor
            }
            Some(_) => StartupAuthorityState::PreReady,
        };
        let last_bandwidth_sample_revision = inner
            .latest_bandwidth_sample()
            .map(|sample| sample.revision);
        let last_valid_operational_rate_bps = startup_authority
            .projects_native()
            .then(|| {
                checked_positive_bytes_per_second_to_bits_per_second(
                    inner.metrics().bandwidth_estimate,
                )
            })
            .flatten();
        Self {
            inner,
            loss_compensation,
            startup_target,
            telemetry,
            path_telemetry,
            native_activation: None,
            startup_authority,
            last_bandwidth_sample_revision,
            last_valid_operational_rate_bps,
            ack_batch_acked_bytes: 0,
            ack_batch_non_app_limited_acked_bytes: 0,
            ack_batch_sample_count: 0,
            ack_batch_non_app_limited_sample_count: 0,
            ack_batch_earliest_non_app_limited_sent: None,
            ack_batch_latest_non_app_limited_sent: None,
            last_non_app_limited_ack: None,
            non_app_limited_sent_high_watermark: None,
            delivery_clock_epoch: 0,
            next_non_app_limited_ack_starts_epoch: true,
        }
    }

    pub(super) fn snapshot(&self) -> QuicCarrierTelemetrySnapshot {
        self.path_telemetry.snapshot()
    }

    pub(super) fn native_authority_snapshot(&self) -> Option<NativeControllerAuthoritySnapshot> {
        let activation = self.native_activation?;
        let controller = self.native_controller_identity();
        let operational_rate_bps = self
            .startup_authority
            .projects_native()
            .then(|| {
                checked_positive_bytes_per_second_to_bits_per_second(
                    self.inner.metrics().bandwidth_estimate,
                )
                .and_then(NonZeroU64::new)
            })
            .flatten();
        let kind = match operational_rate_bps {
            Some(_) => NativeControllerObservationKind::Valid,
            None => NativeControllerObservationKind::Absent,
        };
        Some(NativeControllerAuthoritySnapshot {
            activation,
            controller,
            kind,
            operational_rate_bps,
        })
    }

    fn native_controller_identity(&self) -> NativeControllerIdentity {
        NativeControllerIdentity(
            NonZeroU64::new(self.path_telemetry.path_epoch)
                .expect("instrumented controller identities are nonzero"),
        )
    }

    /// Bind exact active-`PathData` fields to this exact controller clone.
    ///
    /// The caller must obtain both through one Quinn active-path snapshot. No
    /// value is read from shared path telemetry here.
    pub(super) fn native_shape_snapshot(
        &self,
        smoothed_rtt: Duration,
        rtt_variance: Duration,
        bytes_in_flight: u64,
        current_mtu: u16,
        app_limited: bool,
    ) -> Option<NativeControllerShapeSnapshot> {
        let activation = self.native_activation?;
        let metrics = self.inner.metrics();
        Some(NativeControllerShapeSnapshot {
            activation,
            controller: self.native_controller_identity(),
            smoothed_rtt,
            rtt_variance,
            congestion_window: metrics.congestion_window,
            bytes_in_flight,
            current_mtu,
            operational_rate_bps: self
                .startup_authority
                .projects_native()
                .then(|| {
                    checked_positive_bytes_per_second_to_bits_per_second(metrics.bandwidth_estimate)
                        .and_then(NonZeroU64::new)
                })
                .flatten(),
            pacing_rate_bps: checked_positive_bytes_per_second_to_bits_per_second(
                metrics.pacing_rate,
            )
            .and_then(NonZeroU64::new),
            app_limited,
        })
    }

    fn detect_native_operational_change(&mut self) {
        if !self.startup_authority.projects_native() {
            return;
        }
        let Some(rate) = checked_positive_bytes_per_second_to_bits_per_second(
            self.inner.metrics().bandwidth_estimate,
        ) else {
            // Missing, zero, or unrepresentable output is no observation. It
            // never clears the last valid detector state or impersonates a
            // structural invalidation.
            return;
        };
        if self.last_valid_operational_rate_bps != Some(rate) {
            self.last_valid_operational_rate_bps = Some(rate);
            self.telemetry.publish_native_authority_change();
        }
    }

    /// Consume the exact completed BBR sample at most once and advance the
    /// finite-prior handoff without modifying the native controller.
    ///
    /// Two distinct send-time rounds exclude Quinn's one-poll lag in its
    /// application-limited stamp. Ineligible observations are absence, not a
    /// reset of an already armed round.
    fn advance_startup_authority(&mut self) -> Option<u64> {
        if matches!(
            self.startup_authority,
            StartupAuthorityState::Bypass
                | StartupAuthorityState::PreReady
                | StartupAuthorityState::AwaitFloor
                | StartupAuthorityState::Operational
        ) {
            // Still consume a newly completed revision before readiness so a
            // delayed pre-ready sample cannot be reinterpreted after the hook.
            if let Some(sample) = self.inner.latest_bandwidth_sample()
                && self.last_bandwidth_sample_revision != Some(sample.revision)
            {
                self.last_bandwidth_sample_revision = Some(sample.revision);
            }
            return None;
        }

        let sample = self.inner.latest_bandwidth_sample()?;
        if self.last_bandwidth_sample_revision == Some(sample.revision) {
            return None;
        }
        self.last_bandwidth_sample_revision = Some(sample.revision);

        let floor_packet_number = match self.startup_authority {
            StartupAuthorityState::AwaitFirst {
                floor_packet_number,
            }
            | StartupAuthorityState::Armed {
                floor_packet_number,
                ..
            } => floor_packet_number,
            _ => unreachable!("startup authority state filtered above"),
        };
        let operational_rate_bps = checked_positive_bytes_per_second_to_bits_per_second(
            self.inner.metrics().bandwidth_estimate,
        );
        let eligible = sample.valid
            && sample.source_space == quinn::congestion::SpaceId::Data
            && sample.source_packet_number >= floor_packet_number
            && !sample.app_limited
            && operational_rate_bps.is_some();
        if !eligible {
            return None;
        }

        match self.startup_authority {
            StartupAuthorityState::AwaitFirst {
                floor_packet_number,
            } => {
                self.startup_authority = StartupAuthorityState::Armed {
                    floor_packet_number,
                    first_source_round: sample.source_round,
                };
                None
            }
            StartupAuthorityState::Armed {
                first_source_round, ..
            } if sample.source_round > first_source_round => {
                self.startup_authority = StartupAuthorityState::Operational;
                operational_rate_bps
            }
            StartupAuthorityState::Armed { .. } => None,
            _ => unreachable!("startup authority state filtered above"),
        }
    }

    #[cfg(test)]
    pub(super) fn configured_loss_compensation(&self) -> LossPolicyPercent {
        self.loss_compensation
    }

    fn accumulate_ack_telemetry(&mut self, sent: Instant, bytes: u64, app_limited: bool) {
        if bytes == 0 {
            return;
        }
        self.ack_batch_acked_bytes = self.ack_batch_acked_bytes.saturating_add(bytes);
        self.ack_batch_sample_count = self.ack_batch_sample_count.saturating_add(1);
        if app_limited {
            return;
        }
        self.ack_batch_non_app_limited_acked_bytes = self
            .ack_batch_non_app_limited_acked_bytes
            .saturating_add(bytes);
        self.ack_batch_non_app_limited_sample_count = self
            .ack_batch_non_app_limited_sample_count
            .saturating_add(1);
        self.ack_batch_earliest_non_app_limited_sent = Some(
            self.ack_batch_earliest_non_app_limited_sent
                .map_or(sent, |earliest| earliest.min(sent)),
        );
        self.ack_batch_latest_non_app_limited_sent = Some(
            self.ack_batch_latest_non_app_limited_sent
                .map_or(sent, |latest| latest.max(sent)),
        );
    }

    pub(super) fn finish_ack_telemetry(&mut self, now: Instant, in_flight: u64, app_limited: bool) {
        // Delivery rate uses the slower of the ACK and send clocks, expressed
        // as their maximum elapsed time. This excludes propagation RTT from a
        // new epoch while preventing compressed ACKs from inflating rate.
        let batch_sent_range = self
            .ack_batch_earliest_non_app_limited_sent
            .zip(self.ack_batch_latest_non_app_limited_sent);
        if batch_sent_range.is_some() && self.next_non_app_limited_ack_starts_epoch {
            self.delivery_clock_epoch = self
                .delivery_clock_epoch
                .checked_add(1)
                .expect("QUIC delivery-clock epoch exhausted");
            self.next_non_app_limited_ack_starts_epoch = false;
        }
        let non_app_limited_ack_elapsed =
            batch_sent_range.and_then(|(earliest_sent, latest_sent)| {
                let within_batch_send_elapsed =
                    latest_sent.saturating_duration_since(earliest_sent);
                let elapsed = match (
                    self.last_non_app_limited_ack,
                    self.non_app_limited_sent_high_watermark,
                ) {
                    (Some(previous_ack), Some(sent_high_watermark)) => {
                        let ack_elapsed = now.saturating_duration_since(previous_ack);
                        let forward_send_elapsed =
                            latest_sent.saturating_duration_since(sent_high_watermark);
                        ack_elapsed
                            .max(forward_send_elapsed)
                            .max(within_batch_send_elapsed)
                    }
                    _ => within_batch_send_elapsed,
                };
                (!elapsed.is_zero()).then_some(elapsed)
            });
        let totals = QuicAckTelemetryTotals {
            delivery_clock_epoch: self.delivery_clock_epoch,
            acked_bytes: self.ack_batch_acked_bytes,
            non_app_limited_acked_bytes: self.ack_batch_non_app_limited_acked_bytes,
            timed_non_app_limited_acked_bytes: non_app_limited_ack_elapsed
                .map_or(0, |_| self.ack_batch_non_app_limited_acked_bytes),
            non_app_limited_ack_elapsed_nanos: non_app_limited_ack_elapsed
                .map_or(0, duration_as_u64_nanos),
            sample_count: self.ack_batch_sample_count,
            non_app_limited_sample_count: self.ack_batch_non_app_limited_sample_count,
            timed_non_app_limited_sample_count: non_app_limited_ack_elapsed
                .map_or(0, |_| self.ack_batch_non_app_limited_sample_count),
        };

        self.ack_batch_acked_bytes = 0;
        self.ack_batch_non_app_limited_acked_bytes = 0;
        self.ack_batch_sample_count = 0;
        self.ack_batch_non_app_limited_sample_count = 0;
        self.ack_batch_earliest_non_app_limited_sent = None;
        self.ack_batch_latest_non_app_limited_sent = None;

        if app_limited {
            self.last_non_app_limited_ack = None;
            self.non_app_limited_sent_high_watermark = None;
            self.next_non_app_limited_ack_starts_epoch = true;
        } else if let Some((_, latest_sent)) = batch_sent_range {
            self.last_non_app_limited_ack = Some(now);
            self.non_app_limited_sent_high_watermark = Some(
                self.non_app_limited_sent_high_watermark
                    .map_or(latest_sent, |high_watermark| {
                        high_watermark.max(latest_sent)
                    }),
            );
        }
        self.path_telemetry
            .publish_ack_batch(totals, in_flight, app_limited);
    }
}

fn duration_as_u64_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl quinn::congestion::ControllerFactory for InstrumentedBbrConfig {
    fn build(
        self: Arc<Self>,
        now: Instant,
        current_mtu: u16,
    ) -> Box<dyn quinn::congestion::Controller> {
        // Use Quinn BBRv3 for the QUIC carrier. An explicit finite path-rate
        // contract changes only its initial window and pacer; omission keeps
        // Quinn's exact startup geometry, and native delivery observations
        // remain the sole operational bandwidth authority. QUIC still owns
        // packet pacing, loss recovery, and bytes in flight.
        let inner = Self::build_bbr3(
            self.loss_compensation,
            self.startup_target,
            now,
            current_mtu,
        );
        let telemetry = Arc::new(QuicCarrierTelemetry::default());
        let path_telemetry = telemetry.allocate_path_telemetry();
        Box::new(InstrumentedController::for_path(
            inner,
            self.loss_compensation,
            self.startup_target,
            telemetry,
            path_telemetry,
        ))
    }
}

impl quinn::congestion::Controller for InstrumentedController {
    fn activation_fence(&self) -> Option<quinn::congestion::ControllerActivationFence> {
        Some(self.telemetry.controller_activation_fence())
    }

    fn on_activated(&mut self, activation: quinn::congestion::ControllerActivation) {
        self.native_activation = Some(activation);
        // A retained clone may have become inactive before the one active
        // clone received application readiness. Reconcile only PreReady here;
        // clones that already own a floor, armed round, or operational latch
        // retain that exact lineage state across migration and rollback.
        if self.startup_authority == StartupAuthorityState::PreReady
            && self.telemetry.application_ready.load(Ordering::Acquire)
        {
            self.startup_authority = StartupAuthorityState::AwaitFloor;
        }
        self.telemetry
            .current_path_epoch
            .store(self.path_telemetry.path_epoch, Ordering::Release);
    }

    fn on_activation_published(&self) {
        self.telemetry.publish_native_authority_change();
    }

    fn on_activation_terminal(&self) {
        self.telemetry.publish_native_authority_change();
    }

    fn on_application_ready(&mut self) {
        self.inner.on_application_ready();
        if self.startup_authority == StartupAuthorityState::Bypass {
            return;
        }
        self.telemetry
            .application_ready
            .store(true, Ordering::Release);
        if self.startup_authority == StartupAuthorityState::PreReady {
            self.startup_authority = StartupAuthorityState::AwaitFloor;
        }
    }

    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        self.path_telemetry.add_sent(bytes);
        self.inner.on_sent(now, bytes, last_packet_number);
    }

    fn on_packet_sent(
        &mut self,
        now: Instant,
        bytes: u16,
        prior_in_flight: u64,
        packet_number: u64,
        space: quinn::congestion::SpaceId,
        app_limited: bool,
    ) -> Option<quinn::congestion::PacketDeliveryState> {
        if space == quinn::congestion::SpaceId::Data
            && self.startup_authority == StartupAuthorityState::AwaitFloor
        {
            self.startup_authority = StartupAuthorityState::AwaitFirst {
                floor_packet_number: packet_number,
            };
        }
        self.inner.on_packet_sent(
            now,
            bytes,
            prior_in_flight,
            packet_number,
            space,
            app_limited,
        )
    }

    fn on_cwnd_limited(&mut self) {
        self.inner.on_cwnd_limited();
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        packet_number: u64,
        space: quinn::congestion::SpaceId,
        app_limited: bool,
        rtt: &quinn_proto::RttEstimator,
    ) {
        self.accumulate_ack_telemetry(sent, bytes, app_limited);
        self.inner
            .on_ack(now, sent, bytes, packet_number, space, app_limited, rtt);
    }

    fn on_ack_with_packet_state(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        packet_number: u64,
        space: quinn::congestion::SpaceId,
        packet_state: Option<quinn::congestion::PacketDeliveryState>,
        rtt: &quinn_proto::RttEstimator,
    ) {
        self.accumulate_ack_telemetry(sent, bytes, app_limited);
        self.inner.on_ack_with_packet_state(
            now,
            sent,
            bytes,
            app_limited,
            packet_number,
            space,
            packet_state,
            rtt,
        );
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
        space: quinn::congestion::SpaceId,
    ) {
        self.finish_ack_telemetry(now, in_flight, app_limited);
        self.inner
            .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked, space);
        if let Some(rate) = self.advance_startup_authority() {
            // P -> O is a structural authority change even when BBR's hidden
            // numeric value equals one seen before readiness. Publish exactly
            // once, then let ordinary changed-value detection resume in O.
            self.last_valid_operational_rate_bps = Some(rate);
            self.telemetry.publish_native_authority_change();
        } else {
            self.detect_native_operational_change();
        }
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        is_ecn: bool,
        lost_bytes: u64,
        largest_lost: u64,
        space: quinn::congestion::SpaceId,
    ) {
        self.path_telemetry.add_lost(lost_bytes);
        self.inner.on_congestion_event(
            now,
            sent,
            is_persistent_congestion,
            is_ecn,
            lost_bytes,
            largest_lost,
            space,
        );
        self.detect_native_operational_change();
    }

    fn on_packet_lost(
        &mut self,
        lost_bytes: u16,
        packet_number: u64,
        space: quinn::congestion::SpaceId,
        now: Instant,
    ) -> Option<quinn::congestion::RecoveryTransactionId> {
        // Aggregate loss telemetry is recorded once in on_congestion_event.
        self.inner
            .on_packet_lost(lost_bytes, packet_number, space, now)
    }

    fn on_spurious_congestion_event(
        &mut self,
        transaction: quinn::congestion::RecoveryTransactionId,
    ) -> bool {
        let restored = self.inner.on_spurious_congestion_event(transaction);
        self.detect_native_operational_change();
        restored
    }

    fn on_recovery_transaction_abandoned(
        &mut self,
        transaction: quinn::congestion::RecoveryTransactionId,
    ) {
        self.inner.on_recovery_transaction_abandoned(transaction);
    }

    fn on_validated_ecn_congestion_event(&mut self) {
        self.inner.on_validated_ecn_congestion_event();
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.inner.on_mtu_update(new_mtu);
    }

    fn on_ack_frequency_update(
        &mut self,
        ack_eliciting_threshold: u64,
        requested_max_ack_delay: Duration,
    ) {
        self.inner
            .on_ack_frequency_update(ack_eliciting_threshold, requested_max_ack_delay);
    }

    fn window(&self) -> u64 {
        self.inner.window()
    }

    fn metrics(&self) -> quinn::congestion::ControllerMetrics {
        self.inner.metrics()
    }

    fn latest_bandwidth_sample(&self) -> Option<quinn::congestion::BandwidthSample> {
        self.inner.latest_bandwidth_sample()
    }

    fn pacing_rate(&self) -> Option<u64> {
        self.inner.pacing_rate()
    }

    fn clone_box(&self) -> Box<dyn quinn::congestion::Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            loss_compensation: self.loss_compensation,
            startup_target: self.startup_target,
            telemetry: self.telemetry.clone(),
            path_telemetry: self.path_telemetry.clone(),
            native_activation: self.native_activation,
            startup_authority: self.startup_authority,
            last_bandwidth_sample_revision: self.last_bandwidth_sample_revision,
            last_valid_operational_rate_bps: self.last_valid_operational_rate_bps,
            ack_batch_acked_bytes: self.ack_batch_acked_bytes,
            ack_batch_non_app_limited_acked_bytes: self.ack_batch_non_app_limited_acked_bytes,
            ack_batch_sample_count: self.ack_batch_sample_count,
            ack_batch_non_app_limited_sample_count: self.ack_batch_non_app_limited_sample_count,
            ack_batch_earliest_non_app_limited_sent: self.ack_batch_earliest_non_app_limited_sent,
            ack_batch_latest_non_app_limited_sent: self.ack_batch_latest_non_app_limited_sent,
            last_non_app_limited_ack: self.last_non_app_limited_ack,
            non_app_limited_sent_high_watermark: self.non_app_limited_sent_high_watermark,
            delivery_clock_epoch: self.delivery_clock_epoch,
            next_non_app_limited_ack_starts_epoch: self.next_non_app_limited_ack_starts_epoch,
        })
    }

    fn fresh_path_box(
        &self,
        now: Instant,
        current_mtu: u16,
    ) -> Option<Box<dyn quinn::congestion::Controller>> {
        // InstrumentedBbrConfig is the sole production constructor for this
        // wrapper, so a fresh path always starts a fresh BBRv3 model. Only the
        // connection-scoped evidence owner survives the network transition.
        let inner = InstrumentedBbrConfig::build_bbr3(
            self.loss_compensation,
            self.startup_target,
            now,
            current_mtu,
        );
        let path_telemetry = self.telemetry.allocate_path_telemetry();
        Some(Box::new(Self::for_path(
            inner,
            self.loss_compensation,
            self.startup_target,
            self.telemetry.clone(),
            path_telemetry,
        )))
    }

    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

pub(super) fn checked_positive_bytes_per_second_to_bits_per_second(
    bytes_per_second: Option<u64>,
) -> Option<u64> {
    bytes_per_second
        .filter(|rate| *rate > 0)
        .and_then(|rate| rate.checked_mul(8))
}

#[cfg(test)]
#[path = "tests_congestion.rs"]
mod tests;
