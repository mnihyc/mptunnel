//! Quiescent QUIC measurement epochs and their typed observations.

use super::{QuicCarrierError, QuicCarrierTelemetry};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub struct MeasurementSpec {
    pub token: u64,
    pub train_payload_bytes: u64,
    pub sample_floor_bytes: u64,
    pub warmup_carrier_bytes: u64,
    pub required_timed_carrier_bytes: u64,
    pub expires_at: Instant,
    pub retention: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeasurementPhase {
    Writing,
    Measuring,
    AwaitingReceipt,
    Complete,
    Expired,
    Aborted,
}

#[derive(Debug, Clone, Copy)]
pub struct MeasurementMetrics {
    pub token: u64,
    pub train_payload_bytes: u64,
    pub sample_floor_bytes: u64,
    pub warmup_carrier_bytes: u64,
    pub required_timed_carrier_bytes: u64,
    pub expires_at: Instant,
    pub phase: MeasurementPhase,
    pub started_clean: bool,
    pub write_committed: bool,
    pub written_payload_bytes: u64,
    pub written_data_frame_count: u64,
    pub total_acked_carrier_bytes: u64,
    pub total_ack_sample_count: u64,
    pub warmup_acked_carrier_bytes: u64,
    pub warmup_ack_sample_count: u64,
    pub measurement_acked_carrier_bytes: u64,
    pub measurement_ack_sample_count: u64,
    pub timed_measurement_acked_carrier_bytes: u64,
    pub timed_measurement_ack_sample_count: u64,
    pub app_limited_acked_carrier_bytes: u64,
    pub app_limited_ack_sample_count: u64,
    pub timed_measurement_ack_elapsed: Option<Duration>,
    pub native_threshold_at: Option<Instant>,
    pub confirmed_at: Option<Instant>,
    pub retention: Duration,
    pub receipt_received_payload_bytes: u64,
    pub receipt_elapsed: Option<Duration>,
    pub receipt_rtt: Option<Duration>,
    pub receipt_at: Option<Instant>,
    // These snapshots explain native cleanup ordering around the receipt. They
    // are diagnostic because ACK-only sends have no later ACK callback.
    pub last_authoritative_in_flight: Option<u64>,
    pub last_authoritative_in_flight_at: Option<Instant>,
    pub last_authoritative_sent_watermark: Option<u64>,
    pub receipt_frozen_sent_watermark: Option<u64>,
    pub current_sent_watermark: u64,
}

#[derive(Debug, Default)]
pub(super) struct QuicMeasurementState {
    active_token: AtomicU64,
    ordinary_writer_count: AtomicU64,
    gate_notify: tokio::sync::Notify,
    fail_close_notify: tokio::sync::Notify,
    fail_close_requested: AtomicBool,
    epoch: Mutex<Option<MeasurementEpoch>>,
    ack_quarantine_active: AtomicBool,
    ack_quarantine: Mutex<Option<MeasurementAckQuarantine>>,
}

#[derive(Debug)]
struct MeasurementEpoch {
    metrics: MeasurementMetrics,
    started_at: Instant,
    write_started_at: Option<Instant>,
    receiver_confirmed: bool,
    measurement_start_sent: Option<Instant>,
    measurement_latest_sent: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
struct MeasurementAckQuarantine {
    token: u64,
    sent_at_or_after: Instant,
    epoch_sent_before: Instant,
    expires_at: Instant,
}

pub(super) struct OrdinaryWriteGuard {
    telemetry: Arc<QuicCarrierTelemetry>,
}

impl Drop for OrdinaryWriteGuard {
    fn drop(&mut self) {
        if self
            .telemetry
            .measurement
            .ordinary_writer_count
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            self.telemetry.measurement.gate_notify.notify_waiters();
        }
    }
}

pub(super) struct MeasurementGateReservation {
    telemetry: Arc<QuicCarrierTelemetry>,
    token: u64,
    keep_reserved: bool,
}

impl MeasurementGateReservation {
    pub(super) fn commit(mut self) {
        self.keep_reserved = true;
    }
}

impl Drop for MeasurementGateReservation {
    fn drop(&mut self) {
        if !self.keep_reserved {
            self.telemetry.release_measurement_token(self.token);
        }
    }
}

impl QuicCarrierTelemetry {
    pub(super) fn try_enter_ordinary_writer(self: &Arc<Self>) -> Option<OrdinaryWriteGuard> {
        if self.measurement.active_token.load(Ordering::Acquire) != 0 {
            return None;
        }
        self.measurement
            .ordinary_writer_count
            .fetch_add(1, Ordering::AcqRel);
        if self.measurement.active_token.load(Ordering::Acquire) == 0 {
            return Some(OrdinaryWriteGuard {
                telemetry: self.clone(),
            });
        }
        if self
            .measurement
            .ordinary_writer_count
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            self.measurement.gate_notify.notify_waiters();
        }
        None
    }

    pub(super) async fn enter_ordinary_writer(self: &Arc<Self>) -> OrdinaryWriteGuard {
        loop {
            if let Some(guard) = self.try_enter_ordinary_writer() {
                return guard;
            }
            let notified = self.measurement.gate_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.measurement.active_token.load(Ordering::Acquire) != 0 {
                notified.await;
            }
        }
    }

    pub(super) async fn wait_for_measurement_release(&self) {
        loop {
            let notified = self.measurement.gate_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.measurement.active_token.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    pub(super) async fn reserve_measurement_token(
        self: &Arc<Self>,
        token: u64,
        expires_at: Instant,
    ) -> Result<MeasurementGateReservation, QuicCarrierError> {
        if self
            .measurement
            .fail_close_requested
            .load(Ordering::Acquire)
        {
            return Err(QuicCarrierError::MeasurementExpired);
        }
        if self.measurement_ack_quarantine_blocks_new_epoch(Instant::now()) {
            return Err(QuicCarrierError::MeasurementBusy);
        }
        if self
            .measurement
            .active_token
            .compare_exchange(0, token, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(QuicCarrierError::MeasurementBusy);
        }
        let reservation = MeasurementGateReservation {
            telemetry: self.clone(),
            token,
            keep_reserved: false,
        };
        if self
            .measurement
            .fail_close_requested
            .load(Ordering::Acquire)
        {
            return Err(QuicCarrierError::MeasurementExpired);
        }
        loop {
            let notified = self.measurement.gate_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .measurement
                .ordinary_writer_count
                .load(Ordering::Acquire)
                == 0
            {
                return Ok(reservation);
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(expires_at)) => {
                    return Err(QuicCarrierError::MeasurementExpired);
                }
            }
        }
    }

    fn release_measurement_token(&self, token: u64) {
        if self
            .measurement
            .active_token
            .compare_exchange(token, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.measurement.gate_notify.notify_waiters();
        }
    }

    fn measurement_ack_quarantine_blocks_new_epoch(&self, now: Instant) -> bool {
        if !self
            .measurement
            .ack_quarantine_active
            .load(Ordering::Acquire)
        {
            return false;
        }
        let mut current = self
            .measurement
            .ack_quarantine
            .lock()
            .expect("QUIC measurement ACK quarantine lock");
        if current
            .as_ref()
            .is_some_and(|quarantine| now < quarantine.expires_at)
        {
            return true;
        }
        current.take();
        self.measurement
            .ack_quarantine_active
            .store(false, Ordering::Release);
        false
    }

    fn install_measurement_ack_quarantine(
        &self,
        token: u64,
        sent_at_or_after: Instant,
        receipt_at: Instant,
        retention: Duration,
    ) -> bool {
        let Some(expires_at) = receipt_at.checked_add(retention) else {
            return false;
        };
        let mut current = self
            .measurement
            .ack_quarantine
            .lock()
            .expect("QUIC measurement ACK quarantine lock");
        if current.as_ref().is_some_and(|quarantine| {
            quarantine.token != token && receipt_at < quarantine.expires_at
        }) {
            return false;
        }
        *current = Some(MeasurementAckQuarantine {
            token,
            sent_at_or_after,
            epoch_sent_before: receipt_at,
            expires_at,
        });
        self.measurement
            .ack_quarantine_active
            .store(true, Ordering::Release);
        true
    }

    fn consume_measurement_ack_quarantine(&self, now: Instant, sent: Instant) -> bool {
        if !self
            .measurement
            .ack_quarantine_active
            .load(Ordering::Acquire)
        {
            return false;
        }
        let mut current = self
            .measurement
            .ack_quarantine
            .lock()
            .expect("QUIC measurement ACK quarantine lock");
        let Some(quarantine) = current.as_ref() else {
            self.measurement
                .ack_quarantine_active
                .store(false, Ordering::Release);
            return false;
        };
        if now >= quarantine.expires_at {
            current.take();
            self.measurement
                .ack_quarantine_active
                .store(false, Ordering::Release);
            return false;
        }
        sent >= quarantine.sent_at_or_after && sent < quarantine.epoch_sent_before
    }

    fn measurement_metrics(spec: MeasurementSpec) -> MeasurementMetrics {
        MeasurementMetrics {
            token: spec.token,
            train_payload_bytes: spec.train_payload_bytes,
            sample_floor_bytes: spec.sample_floor_bytes,
            warmup_carrier_bytes: spec.warmup_carrier_bytes,
            required_timed_carrier_bytes: spec.required_timed_carrier_bytes,
            expires_at: spec.expires_at,
            retention: spec.retention,
            phase: MeasurementPhase::Writing,
            // Quinn's congestion callback does not identify ACK-only sends and
            // write completion does not drain its cross-stream send queue.
            // Native telemetry therefore never claims a clean start.
            started_clean: false,
            write_committed: false,
            written_payload_bytes: 0,
            written_data_frame_count: 0,
            total_acked_carrier_bytes: 0,
            total_ack_sample_count: 0,
            warmup_acked_carrier_bytes: 0,
            warmup_ack_sample_count: 0,
            measurement_acked_carrier_bytes: 0,
            measurement_ack_sample_count: 0,
            timed_measurement_acked_carrier_bytes: 0,
            timed_measurement_ack_sample_count: 0,
            app_limited_acked_carrier_bytes: 0,
            app_limited_ack_sample_count: 0,
            timed_measurement_ack_elapsed: None,
            native_threshold_at: None,
            confirmed_at: None,
            receipt_received_payload_bytes: 0,
            receipt_elapsed: None,
            receipt_rtt: None,
            receipt_at: None,
            last_authoritative_in_flight: None,
            last_authoritative_in_flight_at: None,
            last_authoritative_sent_watermark: None,
            receipt_frozen_sent_watermark: None,
            current_sent_watermark: 0,
        }
    }

    pub(super) fn install_measurement(
        &self,
        spec: MeasurementSpec,
        write_backlog: u64,
    ) -> Result<(), QuicCarrierError> {
        if spec.token == 0
            || spec.train_payload_bytes == 0
            || spec.sample_floor_bytes == 0
            || spec.sample_floor_bytes > spec.train_payload_bytes
            || spec.required_timed_carrier_bytes == 0
            || spec.retention.is_zero()
            || spec
                .warmup_carrier_bytes
                .saturating_add(spec.required_timed_carrier_bytes)
                > spec.train_payload_bytes
            || spec.expires_at <= Instant::now()
        {
            return Err(QuicCarrierError::InvalidMeasurement);
        }
        // `on_sent` includes ACK-only datagrams, which Quinn never reports to
        // `on_ack`; its additive BIF estimate can therefore contain phantom
        // bytes. The token receipt, not this provisional estimate, owns confirmation.
        if write_backlog != 0 {
            return Err(QuicCarrierError::MeasurementNotIdle);
        }
        let mut current = self
            .measurement
            .epoch
            .lock()
            .expect("QUIC measurement epoch lock");
        if self.measurement.active_token.load(Ordering::Acquire) != spec.token {
            return Err(QuicCarrierError::MeasurementBusy);
        }
        if current.as_ref().is_some_and(|epoch| {
            !matches!(
                epoch.metrics.phase,
                MeasurementPhase::Complete | MeasurementPhase::Expired | MeasurementPhase::Aborted
            )
        }) {
            return Err(QuicCarrierError::MeasurementBusy);
        }
        *current = Some(MeasurementEpoch {
            metrics: Self::measurement_metrics(spec),
            started_at: Instant::now(),
            write_started_at: None,
            receiver_confirmed: false,
            measurement_start_sent: None,
            measurement_latest_sent: None,
        });
        Ok(())
    }

    pub(super) fn mark_measurement_write_started(&self, token: u64, now: Instant) -> bool {
        let mut current = self
            .measurement
            .epoch
            .lock()
            .expect("QUIC measurement epoch lock");
        let Some(epoch) = current.as_mut().filter(|epoch| {
            epoch.metrics.token == token && epoch.metrics.phase == MeasurementPhase::Writing
        }) else {
            return false;
        };
        if now >= epoch.metrics.expires_at {
            return false;
        }
        epoch.write_started_at = Some(now);
        true
    }

    pub(super) fn record_measurement_data_written(&self, token: u64, payload_bytes: u64) -> bool {
        if payload_bytes == 0 {
            return false;
        }
        let mut current = self
            .measurement
            .epoch
            .lock()
            .expect("QUIC measurement epoch lock");
        let Some(epoch) = current.as_mut().filter(|epoch| {
            epoch.metrics.token == token && epoch.metrics.phase == MeasurementPhase::Writing
        }) else {
            return false;
        };
        let Some(written_payload_bytes) = epoch
            .metrics
            .written_payload_bytes
            .checked_add(payload_bytes)
            .filter(|written| *written <= epoch.metrics.train_payload_bytes)
        else {
            return false;
        };
        epoch.metrics.written_payload_bytes = written_payload_bytes;
        epoch.metrics.written_data_frame_count =
            epoch.metrics.written_data_frame_count.saturating_add(1);
        true
    }

    pub(super) fn commit_measurement_write(&self, token: u64, now: Instant) -> bool {
        let mut release_token = false;
        let mut current = self
            .measurement
            .epoch
            .lock()
            .expect("QUIC measurement epoch lock");
        let Some(epoch) = current
            .as_mut()
            .filter(|epoch| epoch.metrics.token == token)
        else {
            return false;
        };
        if !matches!(epoch.metrics.phase, MeasurementPhase::Writing) {
            return false;
        }
        if now >= epoch.metrics.expires_at {
            drop(current);
            let _ = self.finish_measurement(token, MeasurementPhase::Expired, now);
            return false;
        }
        if epoch.metrics.written_payload_bytes != epoch.metrics.train_payload_bytes
            || epoch.metrics.written_data_frame_count == 0
        {
            return false;
        }
        epoch.metrics.write_committed = true;
        epoch.metrics.phase = if self.measurement_can_finalize(epoch) {
            release_token = true;
            MeasurementPhase::Complete
        } else if epoch.metrics.native_threshold_at.is_some() {
            MeasurementPhase::AwaitingReceipt
        } else {
            MeasurementPhase::Measuring
        };
        drop(current);
        if release_token {
            self.release_measurement_token(token);
        }
        true
    }

    pub(super) fn accumulate_measurement_ack(
        &self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
    ) -> bool {
        let token = self.measurement.active_token.load(Ordering::Acquire);
        if bytes == 0 {
            return false;
        }
        if token == 0 {
            return self.consume_measurement_ack_quarantine(now, sent);
        }
        let mut current = self
            .measurement
            .epoch
            .lock()
            .expect("QUIC measurement epoch lock");
        let Some(epoch) = current.as_mut().filter(|epoch| {
            epoch.metrics.token == token
                && matches!(
                    epoch.metrics.phase,
                    MeasurementPhase::Writing
                        | MeasurementPhase::Measuring
                        | MeasurementPhase::AwaitingReceipt
                )
                && sent >= epoch.started_at
        }) else {
            drop(current);
            return self.consume_measurement_ack_quarantine(now, sent);
        };

        let accepted_bytes = bytes.min(
            epoch
                .metrics
                .train_payload_bytes
                .saturating_sub(epoch.metrics.total_acked_carrier_bytes),
        );
        // Carrier headers and autonomous QUIC control can share epoch packets.
        // Never let that overhead create more token credit than the declared
        // train, but continue routing excess ACKs away from product evidence.
        if accepted_bytes == 0 {
            return true;
        }
        epoch.metrics.total_acked_carrier_bytes = epoch
            .metrics
            .total_acked_carrier_bytes
            .saturating_add(accepted_bytes);
        epoch.metrics.total_ack_sample_count =
            epoch.metrics.total_ack_sample_count.saturating_add(1);
        if app_limited {
            epoch.metrics.app_limited_acked_carrier_bytes = epoch
                .metrics
                .app_limited_acked_carrier_bytes
                .saturating_add(accepted_bytes);
            epoch.metrics.app_limited_ack_sample_count =
                epoch.metrics.app_limited_ack_sample_count.saturating_add(1);
        }

        let warmup_remaining = epoch
            .metrics
            .warmup_carrier_bytes
            .saturating_sub(epoch.metrics.warmup_acked_carrier_bytes);
        let warmup_bytes = accepted_bytes.min(warmup_remaining);
        if warmup_bytes > 0 {
            epoch.metrics.warmup_acked_carrier_bytes = epoch
                .metrics
                .warmup_acked_carrier_bytes
                .saturating_add(warmup_bytes);
            epoch.metrics.warmup_ack_sample_count =
                epoch.metrics.warmup_ack_sample_count.saturating_add(1);
            if epoch.metrics.warmup_acked_carrier_bytes >= epoch.metrics.warmup_carrier_bytes {
                epoch.measurement_start_sent = Some(sent);
            }
        }
        let measurement_bytes = accepted_bytes.saturating_sub(warmup_bytes);
        if measurement_bytes > 0 {
            epoch.metrics.measurement_acked_carrier_bytes = epoch
                .metrics
                .measurement_acked_carrier_bytes
                .saturating_add(measurement_bytes);
            epoch.metrics.measurement_ack_sample_count =
                epoch.metrics.measurement_ack_sample_count.saturating_add(1);
            epoch.measurement_start_sent = Some(
                epoch
                    .measurement_start_sent
                    .map_or(sent, |start| start.min(sent)),
            );
            epoch.measurement_latest_sent = Some(
                epoch
                    .measurement_latest_sent
                    .map_or(sent, |latest| latest.max(sent)),
            );
        }
        true
    }

    pub(super) fn finish_measurement_ack_batch(&self, now: Instant, in_flight: u64) {
        let token = self.measurement.active_token.load(Ordering::Acquire);
        if token == 0 {
            return;
        }
        let mut release_token = false;
        let mut current = self
            .measurement
            .epoch
            .lock()
            .expect("QUIC measurement epoch lock");
        let Some(epoch) = current
            .as_mut()
            .filter(|epoch| epoch.metrics.token == token)
        else {
            return;
        };
        if now >= epoch.metrics.expires_at {
            drop(current);
            let _ = self.finish_measurement(token, MeasurementPhase::Expired, now);
            return;
        }
        epoch.metrics.last_authoritative_in_flight = Some(in_flight);
        epoch.metrics.last_authoritative_in_flight_at = Some(now);
        epoch.metrics.last_authoritative_sent_watermark = Some(self.sent_watermark());
        if let Some(elapsed) = epoch
            .measurement_start_sent
            .zip(epoch.measurement_latest_sent)
            .map(|(start, latest)| latest.saturating_duration_since(start))
            .filter(|elapsed| !elapsed.is_zero())
        {
            // The epoch retains bytes from zero-span callback batches. A later
            // paced timestamp makes the whole bounded window measurable once,
            // instead of randomly discarding part of the train per callback.
            // The numerator covers the same full measurement span as the
            // denominator; the required byte count is only the measurement threshold.
            epoch.metrics.timed_measurement_acked_carrier_bytes =
                epoch.metrics.measurement_acked_carrier_bytes;
            epoch.metrics.timed_measurement_ack_sample_count =
                epoch.metrics.measurement_ack_sample_count;
            epoch.metrics.timed_measurement_ack_elapsed = Some(elapsed);
        }

        if epoch.metrics.native_threshold_at.is_none()
            && epoch.metrics.timed_measurement_acked_carrier_bytes
                >= epoch.metrics.required_timed_carrier_bytes
        {
            epoch.metrics.native_threshold_at = Some(now);
            if epoch.metrics.write_committed {
                epoch.metrics.phase = MeasurementPhase::AwaitingReceipt;
            }
        }
        if self.measurement_can_finalize(epoch) {
            epoch.metrics.phase = MeasurementPhase::Complete;
            release_token = true;
        }
        drop(current);
        if release_token {
            self.release_measurement_token(token);
        }
    }

    fn measurement_can_finalize(&self, epoch: &MeasurementEpoch) -> bool {
        // Quinn delivers transmit callbacks before application receive events,
        // so the receipt-triggered ACK-only send can follow the last BIF zero
        // with no later ACK batch. Exact receipt owns completion; native flight
        // and send watermarks remain cleanup diagnostics only.
        epoch.receiver_confirmed
            && epoch.metrics.receipt_received_payload_bytes == epoch.metrics.train_payload_bytes
            && epoch.metrics.receipt_at.is_some()
            && epoch.metrics.receipt_elapsed.is_some()
            && epoch.metrics.write_committed
            && epoch.metrics.written_payload_bytes >= epoch.metrics.train_payload_bytes
    }

    pub(super) fn confirm_measurement_receipt(
        &self,
        token: u64,
        received_payload_bytes: u64,
        received_at: Instant,
        receipt_rtt: Duration,
    ) -> bool {
        let mut release_token = false;
        let mut current = self
            .measurement
            .epoch
            .lock()
            .expect("QUIC measurement epoch lock");
        let Some(epoch) = current.as_mut().filter(|epoch| {
            epoch.metrics.token == token
                && matches!(
                    epoch.metrics.phase,
                    MeasurementPhase::Writing
                        | MeasurementPhase::Measuring
                        | MeasurementPhase::AwaitingReceipt
                )
                && received_payload_bytes == epoch.metrics.train_payload_bytes
                && received_at < epoch.metrics.expires_at
        }) else {
            return false;
        };
        let Some(write_started_at) = epoch.write_started_at else {
            return false;
        };
        if received_at < write_started_at
            || received_at.checked_add(epoch.metrics.retention).is_none()
        {
            return false;
        }
        if epoch.receiver_confirmed {
            return true;
        }
        // Receipt owns completion and releases writers. This timestamp fence has
        // a separate job: suppress epoch-era ACKs after that public epoch retires.
        if !self.install_measurement_ack_quarantine(
            token,
            epoch.started_at,
            received_at,
            epoch.metrics.retention,
        ) {
            return false;
        }
        epoch.receiver_confirmed = true;
        epoch.metrics.receipt_received_payload_bytes = received_payload_bytes;
        epoch.metrics.receipt_elapsed =
            Some(received_at.saturating_duration_since(write_started_at));
        epoch.metrics.receipt_rtt = (!receipt_rtt.is_zero()).then_some(receipt_rtt);
        epoch.metrics.receipt_at = Some(received_at);
        epoch.metrics.confirmed_at = Some(received_at);
        epoch.metrics.receipt_frozen_sent_watermark = Some(self.sent_watermark());
        if self.measurement_can_finalize(epoch) {
            epoch.metrics.phase = MeasurementPhase::Complete;
            release_token = true;
        }
        drop(current);
        if release_token {
            self.release_measurement_token(token);
        }
        true
    }

    fn finish_measurement(&self, token: u64, phase: MeasurementPhase, now: Instant) -> bool {
        let mut current = self
            .measurement
            .epoch
            .lock()
            .expect("QUIC measurement epoch lock");
        let Some(epoch) = current
            .as_mut()
            .filter(|epoch| epoch.metrics.token == token)
        else {
            return false;
        };
        if matches!(
            epoch.metrics.phase,
            MeasurementPhase::Complete | MeasurementPhase::Expired | MeasurementPhase::Aborted
        ) {
            return false;
        }
        if phase == MeasurementPhase::Expired && now < epoch.metrics.expires_at {
            return false;
        }
        epoch.metrics.phase = phase;
        let should_close = epoch.write_started_at.is_some();
        drop(current);
        if should_close {
            self.measurement
                .fail_close_requested
                .store(true, Ordering::Release);
            self.measurement.fail_close_notify.notify_one();
        }
        self.release_measurement_token(token);
        should_close
    }

    pub(super) fn expire_measurement(&self, token: u64, now: Instant) -> bool {
        self.finish_measurement(token, MeasurementPhase::Expired, now)
    }

    pub(super) fn abort_measurement(&self, token: u64) -> bool {
        self.finish_measurement(token, MeasurementPhase::Aborted, Instant::now())
    }

    pub(super) fn retire_measurement(&self, token: u64) -> bool {
        let mut current = self
            .measurement
            .epoch
            .lock()
            .expect("QUIC measurement epoch lock");
        if current.as_ref().is_none_or(|epoch| {
            epoch.metrics.token != token
                || !matches!(
                    epoch.metrics.phase,
                    MeasurementPhase::Complete
                        | MeasurementPhase::Expired
                        | MeasurementPhase::Aborted
                )
        }) {
            return false;
        }
        current.take();
        true
    }

    pub(super) fn measurement_snapshot(&self) -> Option<MeasurementMetrics> {
        self.measurement
            .epoch
            .lock()
            .expect("QUIC measurement epoch lock")
            .as_ref()
            .map(|epoch| {
                let mut metrics = epoch.metrics;
                metrics.current_sent_watermark = self.sent_watermark();
                metrics
            })
    }

    pub(super) fn measurement_active(&self) -> bool {
        self.measurement.active_token.load(Ordering::Acquire) != 0
    }

    pub(super) fn measurement_failed_closed(&self) -> bool {
        self.measurement
            .fail_close_requested
            .load(Ordering::Acquire)
    }

    pub(super) fn mark_measurement_failed_closed(&self) {
        self.measurement
            .fail_close_requested
            .store(true, Ordering::Release);
    }

    pub(super) async fn wait_for_measurement_fail_close(&self) {
        self.measurement.fail_close_notify.notified().await;
    }
}

#[cfg(test)]
#[path = "measurement_test.rs"]
mod tests;
