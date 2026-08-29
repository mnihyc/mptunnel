use super::*;
use quinn::congestion::Controller as _;

#[derive(Clone)]
struct FixedPacingController(u64);

impl quinn::congestion::Controller for FixedPacingController {
    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        _is_ecn: bool,
        _lost_bytes: u64,
        _largest_lost: u64,
        _space: quinn::congestion::SpaceId,
    ) {
    }

    fn on_mtu_update(&mut self, _new_mtu: u16) {}

    fn window(&self) -> u64 {
        12_000
    }

    fn pacing_rate(&self) -> Option<u64> {
        Some(self.0)
    }

    fn metrics(&self) -> quinn::congestion::ControllerMetrics {
        let mut metrics = quinn::congestion::ControllerMetrics::default();
        metrics.congestion_window = self.window();
        metrics.pacing_rate = Some(self.0);
        metrics
    }

    fn clone_box(&self) -> Box<dyn quinn::congestion::Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[derive(Debug, Default)]
struct ForwardedCallbacks {
    sent_space: Option<quinn::congestion::SpaceId>,
    end_ack_space: Option<quinn::congestion::SpaceId>,
    lost_space: Option<quinn::congestion::SpaceId>,
    congestion: Vec<(bool, quinn::congestion::SpaceId)>,
    spurious: u64,
    abandoned: u64,
    validated_ecn: u64,
    cwnd_limited: u64,
    ack_frequency: Option<(u64, Duration)>,
}

#[derive(Debug, Clone)]
struct RecordingController(Arc<Mutex<ForwardedCallbacks>>);

impl quinn::congestion::Controller for RecordingController {
    fn on_packet_sent(
        &mut self,
        _now: Instant,
        _bytes: u16,
        _prior_in_flight: u64,
        _packet_number: u64,
        space: quinn::congestion::SpaceId,
        _app_limited: bool,
    ) -> Option<quinn::congestion::PacketDeliveryState> {
        self.0.lock().unwrap().sent_space = Some(space);
        None
    }

    fn on_cwnd_limited(&mut self) {
        self.0.lock().unwrap().cwnd_limited += 1;
    }

    fn on_end_acks(
        &mut self,
        _now: Instant,
        _in_flight: u64,
        _app_limited: bool,
        _largest_packet_num_acked: Option<u64>,
        space: quinn::congestion::SpaceId,
    ) {
        self.0.lock().unwrap().end_ack_space = Some(space);
    }

    fn on_packet_lost(
        &mut self,
        _lost_bytes: u16,
        packet_number: u64,
        space: quinn::congestion::SpaceId,
        _now: Instant,
    ) -> Option<quinn::congestion::RecoveryTransactionId> {
        self.0.lock().unwrap().lost_space = Some(space);
        Some(quinn::congestion::RecoveryTransactionId::new(packet_number))
    }

    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        is_ecn: bool,
        _lost_bytes: u64,
        _largest_lost: u64,
        space: quinn::congestion::SpaceId,
    ) {
        self.0.lock().unwrap().congestion.push((is_ecn, space));
    }

    fn on_spurious_congestion_event(
        &mut self,
        _transaction: quinn::congestion::RecoveryTransactionId,
    ) -> bool {
        self.0.lock().unwrap().spurious += 1;
        true
    }

    fn on_recovery_transaction_abandoned(
        &mut self,
        _transaction: quinn::congestion::RecoveryTransactionId,
    ) {
        self.0.lock().unwrap().abandoned += 1;
    }

    fn on_validated_ecn_congestion_event(&mut self) {
        self.0.lock().unwrap().validated_ecn += 1;
    }

    fn on_ack_frequency_update(
        &mut self,
        ack_eliciting_threshold: u64,
        requested_max_ack_delay: Duration,
    ) {
        self.0.lock().unwrap().ack_frequency =
            Some((ack_eliciting_threshold, requested_max_ack_delay));
    }

    fn on_mtu_update(&mut self, _new_mtu: u16) {}

    fn window(&self) -> u64 {
        12_000
    }

    fn clone_box(&self) -> Box<dyn quinn::congestion::Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[test]
fn instrumented_controller_forwards_pacing_rate_through_clones() {
    const RATE: u64 = 12_345_678;
    let controller = InstrumentedController::new(
        Box::new(FixedPacingController(RATE)),
        Arc::new(QuicCarrierTelemetry::default()),
    );

    assert_eq!(controller.pacing_rate(), Some(RATE));
    assert_eq!(controller.clone_box().pacing_rate(), Some(RATE));
    assert_eq!(controller.metrics().pacing_rate, Some(RATE));
    assert_eq!(controller.clone_box().metrics().pacing_rate, Some(RATE));
}

#[test]
fn production_initial_and_fresh_paths_both_construct_bbr3() {
    let now = Instant::now();
    let controller = quinn::congestion::ControllerFactory::build(
        Arc::new(InstrumentedBbrConfig::default()),
        now,
        1200,
    )
    .into_any()
    .downcast::<InstrumentedController>()
    .expect("instrumented production controller");
    assert!(
        controller
            .inner
            .clone_box()
            .into_any()
            .downcast::<quinn::congestion::Bbr3>()
            .is_ok(),
        "initial production controller must be BBR3"
    );

    let fresh = controller
        .fresh_path_box(now + Duration::from_secs(1), 1400)
        .expect("fresh controller")
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("fresh instrumented controller");
    assert!(
        fresh
            .inner
            .clone_box()
            .into_any()
            .downcast::<quinn::congestion::Bbr3>()
            .is_ok(),
        "fresh network path must restart with BBR3"
    );
}

#[test]
fn path_loss_compensation_constructs_initial_and_fresh_bbr3_without_reusing_initial_rate() {
    fn assert_bbr3_loss_compensation(controller: &InstrumentedController, expected: &str) {
        let bbr3 = controller
            .inner
            .clone_box()
            .into_any()
            .downcast::<quinn::congestion::Bbr3>()
            .expect("inner BBR3 controller");
        assert!(
            format!("{bbr3:?}").contains(expected),
            "constructed BBR3 must contain {expected}"
        );
    }

    let now = Instant::now();
    let path = concat!(
        "quic://example.com:443?",
        "initial-rate-mbps=25&loss-compensation-percent=5.1234"
    )
    .parse::<crate::transport::PathSpec>()
    .expect("QUIC path metadata");
    let controller = quinn::congestion::ControllerFactory::build(
        Arc::new(InstrumentedBbrConfig::for_path(&path.metadata)),
        now,
        1200,
    )
    .into_any()
    .downcast::<InstrumentedController>()
    .expect("instrumented production controller");
    assert_eq!(controller.loss_compensation.ppm(), 51_234);
    assert_bbr3_loss_compensation(&controller, "loss_compensation_floor: 0.051234");
    let fresh = controller
        .fresh_path_box(now + Duration::from_secs(1), 1400)
        .expect("fresh controller")
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("fresh instrumented controller");
    assert_eq!(fresh.loss_compensation.ppm(), 51_234);
    assert_bbr3_loss_compensation(&fresh, "loss_compensation_floor: 0.051234");
    let rate_only = crate::transport::PathMetadata {
        initial_rate: crate::transport::RateHint::BitsPerSecond(25_000_000),
        ..Default::default()
    };
    let default_controller = quinn::congestion::ControllerFactory::build(
        Arc::new(InstrumentedBbrConfig::for_path(&rate_only)),
        now,
        1200,
    )
    .into_any()
    .downcast::<InstrumentedController>()
    .expect("default instrumented controller");
    assert_eq!(default_controller.loss_compensation.ppm(), 100_000);
    assert_bbr3_loss_compensation(&default_controller, "loss_compensation_floor: 0.1");

    let disabled = "quic://example.com:443?loss-compensation-percent=0"
        .parse::<crate::transport::PathSpec>()
        .expect("explicitly disabled QUIC loss compensation");
    let disabled_controller = quinn::congestion::ControllerFactory::build(
        Arc::new(InstrumentedBbrConfig::for_path(&disabled.metadata)),
        now,
        1200,
    )
    .into_any()
    .downcast::<InstrumentedController>()
    .expect("disabled-compensation instrumented controller");
    assert_eq!(disabled_controller.loss_compensation.ppm(), 0);
    assert_bbr3_loss_compensation(&disabled_controller, "loss_compensation_floor: 0.0");
}

#[test]
fn instrumented_controller_forwards_packet_space_and_recovery_callbacks_once() {
    let now = Instant::now();
    let recorded = Arc::new(Mutex::new(ForwardedCallbacks::default()));
    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let mut controller =
        InstrumentedController::new(Box::new(RecordingController(recorded.clone())), telemetry);
    controller.on_packet_sent(now, 1200, 0, 7, quinn::congestion::SpaceId::Initial, false);
    controller.on_end_acks(
        now + Duration::from_millis(50),
        0,
        false,
        Some(7),
        quinn::congestion::SpaceId::Initial,
    );
    let transaction = controller
        .on_packet_lost(
            1200,
            8,
            quinn::congestion::SpaceId::Handshake,
            now + Duration::from_millis(50),
        )
        .expect("recording controller recovery transaction");
    assert_eq!(
        controller.snapshot().lost_bytes,
        0,
        "per-packet loss forwarding must not duplicate aggregate loss telemetry"
    );
    controller.on_congestion_event(
        now + Duration::from_millis(50),
        now,
        false,
        false,
        1200,
        8,
        quinn::congestion::SpaceId::Handshake,
    );
    controller.on_congestion_event(
        now + Duration::from_millis(50),
        now,
        false,
        true,
        0,
        9,
        quinn::congestion::SpaceId::Data,
    );
    assert!(controller.on_spurious_congestion_event(transaction));
    controller.on_recovery_transaction_abandoned(transaction);
    controller.on_validated_ecn_congestion_event();
    controller.on_cwnd_limited();
    controller.on_ack_frequency_update(4, Duration::from_millis(25));

    assert_eq!(controller.snapshot().lost_bytes, 1200);
    let callbacks = recorded.lock().unwrap();
    assert_eq!(
        callbacks.sent_space,
        Some(quinn::congestion::SpaceId::Initial)
    );
    assert_eq!(
        callbacks.end_ack_space,
        Some(quinn::congestion::SpaceId::Initial)
    );
    assert_eq!(
        callbacks.lost_space,
        Some(quinn::congestion::SpaceId::Handshake)
    );
    assert_eq!(
        callbacks.congestion,
        vec![
            (false, quinn::congestion::SpaceId::Handshake),
            (true, quinn::congestion::SpaceId::Data),
        ]
    );
    assert_eq!(callbacks.spurious, 1);
    assert_eq!(callbacks.abandoned, 1);
    assert_eq!(callbacks.validated_ecn, 1);
    assert_eq!(callbacks.cwnd_limited, 1);
    assert_eq!(
        callbacks.ack_frequency,
        Some((4, Duration::from_millis(25)))
    );
}

#[test]
fn same_path_clone_preserves_telemetry_epoch() {
    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let controller =
        InstrumentedController::new(Box::new(FixedPacingController(1)), telemetry.clone());
    let cloned = controller
        .clone_box()
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("instrumented clone");

    assert!(Arc::ptr_eq(&cloned.telemetry, &telemetry));
    assert!(Arc::ptr_eq(
        &cloned.path_telemetry,
        &controller.path_telemetry
    ));
    assert_eq!(
        cloned.path_telemetry.path_epoch,
        controller.path_telemetry.path_epoch
    );
}

#[test]
fn fresh_network_path_keeps_owner_and_isolates_stale_callbacks() {
    let base = Instant::now();
    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let inner = quinn::congestion::ControllerFactory::build(
        Arc::new(quinn::congestion::BbrConfig::default()),
        base,
        1200,
    );
    let mut old = InstrumentedController::new(inner, telemetry.clone());
    let old_epoch = old.path_telemetry.path_epoch;
    telemetry.record_delivery_evidence_written(1200);

    let mut fresh = old
        .fresh_path_box(base + Duration::from_secs(1), 1400)
        .expect("instrumented controller supports fresh paths")
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("fresh instrumented controller");

    assert!(Arc::ptr_eq(&fresh.telemetry, &telemetry));
    assert!(!Arc::ptr_eq(&fresh.path_telemetry, &old.path_telemetry));
    assert_eq!(fresh.path_telemetry.path_epoch, old_epoch + 1);

    old.accumulate_ack_telemetry(base, 1200, false);
    old.finish_ack_telemetry(base + Duration::from_millis(10), 0, false);
    old.on_congestion_event(
        base + Duration::from_millis(10),
        base,
        false,
        false,
        1200,
        7,
        quinn::congestion::SpaceId::Data,
    );

    let before_current_ack = fresh.snapshot();
    assert_eq!(before_current_ack.path_epoch, old_epoch + 1);
    assert_eq!(before_current_ack.newly_acked_bytes, None);
    assert_eq!(before_current_ack.lost_bytes, 0);
    assert_eq!(before_current_ack.bytes_in_flight, None);
    assert_eq!(
        telemetry.delivery_evidence_pending_ack_bytes(),
        1200,
        "a retired path callback cannot consume current delivery evidence"
    );

    fresh.accumulate_ack_telemetry(base + Duration::from_secs(1), 1200, false);
    fresh.finish_ack_telemetry(
        base + Duration::from_secs(1) + Duration::from_millis(10),
        0,
        false,
    );
    assert_eq!(fresh.snapshot().newly_acked_bytes, Some(1200));
    assert_eq!(telemetry.delivery_evidence_pending_ack_bytes(), 0);
}

#[test]
fn instrumented_controller_preserves_compact_bbr_delivery_state() {
    let base = Instant::now();
    let (mut controller, _) = test_instrumented_controller(base);

    let state = controller
        .on_packet_sent(base, 1200, 0, 7, quinn::congestion::SpaceId::Data, false)
        .expect("BBR packet delivery state");

    assert_eq!(state.delivered, 0);
    assert_eq!(state.delivered_time, base);
    assert_eq!(state.send_elapsed_ns, 0);
}

#[tokio::test]
async fn first_unacknowledged_product_write_wakes_idle_metrics() {
    let telemetry = QuicCarrierTelemetry::default();
    let activity = telemetry.delivery_activity_notify();
    let started = activity.notified();
    tokio::pin!(started);
    started.as_mut().enable();

    telemetry.record_delivery_evidence_written(64 * 1024);

    tokio::time::timeout(Duration::from_secs(1), &mut started)
        .await
        .expect("idle QUIC metrics wake when product delivery becomes active");
    assert_eq!(telemetry.delivery_evidence_pending_ack_bytes(), 64 * 1024);
}

#[test]
fn native_ack_reconciles_pending_delivery_evidence() {
    let telemetry = QuicCarrierTelemetry::default();
    let path_telemetry = telemetry.allocate_path_telemetry();
    telemetry.record_delivery_evidence_written(64 * 1024);
    assert_eq!(telemetry.delivery_evidence_written_bytes(), 64 * 1024);
    assert_eq!(telemetry.delivery_evidence_pending_ack_bytes(), 64 * 1024);
    assert_eq!(
        telemetry.delivery_evidence_pending_ack_bytes(),
        64 * 1024,
        "send-credit reads must not consume ACK evidence"
    );

    path_telemetry.publish_ack_batch(
        QuicAckTelemetryTotals {
            acked_bytes: 16 * 1024,
            sample_count: 1,
            ..QuicAckTelemetryTotals::default()
        },
        48 * 1024,
        false,
    );
    assert_eq!(
        telemetry.reconcile_delivery_evidence_ack(path_telemetry.path_epoch, 16 * 1024),
        16 * 1024
    );
    assert_eq!(telemetry.delivery_evidence_pending_ack_bytes(), 48 * 1024);
    assert_eq!(path_telemetry.bytes_in_flight(), Some(48 * 1024));
}

#[test]
fn cancelled_delivery_evidence_removes_only_unacknowledged_bytes() {
    let telemetry = QuicCarrierTelemetry::default();
    let path_telemetry = telemetry.allocate_path_telemetry();
    telemetry.record_delivery_evidence_written(64 * 1024);
    assert_eq!(
        telemetry.reconcile_delivery_evidence_ack(path_telemetry.path_epoch, 48 * 1024),
        48 * 1024
    );

    telemetry.record_delivery_evidence_cancelled(64 * 1024);

    assert_eq!(telemetry.delivery_evidence_pending_ack_bytes(), 0);
    assert_eq!(telemetry.delivery_evidence_cancelled_bytes(), 16 * 1024);
}

#[test]
fn quic_ack_snapshot_keeps_non_app_limited_classification_coherent() {
    const BATCHES: u64 = 20_000;
    const ACK_BYTES: u64 = 1200;
    const NON_APP_ACKS_PER_BATCH: u64 = 2;
    const ACKS_PER_BATCH: u64 = 3;
    const ELAPSED_PER_BATCH: Duration = Duration::from_micros(7);
    let telemetry = QuicCarrierTelemetry::default().allocate_path_telemetry();
    let writer = {
        let telemetry = telemetry.clone();
        std::thread::spawn(move || {
            for index in 0..BATCHES {
                let non_app_limited = index % 2 == 0;
                telemetry.publish_ack_batch(
                    QuicAckTelemetryTotals {
                        delivery_clock_epoch: 1,
                        acked_bytes: ACKS_PER_BATCH * ACK_BYTES,
                        non_app_limited_acked_bytes: if non_app_limited {
                            NON_APP_ACKS_PER_BATCH * ACK_BYTES
                        } else {
                            0
                        },
                        timed_non_app_limited_acked_bytes: if non_app_limited {
                            NON_APP_ACKS_PER_BATCH * ACK_BYTES
                        } else {
                            0
                        },
                        non_app_limited_ack_elapsed_nanos: if non_app_limited {
                            duration_as_u64_nanos(ELAPSED_PER_BATCH)
                        } else {
                            0
                        },
                        sample_count: ACKS_PER_BATCH,
                        non_app_limited_sample_count: if non_app_limited {
                            NON_APP_ACKS_PER_BATCH
                        } else {
                            0
                        },
                        timed_non_app_limited_sample_count: if non_app_limited {
                            NON_APP_ACKS_PER_BATCH
                        } else {
                            0
                        },
                        timed_non_app_limited_delivery_evidence_acked_bytes: 0,
                        timed_non_app_limited_delivery_evidence_sample_count: 0,
                        timed_non_app_limited_delivery_evidence_elapsed_nanos: 0,
                        delivery_evidence_acked_bytes: 0,
                    },
                    0,
                    !non_app_limited,
                );
            }
        })
    };

    let mut acked_bytes = 0_u64;
    let mut non_app_limited_bytes = 0_u64;
    let mut samples = 0_u64;
    let mut non_app_limited_samples = 0_u64;
    let mut current_timed_non_app_limited_bytes = 0_u64;
    let mut current_timed_non_app_limited_samples = 0_u64;
    let mut current_non_app_limited_elapsed = Duration::ZERO;
    while !writer.is_finished() {
        let snapshot = telemetry.snapshot();
        assert_eq!(
            snapshot.newly_acked_bytes.unwrap_or(0),
            snapshot.delivery_sample_count * ACK_BYTES
        );
        assert_eq!(
            snapshot.non_app_limited_acked_bytes.unwrap_or(0),
            snapshot.non_app_limited_delivery_sample_count * ACK_BYTES
        );
        assert_eq!(
            snapshot.timed_non_app_limited_acked_bytes.unwrap_or(0),
            snapshot.timed_non_app_limited_delivery_sample_count * ACK_BYTES
        );
        assert_eq!(
            snapshot.non_app_limited_ack_elapsed.unwrap_or_default(),
            ELAPSED_PER_BATCH
                * (snapshot.timed_non_app_limited_delivery_sample_count / NON_APP_ACKS_PER_BATCH)
                    as u32
        );
        acked_bytes = acked_bytes.saturating_add(snapshot.newly_acked_bytes.unwrap_or(0));
        non_app_limited_bytes =
            non_app_limited_bytes.saturating_add(snapshot.non_app_limited_acked_bytes.unwrap_or(0));
        samples = samples.saturating_add(snapshot.delivery_sample_count);
        non_app_limited_samples =
            non_app_limited_samples.saturating_add(snapshot.non_app_limited_delivery_sample_count);
        current_timed_non_app_limited_bytes =
            snapshot.timed_non_app_limited_acked_bytes.unwrap_or(0);
        current_timed_non_app_limited_samples =
            snapshot.timed_non_app_limited_delivery_sample_count;
        current_non_app_limited_elapsed = snapshot.non_app_limited_ack_elapsed.unwrap_or_default();
    }
    writer.join().expect("QUIC ACK telemetry writer");
    let final_snapshot = telemetry.snapshot();
    assert_eq!(
        final_snapshot.newly_acked_bytes.unwrap_or(0),
        final_snapshot.delivery_sample_count * ACK_BYTES
    );
    assert_eq!(
        final_snapshot.non_app_limited_acked_bytes.unwrap_or(0),
        final_snapshot.non_app_limited_delivery_sample_count * ACK_BYTES
    );
    assert_eq!(
        final_snapshot
            .non_app_limited_ack_elapsed
            .unwrap_or_default(),
        ELAPSED_PER_BATCH
            * (final_snapshot.timed_non_app_limited_delivery_sample_count / NON_APP_ACKS_PER_BATCH)
                as u32
    );
    acked_bytes = acked_bytes.saturating_add(final_snapshot.newly_acked_bytes.unwrap_or(0));
    non_app_limited_bytes = non_app_limited_bytes
        .saturating_add(final_snapshot.non_app_limited_acked_bytes.unwrap_or(0));
    samples = samples.saturating_add(final_snapshot.delivery_sample_count);
    non_app_limited_samples = non_app_limited_samples
        .saturating_add(final_snapshot.non_app_limited_delivery_sample_count);
    current_timed_non_app_limited_bytes = final_snapshot
        .timed_non_app_limited_acked_bytes
        .unwrap_or(current_timed_non_app_limited_bytes);
    current_timed_non_app_limited_samples = final_snapshot
        .timed_non_app_limited_delivery_sample_count
        .max(current_timed_non_app_limited_samples);
    current_non_app_limited_elapsed = final_snapshot
        .non_app_limited_ack_elapsed
        .unwrap_or(current_non_app_limited_elapsed);

    assert_eq!(acked_bytes, BATCHES * ACKS_PER_BATCH * ACK_BYTES);
    assert_eq!(
        non_app_limited_bytes,
        BATCHES / 2 * NON_APP_ACKS_PER_BATCH * ACK_BYTES
    );
    assert_eq!(samples, BATCHES * ACKS_PER_BATCH);
    assert_eq!(
        non_app_limited_samples,
        BATCHES / 2 * NON_APP_ACKS_PER_BATCH
    );
    assert_eq!(
        current_timed_non_app_limited_bytes,
        BATCHES / 2 * NON_APP_ACKS_PER_BATCH * ACK_BYTES
    );
    assert_eq!(
        current_timed_non_app_limited_samples,
        BATCHES / 2 * NON_APP_ACKS_PER_BATCH
    );
    assert_eq!(
        current_non_app_limited_elapsed,
        ELAPSED_PER_BATCH * (BATCHES / 2) as u32
    );
}
fn test_instrumented_controller(base: Instant) -> (InstrumentedController, Arc<QuicPathTelemetry>) {
    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let inner = quinn::congestion::ControllerFactory::build(
        Arc::new(quinn::congestion::BbrConfig::default()),
        base,
        1200,
    );
    let controller = InstrumentedController::new(inner, telemetry);
    let path_telemetry = controller.path_telemetry.clone();
    (controller, path_telemetry)
}
#[test]
fn quic_first_ack_batch_excludes_path_rtt_and_app_limited_idle() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.accumulate_ack_telemetry(base, 200, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(4), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(200), 900, false);
    let first = telemetry.snapshot();
    assert_eq!(first.delivery_clock_epoch, 1);
    assert_eq!(first.non_app_limited_acked_bytes, Some(300));
    assert_eq!(first.non_app_limited_delivery_sample_count, 2);
    assert_eq!(
        first.non_app_limited_ack_elapsed,
        Some(Duration::from_millis(4)),
        "the first delivery interval must not include the 200 ms path RTT"
    );

    controller.accumulate_ack_telemetry(base + Duration::from_millis(205), 25, true);
    controller.finish_ack_telemetry(base + Duration::from_millis(210), 875, true);
    let idle = telemetry.snapshot();
    assert_eq!(idle.delivery_clock_epoch, 1);
    assert_eq!(idle.newly_acked_bytes, Some(25));
    assert_eq!(idle.non_app_limited_acked_bytes, None);
    assert_eq!(idle.timed_non_app_limited_acked_bytes, Some(300));
    assert_eq!(
        idle.non_app_limited_ack_elapsed,
        Some(Duration::from_millis(4))
    );

    controller.accumulate_ack_telemetry(base + Duration::from_millis(300), 250, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(306), 250, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(600), 0, false);
    let after_idle = telemetry.snapshot();
    assert_eq!(after_idle.delivery_clock_epoch, 2);
    assert_eq!(after_idle.non_app_limited_acked_bytes, Some(500));
    assert_eq!(
        after_idle.non_app_limited_ack_elapsed,
        Some(Duration::from_millis(6)),
        "an app-limited end must reset both delivery clocks"
    );
    assert_eq!(after_idle.bytes_in_flight, Some(0));
}

#[test]
fn quic_coalesced_snapshot_exposes_only_current_delivery_clock_epoch() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.telemetry.record_delivery_evidence_written(900);
    controller.accumulate_ack_telemetry(base, 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(100), 700, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(110), 100, true);
    controller.finish_ack_telemetry(base + Duration::from_millis(120), 600, true);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(130), 300, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(135), 300, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(140), 0, false);

    let coalesced = telemetry.snapshot();
    assert_eq!(coalesced.delivery_clock_epoch, 2);
    assert_eq!(coalesced.newly_acked_bytes, Some(900));
    assert_eq!(coalesced.timed_non_app_limited_acked_bytes, Some(600));
    assert_eq!(
        coalesced.timed_non_app_limited_delivery_evidence_acked_bytes, 600,
        "app-limited Product ACKs and the completed prior clock cannot enter the current timed epoch"
    );
    assert_eq!(
        coalesced.timed_non_app_limited_delivery_evidence_sample_count,
        2
    );
    assert_eq!(
        coalesced.timed_non_app_limited_delivery_evidence_elapsed,
        Duration::from_millis(5)
    );
    assert_eq!(coalesced.timed_non_app_limited_delivery_sample_count, 2);
    assert_eq!(
        coalesced.non_app_limited_ack_elapsed,
        Some(Duration::from_millis(5))
    );

    controller.finish_ack_telemetry(base + Duration::from_millis(150), 0, true);
    controller.finish_ack_telemetry(base + Duration::from_millis(160), 0, true);
    assert_eq!(
        telemetry.snapshot().delivery_clock_epoch,
        2,
        "repeated idle callbacks arm one boundary but cannot mint epochs"
    );
}

#[test]
fn quic_mixed_app_limited_batch_omits_ambiguous_timed_product_attribution() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);
    controller.telemetry.record_delivery_evidence_written(300);
    controller.accumulate_ack_telemetry(base, 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(5), 100, true);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(100), 0, true);

    let mixed = telemetry.snapshot();
    assert_eq!(mixed.delivery_clock_epoch, 1);
    assert_eq!(mixed.delivery_evidence_newly_acked_bytes, Some(300));
    assert_eq!(mixed.timed_non_app_limited_acked_bytes, Some(200));
    assert_eq!(
        mixed.non_app_limited_ack_elapsed,
        Some(Duration::from_millis(10))
    );
    assert_eq!(
        mixed.timed_non_app_limited_delivery_evidence_acked_bytes, 0,
        "aggregate Product attribution cannot be guessed across mixed app-limited packet classes"
    );
    assert_eq!(
        mixed.timed_non_app_limited_delivery_evidence_sample_count,
        0
    );
    assert_eq!(
        mixed.timed_non_app_limited_delivery_evidence_elapsed,
        Duration::ZERO
    );
}

#[test]
fn quic_final_non_app_batch_publishes_before_delivery_clock_closes() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);
    controller.telemetry.record_delivery_evidence_written(200);
    controller.accumulate_ack_telemetry(base, 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(100), 0, true);

    let closing = telemetry.snapshot();
    assert_eq!(closing.delivery_clock_epoch, 1);
    assert!(closing.app_limited);
    assert_eq!(
        closing.timed_non_app_limited_delivery_evidence_acked_bytes,
        200
    );
    assert_eq!(
        closing.timed_non_app_limited_delivery_evidence_sample_count,
        2
    );
    assert_eq!(
        closing.timed_non_app_limited_delivery_evidence_elapsed,
        Duration::from_millis(10)
    );

    controller.finish_ack_telemetry(base + Duration::from_millis(110), 0, true);
    assert_eq!(telemetry.snapshot().delivery_clock_epoch, 1);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(120), 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(125), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(130), 0, false);
    let reopened = telemetry.snapshot();
    assert_eq!(reopened.delivery_clock_epoch, 2);
    assert_eq!(
        reopened.timed_non_app_limited_delivery_evidence_acked_bytes,
        0
    );
}

#[test]
fn quic_ack_send_clock_resists_ack_compression() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.accumulate_ack_telemetry(base, 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(100), 0, false);
    assert_eq!(
        telemetry.snapshot().non_app_limited_ack_elapsed,
        Some(Duration::from_millis(10))
    );

    controller.accumulate_ack_telemetry(base + Duration::from_millis(15), 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(30), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(101), 0, false);
    assert_eq!(
        telemetry.snapshot().non_app_limited_ack_elapsed,
        Some(Duration::from_millis(30)),
        "the send clock must win when ACK batches are only 1 ms apart"
    );
}

#[test]
fn quic_zero_span_first_ack_batch_is_untimed_but_seeds_clocks() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.accumulate_ack_telemetry(base, 1200, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(250), 0, false);
    let seed = telemetry.snapshot();
    assert_eq!(seed.non_app_limited_acked_bytes, Some(1200));
    assert_eq!(seed.non_app_limited_delivery_sample_count, 1);
    assert_eq!(seed.timed_non_app_limited_acked_bytes, None);
    assert_eq!(seed.timed_non_app_limited_delivery_sample_count, 0);
    assert_eq!(seed.non_app_limited_ack_elapsed, None);

    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 1200, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(260), 0, false);
    assert_eq!(
        telemetry.snapshot().non_app_limited_ack_elapsed,
        Some(Duration::from_millis(10)),
        "an untimed first batch must still seed both clocks"
    );
}

#[test]
fn quic_untimed_seed_cannot_join_timed_bytes_between_metric_polls() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.accumulate_ack_telemetry(base, 1200, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(250), 0, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 1200, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(260), 0, false);

    let combined = telemetry.snapshot();
    assert_eq!(combined.non_app_limited_acked_bytes, Some(2400));
    assert_eq!(combined.non_app_limited_delivery_sample_count, 2);
    assert_eq!(combined.timed_non_app_limited_acked_bytes, Some(1200));
    assert_eq!(combined.timed_non_app_limited_delivery_sample_count, 1);
    assert_eq!(
        combined.non_app_limited_ack_elapsed,
        Some(Duration::from_millis(10))
    );
}

#[test]
fn quic_reordered_ack_batch_cannot_move_send_frontier_backward() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.accumulate_ack_telemetry(base, 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(100), 0, false);
    assert_eq!(
        telemetry.snapshot().non_app_limited_ack_elapsed,
        Some(Duration::from_millis(10))
    );

    controller.accumulate_ack_telemetry(base + Duration::from_millis(8), 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(2), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(101), 0, false);
    assert_eq!(
        telemetry.snapshot().non_app_limited_ack_elapsed,
        Some(Duration::from_millis(16)),
        "within-batch send spacing must guard a reordered ACK batch"
    );

    controller.accumulate_ack_telemetry(base + Duration::from_millis(12), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(102), 0, false);
    assert_eq!(
        telemetry.snapshot().non_app_limited_ack_elapsed,
        Some(Duration::from_millis(18)),
        "the send frontier must remain at 10 ms rather than regress to 8 ms"
    );
}
