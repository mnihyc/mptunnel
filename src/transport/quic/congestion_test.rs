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
        _lost_bytes: u64,
    ) {
    }

    fn on_mtu_update(&mut self, _new_mtu: u16) {}

    fn window(&self) -> u64 {
        12_000
    }

    fn pacing_rate(&self) -> Option<u64> {
        Some(self.0)
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
}

#[test]
fn instrumented_controller_preserves_bbr_packet_delivery_state() {
    let base = Instant::now();
    let (mut controller, _) = test_instrumented_controller(base);

    let state = controller
        .on_packet_sent(base, 1200, 0, 7, false)
        .expect("BBR packet delivery state");

    assert_eq!(state.delivered, 0);
    assert_eq!(state.delivered_time, base);
    assert_eq!(state.first_sent_time, base);
    assert_eq!(state.packet_number, 7);
    assert!(!state.app_limited);
    assert_eq!(state.tx_in_flight, 1200);
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
    telemetry.record_delivery_evidence_written(64 * 1024);
    assert_eq!(telemetry.delivery_evidence_written_bytes(), 64 * 1024);
    assert_eq!(telemetry.delivery_evidence_pending_ack_bytes(), 64 * 1024);
    assert_eq!(
        telemetry.delivery_evidence_pending_ack_bytes(),
        64 * 1024,
        "send-credit reads must not consume ACK evidence"
    );

    telemetry.publish_ack_batch(
        QuicAckTelemetryTotals {
            acked_bytes: 16 * 1024,
            sample_count: 1,
            ..QuicAckTelemetryTotals::default()
        },
        48 * 1024,
        false,
    );
    assert_eq!(telemetry.delivery_evidence_pending_ack_bytes(), 48 * 1024);
    assert_eq!(telemetry.bytes_in_flight(), Some(48 * 1024));
}

#[test]
fn quic_ack_snapshot_keeps_non_app_limited_classification_coherent() {
    const BATCHES: u64 = 20_000;
    const ACK_BYTES: u64 = 1200;
    const NON_APP_ACKS_PER_BATCH: u64 = 2;
    const ACKS_PER_BATCH: u64 = 3;
    const ELAPSED_PER_BATCH: Duration = Duration::from_micros(7);
    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let writer = {
        let telemetry = telemetry.clone();
        std::thread::spawn(move || {
            for index in 0..BATCHES {
                let non_app_limited = index % 2 == 0;
                telemetry.publish_ack_batch(
                    QuicAckTelemetryTotals {
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
    let mut non_app_limited_elapsed = Duration::ZERO;
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
            snapshot.timed_non_app_limited_acked_bytes,
            snapshot.non_app_limited_acked_bytes
        );
        assert_eq!(
            snapshot.timed_non_app_limited_delivery_sample_count,
            snapshot.non_app_limited_delivery_sample_count
        );
        assert_eq!(
            snapshot.non_app_limited_ack_elapsed.unwrap_or_default(),
            ELAPSED_PER_BATCH
                * (snapshot.non_app_limited_delivery_sample_count / NON_APP_ACKS_PER_BATCH) as u32
        );
        acked_bytes = acked_bytes.saturating_add(snapshot.newly_acked_bytes.unwrap_or(0));
        non_app_limited_bytes =
            non_app_limited_bytes.saturating_add(snapshot.non_app_limited_acked_bytes.unwrap_or(0));
        samples = samples.saturating_add(snapshot.delivery_sample_count);
        non_app_limited_samples =
            non_app_limited_samples.saturating_add(snapshot.non_app_limited_delivery_sample_count);
        non_app_limited_elapsed += snapshot.non_app_limited_ack_elapsed.unwrap_or_default();
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
            * (final_snapshot.non_app_limited_delivery_sample_count / NON_APP_ACKS_PER_BATCH)
                as u32
    );
    acked_bytes = acked_bytes.saturating_add(final_snapshot.newly_acked_bytes.unwrap_or(0));
    non_app_limited_bytes = non_app_limited_bytes
        .saturating_add(final_snapshot.non_app_limited_acked_bytes.unwrap_or(0));
    samples = samples.saturating_add(final_snapshot.delivery_sample_count);
    non_app_limited_samples = non_app_limited_samples
        .saturating_add(final_snapshot.non_app_limited_delivery_sample_count);
    non_app_limited_elapsed += final_snapshot
        .non_app_limited_ack_elapsed
        .unwrap_or_default();

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
        non_app_limited_elapsed,
        ELAPSED_PER_BATCH * (BATCHES / 2) as u32
    );
}
fn test_instrumented_controller(
    base: Instant,
) -> (InstrumentedController, Arc<QuicCarrierTelemetry>) {
    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let inner = quinn::congestion::ControllerFactory::build(
        Arc::new(quinn::congestion::BbrConfig::default()),
        base,
        1200,
    );
    (
        InstrumentedController::new(inner, telemetry.clone()),
        telemetry,
    )
}
#[test]
fn quic_first_ack_batch_excludes_path_rtt_and_app_limited_idle() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.accumulate_ack_telemetry(base, 200, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(4), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(200), 900, false);
    let first = telemetry.snapshot();
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
    assert_eq!(idle.newly_acked_bytes, Some(25));
    assert_eq!(idle.non_app_limited_acked_bytes, None);
    assert_eq!(idle.non_app_limited_ack_elapsed, None);

    controller.accumulate_ack_telemetry(base + Duration::from_millis(300), 250, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(306), 250, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(600), 0, false);
    let after_idle = telemetry.snapshot();
    assert_eq!(after_idle.non_app_limited_acked_bytes, Some(500));
    assert_eq!(
        after_idle.non_app_limited_ack_elapsed,
        Some(Duration::from_millis(6)),
        "an app-limited end must reset both delivery clocks"
    );
    assert_eq!(after_idle.bytes_in_flight, Some(0));
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
        Some(Duration::from_millis(20)),
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
        Some(Duration::from_millis(6)),
        "within-batch send spacing must guard a reordered ACK batch"
    );

    controller.accumulate_ack_telemetry(base + Duration::from_millis(12), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(102), 0, false);
    assert_eq!(
        telemetry.snapshot().non_app_limited_ack_elapsed,
        Some(Duration::from_millis(2)),
        "the send frontier must remain at 10 ms rather than regress to 8 ms"
    );
}
