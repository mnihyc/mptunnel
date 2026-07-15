use super::super::InstrumentedController;
use super::*;
use tokio::time::timeout;

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
fn test_measurement_spec(base: Instant) -> MeasurementSpec {
    MeasurementSpec {
        token: 91,
        train_payload_bytes: 300,
        sample_floor_bytes: 300,
        warmup_carrier_bytes: 100,
        required_timed_carrier_bytes: 200,
        expires_at: base + Duration::from_secs(5),
        retention: Duration::from_secs(1),
    }
}

async fn install_test_measurement(
    telemetry: &Arc<QuicCarrierTelemetry>,
    spec: MeasurementSpec,
) -> Instant {
    let reservation = telemetry
        .reserve_measurement_token(spec.token, spec.expires_at)
        .await
        .expect("reserve capacity token");
    telemetry
        .install_measurement(spec, 0)
        .expect("install measurement epoch");
    reservation.commit();
    let write_started_at = Instant::now();
    assert!(telemetry.mark_measurement_write_started(spec.token, write_started_at));
    assert!(telemetry.record_measurement_data_written(spec.token, spec.train_payload_bytes,));
    assert!(telemetry.commit_measurement_write(spec.token, Instant::now()));
    write_started_at
}
#[tokio::test]
async fn quic_measurement_accepts_app_limited_acks_without_product_evidence() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);
    let spec = test_measurement_spec(base);
    install_test_measurement(&telemetry, spec).await;

    controller.route_ack_telemetry(
        base + Duration::from_millis(100),
        base + Duration::from_millis(1),
        100,
        true,
    );
    controller.finish_ack_telemetry(base + Duration::from_millis(100), 200, true);
    telemetry.finish_measurement_ack_batch(base + Duration::from_millis(100), 200);
    controller.route_ack_telemetry(
        base + Duration::from_millis(110),
        base + Duration::from_millis(11),
        100,
        true,
    );
    controller.route_ack_telemetry(
        base + Duration::from_millis(110),
        base + Duration::from_millis(21),
        100,
        true,
    );
    controller.finish_ack_telemetry(base + Duration::from_millis(110), 0, true);
    telemetry.finish_measurement_ack_batch(base + Duration::from_millis(110), 0);

    let provisional = telemetry.snapshot();
    assert_eq!(provisional.newly_acked_bytes, None);
    assert_eq!(provisional.non_app_limited_acked_bytes, None);
    let capacity = provisional.measurement.expect("capacity snapshot");
    assert_eq!(capacity.phase, MeasurementPhase::AwaitingReceipt);
    assert_eq!(capacity.total_acked_carrier_bytes, 300);
    assert_eq!(capacity.warmup_acked_carrier_bytes, 100);
    assert_eq!(capacity.measurement_acked_carrier_bytes, 200);
    assert_eq!(capacity.timed_measurement_acked_carrier_bytes, 200);
    assert_eq!(capacity.app_limited_acked_carrier_bytes, 300);
    assert_eq!(capacity.app_limited_ack_sample_count, 3);
    assert_eq!(
        capacity.timed_measurement_ack_elapsed,
        Some(Duration::from_millis(20))
    );
    assert!(capacity.native_threshold_at.is_some());
    assert_eq!(capacity.confirmed_at, None);
    assert!(telemetry.confirm_measurement_receipt(
        spec.token,
        spec.train_payload_bytes,
        base + Duration::from_millis(111),
        Duration::from_millis(10),
    ));
    let completed = telemetry
        .snapshot()
        .measurement
        .expect("receipt-completed epoch");
    assert_eq!(completed.phase, MeasurementPhase::Complete);
    assert_eq!(completed.confirmed_at, completed.receipt_at);
    assert_eq!(
        completed.receipt_received_payload_bytes,
        spec.train_payload_bytes
    );
    assert!(completed.receipt_elapsed.is_some());
}

#[tokio::test]
async fn quic_measurement_zero_span_measurement_does_not_prove() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);
    let spec = MeasurementSpec {
        train_payload_bytes: 200,
        sample_floor_bytes: 200,
        warmup_carrier_bytes: 0,
        required_timed_carrier_bytes: 100,
        ..test_measurement_spec(base)
    };
    install_test_measurement(&telemetry, spec).await;

    controller.route_ack_telemetry(
        base + Duration::from_millis(100),
        base + Duration::from_millis(1),
        100,
        false,
    );
    controller.finish_ack_telemetry(base + Duration::from_millis(100), 100, false);
    telemetry.finish_measurement_ack_batch(base + Duration::from_millis(100), 100);
    let untimed = telemetry.snapshot().measurement.expect("untimed epoch");
    assert_eq!(untimed.measurement_acked_carrier_bytes, 100);
    assert_eq!(untimed.timed_measurement_acked_carrier_bytes, 0);
    assert_eq!(untimed.phase, MeasurementPhase::Measuring);

    controller.route_ack_telemetry(
        base + Duration::from_millis(110),
        base + Duration::from_millis(11),
        100,
        false,
    );
    controller.finish_ack_telemetry(base + Duration::from_millis(110), 0, false);
    telemetry.finish_measurement_ack_batch(base + Duration::from_millis(110), 0);
    let timed = telemetry.snapshot().measurement.expect("timed epoch");
    assert_eq!(
        timed.timed_measurement_acked_carrier_bytes,
        timed.measurement_acked_carrier_bytes
    );
    assert_eq!(
        timed.timed_measurement_ack_elapsed,
        Some(Duration::from_millis(10))
    );
    assert_eq!(timed.phase, MeasurementPhase::AwaitingReceipt);
    assert!(telemetry.confirm_measurement_receipt(
        spec.token,
        spec.train_payload_bytes,
        base + Duration::from_millis(111),
        Duration::from_millis(10),
    ));
    assert_eq!(
        telemetry
            .snapshot()
            .measurement
            .expect("receipt-completed epoch")
            .phase,
        MeasurementPhase::Complete
    );
}

#[tokio::test]
async fn quic_measurement_measurement_epoch_retains_zero_span_batches() {
    let base = Instant::now();
    let (_controller, telemetry) = test_instrumented_controller(base);
    let spec = MeasurementSpec {
        train_payload_bytes: 400,
        sample_floor_bytes: 300,
        warmup_carrier_bytes: 100,
        required_timed_carrier_bytes: 250,
        ..test_measurement_spec(base)
    };
    install_test_measurement(&telemetry, spec).await;

    for (ack_ms, sent_ms) in [(100, 1), (110, 11), (120, 11), (130, 21)] {
        assert!(telemetry.accumulate_measurement_ack(
            base + Duration::from_millis(ack_ms),
            base + Duration::from_millis(sent_ms),
            100,
            true,
        ));
        telemetry.finish_measurement_ack_batch(base + Duration::from_millis(ack_ms), 0);
    }

    let epoch = telemetry.snapshot().measurement.expect("measurement epoch");
    assert_eq!(epoch.measurement_acked_carrier_bytes, 300);
    assert_eq!(
        epoch.timed_measurement_acked_carrier_bytes,
        epoch.measurement_acked_carrier_bytes
    );
    assert_eq!(
        epoch.timed_measurement_ack_elapsed,
        Some(Duration::from_millis(20))
    );
    assert_eq!(epoch.phase, MeasurementPhase::AwaitingReceipt);
}

#[tokio::test]
async fn quic_measurement_snapshot_is_cumulative_and_terminal_is_sticky() {
    let base = Instant::now();
    let (_controller, telemetry) = test_instrumented_controller(base);
    let spec = test_measurement_spec(base);
    install_test_measurement(&telemetry, spec).await;
    assert!(telemetry.accumulate_measurement_ack(
        base + Duration::from_millis(10),
        base + Duration::from_millis(1),
        75,
        true,
    ));
    telemetry.finish_measurement_ack_batch(base + Duration::from_millis(10), 225);

    let first = telemetry.snapshot().measurement.expect("first snapshot");
    let second = telemetry.snapshot().measurement.expect("second snapshot");
    assert_eq!(first.total_acked_carrier_bytes, 75);
    assert_eq!(second.total_acked_carrier_bytes, 75);
    assert_eq!(second.app_limited_acked_carrier_bytes, 75);
    assert!(!telemetry.retire_measurement(spec.token));
    assert!(telemetry.abort_measurement(spec.token));
    assert_eq!(
        telemetry
            .snapshot()
            .measurement
            .expect("aborted epoch")
            .phase,
        MeasurementPhase::Aborted
    );
    assert!(telemetry.retire_measurement(spec.token));
    assert!(telemetry.snapshot().measurement.is_none());
}

#[tokio::test]
async fn quic_measurement_replaces_only_a_terminal_prior_token() {
    let base = Instant::now();
    let (_controller, telemetry) = test_instrumented_controller(base);
    let first = test_measurement_spec(base);
    let first_reservation = telemetry
        .reserve_measurement_token(first.token, first.expires_at)
        .await
        .expect("reserve first token");
    telemetry
        .install_measurement(first, 0)
        .expect("install first epoch");
    first_reservation.commit();
    assert!(!telemetry.abort_measurement(first.token));

    let second = MeasurementSpec {
        token: first.token + 1,
        ..first
    };
    install_test_measurement(&telemetry, second).await;
    let snapshot = telemetry.snapshot().measurement.expect("replacement epoch");
    assert_eq!(snapshot.token, second.token);
    assert_eq!(snapshot.phase, MeasurementPhase::Measuring);
    assert_eq!(snapshot.total_acked_carrier_bytes, 0);
    assert_eq!(snapshot.timed_measurement_acked_carrier_bytes, 0);
}

#[tokio::test]
async fn quic_measurement_rejects_mismatched_receipt_without_releasing_gate() {
    let base = Instant::now();
    let (_controller, telemetry) = test_instrumented_controller(base);
    let spec = test_measurement_spec(base);
    install_test_measurement(&telemetry, spec).await;
    telemetry.finish_measurement_ack_batch(base + Duration::from_millis(1), 0);

    assert!(!telemetry.confirm_measurement_receipt(
        spec.token,
        spec.train_payload_bytes - 1,
        base + Duration::from_millis(2),
        Duration::from_millis(10),
    ));
    assert_eq!(
        telemetry.measurement.active_token.load(Ordering::Acquire),
        spec.token
    );
    let snapshot = telemetry.snapshot().measurement.expect("active epoch");
    assert_eq!(snapshot.receipt_received_payload_bytes, 0);
    assert_eq!(snapshot.receipt_elapsed, None);
}

#[tokio::test]
async fn quic_measurement_exact_receipt_releases_despite_native_flight_snapshot() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);
    let spec = test_measurement_spec(base);
    install_test_measurement(&telemetry, spec).await;

    controller.route_ack_telemetry(
        base + Duration::from_millis(20),
        base + Duration::from_millis(1),
        100,
        true,
    );
    controller.route_ack_telemetry(
        base + Duration::from_millis(20),
        base + Duration::from_millis(11),
        200,
        true,
    );
    controller.finish_ack_telemetry(base + Duration::from_millis(20), 120, true);
    telemetry.finish_measurement_ack_batch(base + Duration::from_millis(20), 120);
    assert!(telemetry.confirm_measurement_receipt(
        spec.token,
        spec.train_payload_bytes,
        base + Duration::from_millis(21),
        Duration::from_millis(10),
    ));
    assert_eq!(
        telemetry
            .snapshot()
            .measurement
            .expect("completed epoch")
            .phase,
        MeasurementPhase::Complete
    );
    assert_eq!(
        telemetry.measurement.active_token.load(Ordering::Acquire),
        0
    );
}

#[tokio::test]
async fn quic_measurement_zero_then_ack_only_then_receipt_releases_gate() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);
    let spec = test_measurement_spec(base);
    install_test_measurement(&telemetry, spec).await;

    telemetry.finish_measurement_ack_batch(base + Duration::from_millis(1), 0);
    quinn::congestion::Controller::on_sent(&mut controller, base, 1200, 0);
    assert!(telemetry.confirm_measurement_receipt(
        spec.token,
        spec.train_payload_bytes,
        base + Duration::from_millis(2),
        Duration::from_millis(10),
    ));
    assert_eq!(
        telemetry
            .snapshot()
            .measurement
            .expect("receipt-completed epoch")
            .phase,
        MeasurementPhase::Complete,
        "exact receipt must not wait for an ACK-only send to receive an impossible ACK"
    );
    assert_eq!(
        telemetry.measurement.active_token.load(Ordering::Acquire),
        0
    );
    let completed = telemetry
        .snapshot()
        .measurement
        .expect("receipt-completed epoch metrics");
    assert_eq!(completed.last_authoritative_in_flight, Some(0));
    assert_eq!(completed.last_authoritative_sent_watermark, Some(0));
    assert_eq!(completed.receipt_frozen_sent_watermark, Some(1200));
    assert_eq!(completed.current_sent_watermark, 1200);
}

#[tokio::test]
async fn quic_measurement_exact_receipt_releases_without_ack_batch() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);
    let spec = test_measurement_spec(base);
    install_test_measurement(&telemetry, spec).await;

    quinn::congestion::Controller::on_sent(&mut controller, base, 1200, 0);
    assert!(telemetry.confirm_measurement_receipt(
        spec.token,
        spec.train_payload_bytes,
        base + Duration::from_millis(1),
        Duration::from_millis(10),
    ));
    let received = telemetry
        .snapshot()
        .measurement
        .expect("receipt-confirmed epoch");
    assert_eq!(received.receipt_frozen_sent_watermark, Some(1200));
    assert_eq!(received.current_sent_watermark, 1200);
    assert_eq!(received.phase, MeasurementPhase::Complete);
    assert_eq!(received.last_authoritative_in_flight, None);
    assert_eq!(received.last_authoritative_sent_watermark, None);
    assert_eq!(
        telemetry.measurement.active_token.load(Ordering::Acquire),
        0
    );
}

#[tokio::test]
async fn quic_capacity_receipt_releases_writers_but_quarantines_probe_era_acks() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);
    let spec = test_measurement_spec(base);
    let probe_started_at = install_test_measurement(&telemetry, spec).await;
    let receipt_at = Instant::now();

    assert!(telemetry.confirm_measurement_receipt(
        spec.token,
        spec.train_payload_bytes,
        receipt_at,
        Duration::from_millis(10),
    ));
    assert_eq!(
        telemetry.measurement.active_token.load(Ordering::Acquire),
        0
    );
    assert!(telemetry.retire_measurement(spec.token));
    assert!(telemetry.snapshot().measurement.is_none());

    let second = MeasurementSpec {
        token: spec.token + 1,
        ..spec
    };
    assert!(matches!(
        telemetry
            .reserve_measurement_token(second.token, second.expires_at)
            .await,
        Err(QuicCarrierError::MeasurementBusy)
    ));

    // Product payload may be admitted as soon as receipt releases the writer
    // gate. A late ACK from the epoch interval must not satisfy that evidence.
    controller.route_ack_telemetry(
        receipt_at + Duration::from_millis(1),
        probe_started_at,
        200,
        false,
    );
    controller.finish_ack_telemetry(receipt_at + Duration::from_millis(1), 0, false);
    let quarantined = telemetry.snapshot();
    assert_eq!(quarantined.newly_acked_bytes, None);
    assert_eq!(quarantined.non_app_limited_acked_bytes, None);

    controller.route_ack_telemetry(
        receipt_at + Duration::from_millis(2),
        receipt_at + Duration::from_millis(1),
        88,
        false,
    );
    controller.finish_ack_telemetry(receipt_at + Duration::from_millis(2), 0, false);
    let immediate_product = telemetry.snapshot();
    assert_eq!(immediate_product.newly_acked_bytes, Some(88));
    assert_eq!(immediate_product.non_app_limited_acked_bytes, Some(88));

    let quarantine_end = receipt_at + spec.retention;
    controller.route_ack_telemetry(
        quarantine_end + Duration::from_millis(1),
        quarantine_end,
        77,
        false,
    );
    controller.finish_ack_telemetry(quarantine_end + Duration::from_millis(1), 0, false);
    let later_product = telemetry.snapshot();
    assert_eq!(later_product.newly_acked_bytes, Some(77));
    assert_eq!(later_product.non_app_limited_acked_bytes, Some(77));

    let reservation = telemetry
        .reserve_measurement_token(second.token, second.expires_at)
        .await
        .expect("expired quarantine permits replacement epoch");
    drop(reservation);
}

#[tokio::test]
async fn quic_measurement_ack_after_deadline_expires_instead_of_proving() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);
    let spec = MeasurementSpec {
        expires_at: base + Duration::from_secs(1),
        ..test_measurement_spec(base)
    };
    install_test_measurement(&telemetry, spec).await;

    controller.route_ack_telemetry(
        base + Duration::from_millis(1_001),
        base + Duration::from_millis(1),
        100,
        true,
    );
    controller.route_ack_telemetry(
        base + Duration::from_millis(1_001),
        base + Duration::from_millis(2),
        200,
        true,
    );
    controller.finish_ack_telemetry(base + Duration::from_millis(1_001), 0, true);
    telemetry.finish_measurement_ack_batch(base + Duration::from_millis(1_001), 0);

    let expired = telemetry.snapshot().measurement.expect("expired epoch");
    assert_eq!(expired.phase, MeasurementPhase::Expired);
    assert_eq!(expired.confirmed_at, None);
    assert_eq!(
        telemetry.measurement.active_token.load(Ordering::Acquire),
        0
    );
}

#[tokio::test]
async fn quic_ack_only_flight_estimate_cannot_claim_or_block_clean_start() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);
    quinn::congestion::Controller::on_sent(&mut controller, base, 37, 0);
    assert_eq!(telemetry.snapshot().bytes_in_flight, None);
    let spec = test_measurement_spec(base);
    let reservation = telemetry
        .reserve_measurement_token(spec.token, spec.expires_at)
        .await
        .expect("reserve capacity token");
    telemetry
        .install_measurement(spec, 0)
        .expect("provisional epoch ignores phantom native flight");
    reservation.commit();

    let epoch = telemetry.snapshot().measurement.expect("installed epoch");
    assert!(!epoch.started_clean);
    assert_eq!(epoch.phase, MeasurementPhase::Writing);
    assert!(!telemetry.abort_measurement(spec.token));
}

#[tokio::test]
async fn quic_capacity_gate_waits_for_an_existing_ordinary_writer() {
    let base = Instant::now();
    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let ordinary = telemetry.enter_ordinary_writer().await;
    let waiter_telemetry = telemetry.clone();
    let waiter = tokio::spawn(async move {
        waiter_telemetry
            .reserve_measurement_token(41, base + Duration::from_secs(1))
            .await
    });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    drop(ordinary);
    let reservation = timeout(Duration::from_secs(1), waiter)
        .await
        .expect("capacity reservation timeout")
        .expect("capacity reservation task")
        .expect("capacity reservation");
    assert_eq!(
        telemetry.measurement.active_token.load(Ordering::Acquire),
        41
    );
    drop(reservation);
    assert_eq!(
        telemetry.measurement.active_token.load(Ordering::Acquire),
        0
    );
}
