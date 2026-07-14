use super::super::response_evidence::ServerPathMetricsSource;
use super::super::response_snapshot::server_bulk_output_snapshot;
use super::super::test_support::{
    assert_test_rate_close, binding_for_underlay, output_entry_for_key, stream_data_frame,
    stream_data_frame_at, test_ack_clock_rate_sample,
};
use super::{RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED, ResponseAckClockCalibrationState};
use crate::model::capacity::{
    MIN_RATE_SAMPLE_BYTES, PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS,
};
use crate::protocol::{OffsetRange, PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::path::model::metric_epoch_now;
use crate::runtime::relay_striping::reliable_stream_frame_payload_bytes;
use crate::scheduler::{FlowLane, PathRateScope};
use std::time::{Duration, Instant};

#[test]
fn tcp_first_stream_ack_is_progress_but_not_a_capacity_clock() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let frame = stream_data_frame(MIN_RATE_SAMPLE_BYTES as usize);

    binding.record_owner_flight(key, &frame);
    std::thread::sleep(Duration::from_millis(1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: reliable_stream_frame_payload_bytes(&frame) as u64,
    }]);

    let entry = output_entry_for_key(&binding, key);
    assert_eq!(entry.bytes_in_flight, 0);
    assert_eq!(entry.delivery_samples, 1);
    assert_eq!(entry.owner_data_acked_bytes, MIN_RATE_SAMPLE_BYTES);
    assert!(entry.product_progress_rate_bps.is_none());
    assert!(entry.delivery_rate_bps.is_none());
    assert!(entry.tcp_ack_clock_rate_bps.is_none());
    assert!(entry.srtt_ms.is_none());
}

#[test]
fn tcp_ordinary_ack_clock_excludes_assignment_queue_residence() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let window_bytes = PATH_OPEN_SCORE_BYTES;
    binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
    binding.record_owner_flight(
        key,
        &stream_data_frame_at(window_bytes as u64, window_bytes),
    );
    let clock = Instant::now();
    let first_ack_at = clock + Duration::from_secs(1);
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: 0,
            end: window_bytes as u64,
        }],
        first_ack_at,
    );
    let provisional = output_entry_for_key(&binding, key);
    assert!(provisional.tcp_ack_clock_rate_bps.is_none());
    assert!(provisional.product_progress_rate_bps.is_none());
    assert!(provisional.delivery_rate_bps.is_none());
    assert!(provisional.srtt_ms.is_none());

    let second_ack_at = first_ack_at + Duration::from_millis(100);
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: window_bytes as u64,
            end: (2 * window_bytes) as u64,
        }],
        second_ack_at,
    );
    let expected_rate = window_bytes as f64 * 8.0 / 0.1;
    let measured = output_entry_for_key(&binding, key);
    assert_test_rate_close(measured.tcp_ack_clock_rate_bps, expected_rate);
    assert_test_rate_close(measured.product_progress_rate_bps, expected_rate);
    assert_test_rate_close(measured.delivery_rate_bps, expected_rate);

    let late_offset = (2 * window_bytes) as u64;
    binding.record_owner_flight(key, &stream_data_frame_at(late_offset, window_bytes));
    {
        let mut flights = binding
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        flights
            .get_mut(&late_offset)
            .expect("late flight")
            .iter_mut()
            .for_each(|flight| flight.sent_at = second_ack_at + Duration::from_millis(1));
    }
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: late_offset,
            end: late_offset + window_bytes as u64,
        }],
        second_ack_at + Duration::from_millis(100),
    );
    let after_late_assignment = output_entry_for_key(&binding, key);
    assert_test_rate_close(after_late_assignment.tcp_ack_clock_rate_bps, expected_rate);
    assert_test_rate_close(after_late_assignment.delivery_rate_bps, expected_rate);

    let late_ack_at = second_ack_at + Duration::from_millis(100);
    let recovery_offset = late_offset + window_bytes as u64;
    binding.record_owner_flight(key, &stream_data_frame_at(recovery_offset, window_bytes));
    {
        let mut flights = binding
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        flights
            .get_mut(&recovery_offset)
            .expect("recovery flight")
            .iter_mut()
            .for_each(|flight| flight.sent_at = late_ack_at - Duration::from_millis(1));
    }
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: recovery_offset,
            end: recovery_offset + window_bytes as u64,
        }],
        late_ack_at + Duration::from_millis(50),
    );
    let recovered_rate = (3 * window_bytes) as f64 * 8.0 / 0.25;
    let recovered = output_entry_for_key(&binding, key);
    assert_test_rate_close(recovered.tcp_ack_clock_rate_bps, recovered_rate);
    assert_test_rate_close(recovered.delivery_rate_bps, recovered_rate);
}

#[test]
fn tcp_ack_clock_can_reduce_rate_while_carrier_snapshot_is_app_limited() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let window_bytes = PATH_OPEN_SCORE_BYTES;
    binding.update_path_metrics(
        key,
        PathMetrics {
            path_id: key.path_id,
            underlay: key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 20_000,
            srtt_us: 20_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 100_000_000,
            pacing_rate_bps: 100_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: window_bytes as u64,
            inflight_hi_bytes: window_bytes as u64,
            confidence_ppm: 1_000_000,
            app_limited: true,
            has_ack_derived_data_sample: false,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
        ServerPathMetricsSource::LocalSender,
    );
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs.entries.first_mut().expect("TCP output");
        entry.tcp_ack_clock_rate_bps = Some(100_000_000.0);
        entry.product_progress_rate_bps = Some(100_000_000.0);
        entry.delivery_rate_bps = Some(100_000_000.0);
    }
    binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
    binding.record_owner_flight(
        key,
        &stream_data_frame_at(window_bytes as u64, window_bytes),
    );
    let first_ack_at = Instant::now() + Duration::from_millis(100);
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: 0,
            end: window_bytes as u64,
        }],
        first_ack_at,
    );
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: window_bytes as u64,
            end: (2 * window_bytes) as u64,
        }],
        first_ack_at + Duration::from_millis(100),
    );

    let entry = output_entry_for_key(&binding, key);
    assert!(
        entry
            .tcp_ack_clock_rate_bps
            .is_some_and(|rate| rate < 100_000_000.0),
        "per-flow TCP ACK evidence must not inherit QUIC's app-limited max filter"
    );
}

#[test]
fn tcp_ack_clock_is_independent_from_global_contiguous_frontier() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let window_bytes = PATH_OPEN_SCORE_BYTES;
    binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
    binding.record_owner_flight(
        key,
        &stream_data_frame_at(window_bytes as u64, window_bytes),
    );
    let first_ack_at = Instant::now() + Duration::from_secs(1);
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: window_bytes as u64,
            end: (2 * window_bytes) as u64,
        }],
        first_ack_at,
    );
    let hole = output_entry_for_key(&binding, key);
    assert!(hole.tcp_ack_clock_rate_bps.is_none());

    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: 0,
            end: window_bytes as u64,
        }],
        first_ack_at + Duration::from_millis(100),
    );
    let expected_rate = window_bytes as f64 * 8.0 / 0.1;
    let measured = output_entry_for_key(&binding, key);
    assert_test_rate_close(measured.tcp_ack_clock_rate_bps, expected_rate);
}

#[test]
fn tcp_response_single_stage_ack_clock_sample_preserves_startup_rate() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let window_bytes = PATH_OPEN_SCORE_BYTES;
    let identity = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs.entries.first_mut().expect("TCP output");
        entry.product_progress_rate_bps = Some(1.0);
        entry.delivery_rate_bps = Some(1.0);
        let identity = (entry.key, entry.incarnation);
        let mut calibration = ResponseAckClockCalibrationState::new(
            (2 * window_bytes) as u64,
            (2 * window_bytes) as u64,
        );
        calibration.spent_bytes = (2 * window_bytes) as u64;
        outputs.ack_clock_calibrations.insert(identity, calibration);
        outputs.active_ack_clock_calibration = Some(identity);
        identity
    };
    binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
    binding.record_owner_flight(
        key,
        &stream_data_frame_at(window_bytes as u64, window_bytes),
    );
    std::thread::sleep(Duration::from_millis(2));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: window_bytes as u64,
    }]);
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        assert!(
            !outputs
                .ack_clock_calibrations
                .get(&identity)
                .expect("calibration state")
                .proven,
            "the first send-to-ACK window remains provisional"
        );
        assert_eq!(outputs.active_ack_clock_calibration, Some(identity));
    }

    std::thread::sleep(Duration::from_millis(2));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: window_bytes as u64,
        end: (2 * window_bytes) as u64,
    }]);
    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let entry = outputs.entries.first().expect("TCP output");
    assert!(
        outputs
            .ack_clock_calibrations
            .get(&identity)
            .expect("calibration state")
            .proven,
        "the later window was already in flight at the previous ACK"
    );
    assert_eq!(entry.delivery_rate_bps, Some(1.0));
    assert_eq!(entry.product_progress_rate_bps, Some(1.0));
    assert_eq!(
        outputs
            .ack_clock_calibrations
            .get(&identity)
            .expect("calibration state")
            .calibrated_rate_bps,
        None,
        "one compressed stage sample cannot replace the startup rate"
    );
    assert_eq!(outputs.active_ack_clock_calibration, None);
    assert!(entry.tcp_product_rate_evidence.is_none());
    assert!(entry.tcp_ack_clock_rate_bps.is_none());
    assert!(entry.tcp_capacity_prior.is_none());
    drop(outputs);

    let next_offset = (2 * window_bytes) as u64;
    binding.record_owner_flight(
        key,
        &stream_data_frame_at(next_offset, MIN_RATE_SAMPLE_BYTES as usize),
    );
    binding.record_owner_flight(
        key,
        &stream_data_frame_at(
            next_offset + MIN_RATE_SAMPLE_BYTES,
            MIN_RATE_SAMPLE_BYTES as usize,
        ),
    );
    let ordinary_ack_at = Instant::now() + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: next_offset,
            end: next_offset + MIN_RATE_SAMPLE_BYTES,
        }],
        ordinary_ack_at,
    );
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: next_offset + MIN_RATE_SAMPLE_BYTES,
            end: next_offset + 2 * MIN_RATE_SAMPLE_BYTES,
        }],
        ordinary_ack_at + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED,
    );
    let entry = output_entry_for_key(&binding, key);
    assert!(
        entry.delivery_rate_bps.is_some_and(|rate| rate > 1.0),
        "a terminal calibration without a robust rate must not freeze later ordinary TCP evidence"
    );
}

#[test]
fn tcp_response_robust_calibration_yields_to_mature_ordinary_rate_without_fake_rtt() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let identity = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs.entries.first_mut().expect("TCP output");
        entry.product_progress_rate_bps = Some(7_000_000_000.0);
        entry.delivery_rate_bps = Some(7_000_000_000.0);
        entry.srtt_ms = Some(10.0);
        entry.delivery_samples = u32::MAX;
        let identity = (entry.key, entry.incarnation);
        let initial = PATH_OPEN_SCORE_BYTES as u64;
        let mut calibration = ResponseAckClockCalibrationState::new(initial, 4 * initial);
        for sample_bps in [90_000_000.0, 7_000_000_000.0, 110_000_000.0] {
            calibration.spent_bytes = calibration.credit_limit_bytes;
            let stage_authorized_at = calibration.stage_authorized_at;
            let sample =
                test_ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, sample_bps);
            let _ = calibration.record_ack_clock_sample(
                sample,
                stage_authorized_at + Duration::from_millis(1),
                stage_authorized_at + Duration::from_millis(10),
            );
        }
        assert_test_rate_close(calibration.calibrated_rate_bps, 110_000_000.0);
        outputs.ack_clock_calibrations.insert(identity, calibration);
        outputs.active_ack_clock_calibration = Some(identity);
        identity
    };
    let sample_bytes = 4096;
    binding.record_owner_flight(key, &stream_data_frame_at(0, sample_bytes));
    std::thread::sleep(Duration::from_millis(1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: sample_bytes as u64,
    }]);

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let entry = outputs
        .entries
        .iter()
        .find(|entry| (entry.key, entry.incarnation) == identity)
        .expect("TCP output");
    assert_test_rate_close(entry.product_progress_rate_bps, 110_000_000.0);
    assert_test_rate_close(entry.delivery_rate_bps, 110_000_000.0);
    assert!(entry.tcp_ack_clock_rate_bps.is_none());
    assert_eq!(entry.srtt_ms, Some(10.0));
    assert!(entry.tcp_product_rate_evidence.is_none());
    let prior = entry.tcp_capacity_prior.expect("robust capacity prior");
    assert_test_rate_close(Some(prior.rate_bps), 110_000_000.0);
    assert_eq!(prior.ordinary_windows, 0);
    assert_eq!(outputs.active_ack_clock_calibration, None);
    drop(outputs);

    let prior_entry = output_entry_for_key(&binding, key);
    let prior_snapshot = server_bulk_output_snapshot(
        &prior_entry,
        binding.session_id,
        FlowLane::Throughput,
        binding.lane_tracker.as_ref(),
        binding.mux_limits,
        Instant::now(),
    );
    assert_test_rate_close(Some(prior_snapshot.delivery_rate_bps), 110_000_000.0);
    assert_eq!(
        prior_snapshot.rate_scope,
        crate::scheduler::PathRateScope::PathCapacity
    );

    let fragment_bytes = (PATH_OPEN_SCORE_BYTES / (RELIABLE_INITIAL_WINDOW_PACKETS + 1)).max(1);
    let fragmented_ack_start = Instant::now() + Duration::from_millis(1);
    for fragment_index in 0..RELIABLE_INITIAL_WINDOW_PACKETS as u64 {
        let offset = sample_bytes as u64 + fragment_index * fragment_bytes as u64;
        binding.record_owner_flight(key, &stream_data_frame_at(offset, fragment_bytes));
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: offset,
                end: offset + fragment_bytes as u64,
            }],
            fragmented_ack_start + Duration::from_millis(fragment_index),
        );
    }
    let entry = output_entry_for_key(&binding, key);
    assert_eq!(
        entry
            .tcp_capacity_prior
            .expect("fragmented ACKs retain capacity prior")
            .ordinary_windows,
        0,
        "callbacks smaller than one exact ACK window are not evidence samples"
    );

    let ordinary_sample_bytes = PATH_OPEN_SCORE_BYTES;
    let ordinary_ack_start = Instant::now() + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
    for sample_index in 0..RELIABLE_INITIAL_WINDOW_PACKETS as u64 {
        let offset = sample_bytes as u64
            + RELIABLE_INITIAL_WINDOW_PACKETS as u64 * fragment_bytes as u64
            + sample_index * ordinary_sample_bytes as u64;
        binding.record_owner_flight(key, &stream_data_frame_at(offset, ordinary_sample_bytes));
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: offset,
                end: offset + ordinary_sample_bytes as u64,
            }],
            ordinary_ack_start
                + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED.mul_f64(sample_index as f64),
        );
        if sample_index + 1 < RELIABLE_INITIAL_WINDOW_PACKETS as u64 {
            let entry = output_entry_for_key(&binding, key);
            assert_test_rate_close(entry.delivery_rate_bps, 110_000_000.0);
        }
    }
    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let entry = outputs
        .entries
        .iter()
        .find(|entry| (entry.key, entry.incarnation) == identity)
        .expect("TCP output");
    assert!(entry.tcp_capacity_prior.is_none());
    assert!(
        entry
            .delivery_rate_bps
            .is_some_and(|rate_bps| rate_bps < 110_000_000.0),
        "mature ordinary exact-ACK evidence must replace the bounded capacity prior"
    );
    assert_eq!(entry.product_progress_rate_bps, entry.delivery_rate_bps);
    assert_eq!(entry.tcp_ack_clock_rate_bps, entry.delivery_rate_bps);
    assert_eq!(
        entry.srtt_ms,
        Some(10.0),
        "scheduler assignment time is not a TCP dispatch or RTT timestamp"
    );
    let ordinary_snapshot = server_bulk_output_snapshot(
        entry,
        binding.session_id,
        FlowLane::Throughput,
        binding.lane_tracker.as_ref(),
        binding.mux_limits,
        Instant::now(),
    );
    assert_eq!(
        ordinary_snapshot.rate_scope,
        crate::scheduler::PathRateScope::PerFlowGoodput
    );
}

#[test]
fn tcp_response_mixed_window_consumes_fresh_capacity_without_publishing_rate() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let window_bytes = PATH_OPEN_SCORE_BYTES;
    let identity = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs.entries.first().expect("TCP output");
        let identity = (entry.key, entry.incarnation);
        let mut calibration = ResponseAckClockCalibrationState::new(
            (2 * window_bytes) as u64,
            (2 * window_bytes) as u64,
        );
        calibration.spent_bytes = (2 * window_bytes) as u64;
        outputs.ack_clock_calibrations.insert(identity, calibration);
        outputs.active_ack_clock_calibration = Some(identity);
        identity
    };
    binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
    binding.record_owner_flight(key, &stream_data_frame_at(window_bytes as u64, 1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: window_bytes as u64,
    }]);

    binding.record_owner_flight(
        key,
        &stream_data_frame_at(window_bytes as u64 + 1, window_bytes.saturating_sub(1)),
    );
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: window_bytes as u64,
        end: (2 * window_bytes) as u64,
    }]);

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let calibration = outputs
        .ack_clock_calibrations
        .get(&identity)
        .expect("calibration state");
    assert_eq!(calibration.calibrated_rate_bps, None);
    assert!(
        calibration.proven,
        "the hard-capped stage cannot recover a representative strict window"
    );
    assert_eq!(
        outputs.active_ack_clock_calibration, None,
        "a terminal stage without causal evidence retires after exact flights drain"
    );
}
