use super::ResponseStreamBinding;
use super::ack_clock::{RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED, ResponseAckClockCalibrationState};
use super::attachment::ResponseStreamAttachOutcome;
use super::evidence::ServerPathMetricsSource;
use super::subflow::{
    server_output_accepts_service_capacity_prior, server_output_has_bulk_rate_evidence_with_limits,
};
use super::test_support::{
    assert_test_rate_close, binding_for_underlay, output_entry_for_key, stream_data_frame,
    stream_data_frame_at,
};
use crate::model::capacity::{
    BBR_MAX_SEND_QUANTUM_BYTES, BBR_MIN_SEND_QUANTUM_PACKETS, MIN_RATE_SAMPLE_BYTES,
    PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS, TRANSPORT_MSS_BYTES,
    adaptive_reliable_relay_chunk_bytes, reliable_subflow_startup_sample_limit_bytes,
};
use crate::model::multipath::{PathAdmission, SubflowAdmissionInput};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
use crate::protocol::{
    OffsetRange, PathId, PathMetricDirection, PathMetrics, SessionId, StreamOpenRole,
    UnderlayProtocol,
};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::path::model::metric_epoch_now;
use crate::scheduler::{FlowLane, PathRateScope};
use std::time::{Duration, Instant};

#[test]
fn tcp_response_active_calibration_remainder_honors_state_boundaries() {
    let (binding, _key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let stage_limit = (2 * 1024 * 1024) as u64;
    let residual = 4032_u64;
    let identity = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs.entries.first().expect("TCP output");
        let identity = (entry.key, entry.incarnation);
        let mut calibration = ResponseAckClockCalibrationState::new(stage_limit, 4 * stage_limit);
        calibration.spent_bytes = stage_limit - residual;
        outputs.ack_clock_calibrations.insert(identity, calibration);
        outputs.active_ack_clock_calibration = Some(identity);
        identity
    };

    assert_eq!(
        binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        Some(residual as usize),
    );

    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs
            .ack_clock_calibrations
            .get_mut(&identity)
            .expect("calibration state")
            .spent_bytes = stage_limit;
    }
    assert_eq!(
        binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        None,
        "an exhausted stage returns to Service while it awaits ACK evidence",
    );

    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let calibration = outputs
            .ack_clock_calibrations
            .get_mut(&identity)
            .expect("calibration state");
        calibration.spent_bytes = stage_limit - 1;
    }
    assert_eq!(
        binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        Some(1),
        "a one-byte residual must not be expanded to a minimum quantum",
    );

    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs
            .ack_clock_calibrations
            .get_mut(&identity)
            .expect("calibration state")
            .proven = true;
    }
    assert_eq!(
        binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        None
    );
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let calibration = outputs
            .ack_clock_calibrations
            .get_mut(&identity)
            .expect("calibration state");
        calibration.proven = false;
        calibration.retired = true;
    }
    assert_eq!(
        binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        None
    );
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs.active_ack_clock_calibration = Some((
            CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(99),
            },
            99,
        ));
    }
    assert_eq!(
        binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        None
    );

    let (udp_binding, _udp_key) = binding_for_underlay(UnderlayProtocol::Udp);
    {
        let mut outputs = udp_binding
            .outputs
            .lock()
            .expect("test response outputs lock");
        let entry = outputs.entries.first().expect("UDP output");
        let identity = (entry.key, entry.incarnation);
        outputs.ack_clock_calibrations.insert(
            identity,
            ResponseAckClockCalibrationState::new(stage_limit, 4 * stage_limit),
        );
        outputs.active_ack_clock_calibration = Some(identity);
    }
    assert_eq!(
        udp_binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        None,
        "QUIC/UDP product frames stay under the carrier-local controller",
    );
}

#[test]
fn endpoint_only_tcp_startup_uses_service_capacity_prior_without_exclusive_calibration() {
    let limits = MuxLimits::default();
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(limits) as usize;
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
    );
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.mark_output_bulk_proven_for_test(service);
    binding.update_path_metrics(
        candidate,
        PathMetrics {
            path_id: candidate.path_id,
            underlay: candidate.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            srtt_us: 333_000,
            rttvar_us: 0,
            jitter_us: 0,
            delivery_rate_bps: 1,
            pacing_rate_bps: 1,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: 0,
            inflight_hi_bytes: 0,
            confidence_ppm: 0,
            app_limited: true,
            has_ack_derived_data_sample: false,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
        ServerPathMetricsSource::PeerHint,
    );
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let service_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == service)
            .expect("Service output");
        let candidate_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("candidate output");
        assert!(server_output_has_bulk_rate_evidence_with_limits(
            service_entry,
            limits
        ));
        assert!(server_output_accepts_service_capacity_prior(
            candidate_entry
        ));
    }
    assert_eq!(
        binding.commit_subflow_owner_admission(
            service,
            sample_bytes,
            SubflowAdmissionInput {
                key: candidate,
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                owner_bytes: sample_bytes,
            },
        ),
        PathAdmission::Subflow
    );
    binding.record_owner_flight(candidate, &stream_data_frame(sample_bytes));
    std::thread::sleep(Duration::from_millis(1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: sample_bytes as u64,
    }]);
    assert_eq!(
        binding
            .subflow_state_snapshot()
            .1
            .and_then(|epoch| epoch.startup_owner_key()),
        None,
        "exact startup drain must graduate before installing a capacity prior"
    );

    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("graduated candidate output");
        assert!(
            server_output_accepts_service_capacity_prior(entry),
            "post-startup eligibility: delivery_samples={} local_ack={} peer_app_limited={} peer_ack={} calibrations={}",
            entry.delivery_samples,
            entry
                .local_path_metrics
                .is_some_and(|metrics| metrics.metrics.has_ack_derived_data_sample),
            entry
                .peer_path_metrics
                .is_some_and(|metrics| metrics.metrics.app_limited),
            entry
                .peer_path_metrics
                .is_some_and(|metrics| metrics.metrics.has_ack_derived_data_sample),
            outputs.ack_clock_calibrations.len(),
        );
        let prior = entry
            .tcp_capacity_prior
            .expect("Service opportunity capacity prior");
        assert_test_rate_close(Some(prior.rate_bps), 100_000_000.0);
        assert_eq!(prior.ordinary_windows, 0);
        assert!(outputs.ack_clock_calibrations.is_empty());
    }
    let target = binding
        .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
        .into_iter()
        .find(|target| target.observation.key == candidate)
        .expect("graduated endpoint-only candidate");
    assert!(target.observation.has_bulk_rate_evidence);
    assert!(!target.ack_clock_calibration_eligible);
    assert_eq!(
        target.observation.snapshot.rate_scope,
        PathRateScope::PathCapacity
    );
    assert_test_rate_close(
        Some(target.observation.snapshot.delivery_rate_bps),
        100_000_000.0,
    );
}

#[test]
fn tcp_response_graduation_skips_calibration_below_ack_sample_resource_floor() {
    let limits = MuxLimits {
        max_path_flight_bytes: PATH_OPEN_SCORE_BYTES - 1,
        ..MuxLimits::default()
    };
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(limits) as usize;
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(43),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        limits,
    );
    let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(
        binding.commit_subflow_owner_admission(
            service,
            sample_bytes,
            SubflowAdmissionInput {
                key: candidate,
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                owner_bytes: sample_bytes,
            },
        ),
        PathAdmission::Subflow
    );
    binding.record_owner_flight(candidate, &stream_data_frame(sample_bytes));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: sample_bytes as u64,
    }]);
    assert_eq!(
        binding
            .subflow_state_snapshot()
            .1
            .and_then(|epoch| epoch.startup_owner_key()),
        None,
        "a fully ACKed TCP startup sample may graduate without inventing a rate"
    );
    let candidate_target = binding
        .sender_path_targets(FlowLane::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key == candidate)
        .expect("graduated candidate target");
    assert!(!candidate_target.ack_clock_calibration_eligible);
}

#[test]
fn tcp_local_sender_metrics_remain_send_quantum_prior_after_low_product_sample() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let mux_limits = binding.mux_limits();
    let metrics = PathMetrics {
        path_id: key.path_id,
        underlay: key.underlay,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        srtt_us: 20_000,
        rttvar_us: 1_000,
        jitter_us: 1_000,
        delivery_rate_bps: 500_000_000,
        pacing_rate_bps: 500_000_000,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: true,
        ecn_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: 0,
        inflight_hi_bytes: 0,
        confidence_ppm: 0,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        data_sample_bytes: MIN_RATE_SAMPLE_BYTES,
    };
    binding.update_path_metrics(key, metrics, ServerPathMetricsSource::LocalSender);

    let before_ack = binding
        .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
        .expect("path metrics seed response path snapshot");
    assert_eq!(before_ack.delivery_rate_bps, 500_000_000.0);

    let frame = stream_data_frame(MIN_RATE_SAMPLE_BYTES as usize);
    let later = stream_data_frame_at(MIN_RATE_SAMPLE_BYTES, MIN_RATE_SAMPLE_BYTES as usize);
    binding.record_owner_flight(key, &frame);
    binding.record_owner_flight(key, &later);
    let first_ack = Instant::now() + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: 0,
            end: reliable_stream_frame_accounted_bytes(&frame) as u64,
        }],
        first_ack,
    );
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: MIN_RATE_SAMPLE_BYTES,
            end: 2 * MIN_RATE_SAMPLE_BYTES,
        }],
        first_ack + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED,
    );

    let entry = output_entry_for_key(&binding, key);
    assert_eq!(entry.delivery_samples, 2);
    assert!(
        entry.delivery_rate_bps.unwrap_or(f64::INFINITY) < 500_000_000.0,
        "the test must create a low product progress sample"
    );
    let after_ack = binding
        .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
        .expect("peer rate prior remains available after product ACK sample");
    assert_eq!(after_ack.delivery_rate_bps, 500_000_000.0);
    assert!(
        adaptive_reliable_relay_chunk_bytes(Some(after_ack), FlowLane::Throughput, mux_limits)
            > BBR_MIN_SEND_QUANTUM_PACKETS * TRANSPORT_MSS_BYTES,
        "a low product ACK sample must not collapse TCP send quantum below the path-rate prior"
    );
}
