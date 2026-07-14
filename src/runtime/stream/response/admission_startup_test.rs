use super::super::ack_clock::RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
use super::super::evidence::ServerPathMetricsSource;
use super::super::test_support::{
    binding_for_underlay, output_entry_for_key, stream_data_frame, stream_data_frame_at,
};
use super::super::topology::ResponseStreamAttachOutcome;
use super::server_output_has_bulk_rate_evidence;
use crate::model::capacity::{
    BBR_MAX_SEND_QUANTUM_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS,
    reliable_subflow_startup_sample_limit_bytes,
};
use crate::model::multipath::{FlowSubflowSet, PathAdmissionDecision, SubflowAdmissionInput};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{
    OffsetRange, PathId, PathMetricDirection, PathMetrics, StreamOpenRole, UnderlayProtocol,
};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::path::model::metric_epoch_now;
use crate::runtime::relay::io::reliable_relay_buffer_len;
use crate::scheduler::FlowLane;
use std::time::{Duration, Instant};

#[test]
fn later_owner_ack_window_proves_tcp_but_not_udp_without_carrier_evidence() {
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(MuxLimits::default());
    let frame_bytes = BBR_MAX_SEND_QUANTUM_BYTES as u64;
    assert_eq!(sample_bytes % frame_bytes, 0);

    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let (binding, key) = binding_for_underlay(underlay);
        for offset in (0..2 * sample_bytes).step_by(BBR_MAX_SEND_QUANTUM_BYTES) {
            binding.record_owner_flight(
                key,
                &stream_data_frame_at(offset, BBR_MAX_SEND_QUANTUM_BYTES),
            );
        }
        let first_ack = Instant::now() + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: 0,
                end: sample_bytes,
            }],
            first_ack,
        );
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: sample_bytes,
                end: 2 * sample_bytes,
            }],
            first_ack + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED,
        );

        let entry = output_entry_for_key(&binding, key);
        assert_eq!(
            entry.owner_data_acked_bytes,
            2 * sample_bytes,
            "{underlay:?}"
        );
        assert!(entry.product_progress_rate_bps.is_some(), "{underlay:?}");
        assert_eq!(
            server_output_has_bulk_rate_evidence(&entry),
            underlay == UnderlayProtocol::Tcp,
            "TCP may use product owner ACKs; QUIC requires local carrier bulk evidence"
        );
    }
}

#[test]
fn tcp_response_startup_ack_graduates_epoch_and_admits_next_candidate() {
    let limits = MuxLimits::default();
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(limits) as usize;
    let (binding, service) = binding_for_underlay(UnderlayProtocol::Tcp);
    let first = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let second = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(2),
    };
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            first.underlay,
            first.path_id,
            first_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(
        binding.attach(
            second.underlay,
            second.path_id,
            second_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let startup_input = |key| SubflowAdmissionInput {
        key,
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: sample_bytes,
        optional_overhead_bytes: 0,
    };

    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
                0,
                Duration::ZERO,
                startup_input(first),
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
                0,
                Duration::ZERO,
                startup_input(second),
            )
            .decision,
        PathAdmissionDecision::ProbeOnly,
        "only one unproven response candidate may own startup bytes"
    );
    let generation_before_ack = binding.subflow_state_snapshot().0;
    binding.record_owner_flight(first, &stream_data_frame(sample_bytes));
    std::thread::sleep(Duration::from_millis(1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: sample_bytes as u64,
    }]);

    let (generation_after_ack, epoch) = binding.subflow_state_snapshot();
    assert_ne!(generation_after_ack, generation_before_ack);
    assert_eq!(
        epoch.as_ref().and_then(FlowSubflowSet::startup_owner_key),
        None,
        "exact TCP OwnerData ACK evidence should graduate the sampled response path"
    );
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == first)
            .expect("graduated TCP output remains attached");
        assert!(
            outputs
                .ack_clock_calibrations
                .contains_key(&(entry.key, entry.incarnation)),
            "TCP graduation creates an exact-incarnation ACK-clock phase"
        );
    }
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
                0,
                Duration::ZERO,
                startup_input(second),
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    assert_eq!(
        binding
            .subflow_state_snapshot()
            .1
            .and_then(|epoch| epoch.startup_owner_key()),
        Some(second)
    );
}

#[test]
fn udp_response_startup_requires_local_carrier_bulk_evidence_to_graduate() {
    let limits = MuxLimits::default();
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(limits) as usize;
    let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
    let first = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let second = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            first.underlay,
            first.path_id,
            first_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(
        binding.attach(
            second.underlay,
            second.path_id,
            second_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let startup_input = |key| SubflowAdmissionInput {
        key,
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: sample_bytes,
        optional_overhead_bytes: 0,
    };

    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
                0,
                Duration::ZERO,
                startup_input(first),
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    let generation_before_ack = binding.subflow_state_snapshot().0;
    binding.record_owner_flight(first, &stream_data_frame(sample_bytes));
    std::thread::sleep(Duration::from_millis(1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: sample_bytes as u64,
    }]);

    let (generation_after_ack, epoch) = binding.subflow_state_snapshot();
    assert_eq!(generation_after_ack, generation_before_ack);
    assert_eq!(
        epoch.as_ref().and_then(FlowSubflowSet::startup_owner_key),
        Some(first),
        "UDP product ACKs alone must not graduate a QUIC response Subflow"
    );
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
                0,
                Duration::ZERO,
                startup_input(second),
            )
            .decision,
        PathAdmissionDecision::ProbeOnly
    );

    binding.update_path_metrics(
        first,
        PathMetrics {
            path_id: first.path_id,
            underlay: first.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 80_000,
            srtt_us: 80_000,
            rttvar_us: 5_000,
            jitter_us: 5_000,
            delivery_rate_bps: 200_000_000,
            pacing_rate_bps: 200_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: sample_bytes as u64,
            inflight_hi_bytes: sample_bytes as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            data_sample_bytes: sample_bytes as u64,
        },
        ServerPathMetricsSource::LocalSender,
    );

    let (generation_after_carrier_proof, epoch) = binding.subflow_state_snapshot();
    assert_ne!(generation_after_carrier_proof, generation_after_ack);
    assert_eq!(
        epoch.as_ref().and_then(FlowSubflowSet::startup_owner_key),
        None
    );
    assert!(
        binding
            .outputs
            .lock()
            .expect("test response outputs lock")
            .ack_clock_calibrations
            .is_empty(),
        "UDP/QUIC graduation remains carrier-owned and never enters TCP calibration"
    );
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
                0,
                Duration::ZERO,
                startup_input(second),
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
}
