use super::super::ResponseStreamBinding;
use super::super::evidence::{
    server_output_fresh_quic_capacity_proof, server_output_quic_capacity_proof_marker,
};
use super::super::handoff::ResponseServiceHandoffDrainRequest;
use super::super::session::ServerPathLaneTracker;
use super::super::test_support::{
    mark_test_quic_output_receipt_bulk_proven, mark_test_response_output_bulk_proven,
    stream_data_frame_at,
};
use super::ResponseServiceHandoffRequest;
use crate::model::path::CarrierPathKey;
use crate::model::response::{ResponseServiceFamilyLoads, ResponseServiceHandoffMode};
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathId, SessionId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::scheduler::FlowLane;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn exact_clear_frontier_handoff_pins_quic_proof_through_marker_expiry() {
    let mux_limits = MuxLimits::default();
    let session_id = SessionId(511);
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker.clone(),
    );
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    let _second_flow = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(2),
        second_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker,
    );
    let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        candidate.underlay,
        candidate.path_id,
        candidate_commands,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
    );
    let _ = try_recv_reliable_path_command(&mut candidate_receivers);
    let proof = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let mut proof = None;
        for entry in &mut outputs.entries {
            if entry.key == service {
                mark_test_response_output_bulk_proven(entry, mux_limits);
            } else if entry.key == candidate {
                proof = Some(mark_test_quic_output_receipt_bulk_proven(
                    entry,
                    mux_limits,
                    511,
                    Duration::from_millis(250),
                ));
            }
        }
        proof.expect("installed QUIC receipt proof")
    };
    let frontier = 4096;
    binding
        .ack_ordering
        .lock()
        .expect("test response ACK ordering lock")
        .contiguous_frontier = frontier;
    let (planner_generation, _) = binding.subflow_state_snapshot();
    let scheduling = binding.response_scheduling_snapshot();
    assert_eq!(
        scheduling.service_family_loads,
        ResponseServiceFamilyLoads::new(2, 0)
    );
    let model_generation = binding.response_model_generation();
    let targets = binding.sender_path_targets(FlowLane::Throughput, 64 * 1024);
    let service_target = targets
        .iter()
        .find(|target| target.observation.key == service)
        .expect("TCP Service target")
        .clone();
    let candidate_target = targets
        .iter()
        .find(|target| target.observation.key == candidate)
        .expect("measured QUIC target")
        .clone();
    let frame = stream_data_frame_at(frontier, 64 * 1024);
    let request = ResponseServiceHandoffRequest {
        expected_planner_generation: planner_generation,
        expected_lane_generation: scheduling.generation,
        expected_model_generation: model_generation,
        handoff_frontier: frontier,
        service,
        service_path_instance_id: service_target.observation.path_instance_id,
        service_incarnation: service_target.observation.incarnation,
        target: candidate,
        target_path_instance_id: candidate_target.observation.path_instance_id,
        target_incarnation: candidate_target.observation.incarnation,
        mode: ResponseServiceHandoffMode::Diversification,
        target_command_pending_limit_bytes: u64::MAX,
        capacity_proof: Some(proof),
    };
    assert!(matches!(
        binding.try_enqueue_response_service_handoff(
            &candidate_target,
            &frame,
            FlowLane::Throughput,
            ResponseServiceHandoffRequest {
                expected_model_generation: model_generation.wrapping_sub(1),
                ..request
            },
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(binding.ordered_data_owner(), Some(service));
    assert_eq!(
        binding.response_scheduling_snapshot().service_family_loads,
        ResponseServiceFamilyLoads::new(2, 0),
        "a stale handoff must not reserve or move session Service load"
    );
    assert!(binding.try_start_response_service_handoff_drain(
        &service_target,
        &candidate_target,
        FlowLane::Throughput,
        ResponseServiceHandoffDrainRequest {
            expected_planner_generation: planner_generation,
            expected_lane_generation: request.expected_lane_generation,
            expected_model_generation: model_generation,
            service,
            service_path_instance_id: service_target.observation.path_instance_id,
            service_incarnation: service_target.observation.incarnation,
            target: candidate,
            target_path_instance_id: candidate_target.observation.path_instance_id,
            target_incarnation: candidate_target.observation.incarnation,
            mode: ResponseServiceHandoffMode::Diversification,
            capacity_proof: Some(proof),
            outstanding_owner_bytes: 64 * 1024,
            lease: Duration::from_secs(1),
        },
    ));
    let drained_scheduling = binding.response_scheduling_snapshot();
    assert!(drained_scheduling.response_service_handoff_drain.is_some());
    std::thread::sleep(
        proof
            .expires_at
            .saturating_duration_since(Instant::now())
            .saturating_add(Duration::from_millis(10)),
    );
    assert!(
        binding
            .outputs
            .lock()
            .expect("test response outputs lock")
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .is_some_and(|entry| {
                server_output_quic_capacity_proof_marker(entry) == Some(proof)
                    && server_output_fresh_quic_capacity_proof(entry).is_none()
            }),
        "the raw marker remains observable after ordinary authority expires"
    );
    let candidate_target = binding
        .sender_path_targets(FlowLane::Throughput, 64 * 1024)
        .into_iter()
        .find(|target| target.observation.key == candidate)
        .expect("reserved QUIC target after marker expiry");
    assert!(!candidate_target.observation.has_bulk_rate_evidence);
    binding
        .try_enqueue_response_service_handoff(
            &candidate_target,
            &frame,
            FlowLane::Throughput,
            ResponseServiceHandoffRequest {
                expected_lane_generation: drained_scheduling.generation,
                ..request
            },
        )
        .expect("exact drained frontier should commit one sticky handoff");

    assert_eq!(binding.ordered_data_owner(), Some(candidate));
    assert!(!binding.response_service_handoff_open());
    assert!(
        binding
            .response_scheduling_snapshot()
            .response_service_handoff_drain
            .is_none(),
        "the matching drain intent must be consumed with the Service move"
    );
    assert_eq!(
        binding.response_scheduling_snapshot().service_family_loads,
        ResponseServiceFamilyLoads::new(1, 1)
    );
    assert_eq!(
        binding
            .lane_tracker
            .response_service_snapshot(session_id, service)
            .active_flows,
        0,
        "the old Active attachment must not retain response Service pressure"
    );
    assert_eq!(
        binding
            .lane_tracker
            .response_service_snapshot(session_id, candidate)
            .active_flows,
        1
    );
    let moved_targets = binding.sender_path_targets(FlowLane::Throughput, 64 * 1024);
    assert_eq!(
        moved_targets
            .iter()
            .find(|target| target.observation.key == service)
            .expect("old TCP attachment")
            .observation
            .snapshot
            .active_flows,
        0
    );
    assert_eq!(
        moved_targets
            .iter()
            .find(|target| target.observation.key == candidate)
            .expect("new QUIC Service")
            .observation
            .snapshot
            .active_flows,
        1
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset,
            ..
        })) if offset == frontier
    ));
}
