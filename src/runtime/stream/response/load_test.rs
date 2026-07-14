use super::super::ResponseStreamBinding;
use super::super::session::ServerPathLaneTracker;
use super::super::topology::ResponseStreamAttachOutcome;
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{PathId, SessionId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::relay::io::reliable_relay_buffer_len;
use crate::scheduler::FlowLane;
use std::sync::Arc;

#[test]
fn active_response_flow_count_is_per_binding_not_per_attachment() {
    let session_id = SessionId(99);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let service_commands_for_detach = service_commands.clone();
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    let alternate_commands_for_detach = alternate_commands.clone();
    assert_eq!(
        binding.attach(
            alternate.underlay,
            alternate.path_id,
            alternate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 2);
    assert_eq!(
        binding.lane_generation_and_active_response_flows().1,
        1,
        "one response stream must contribute one flow despite two Active attachments"
    );

    binding.detach(service, &service_commands_for_detach);
    assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 1);
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
    binding.detach(alternate, &alternate_commands_for_detach);
    assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 0);
    assert_eq!(
        binding.lane_generation_and_active_response_flows().1,
        0,
        "a response stream with no Active attachment must not satisfy the gate"
    );
}

#[test]
fn passive_attachments_do_not_consume_or_release_shared_flow_load() {
    let session_id = SessionId(97);
    let service_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let shared_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service_key.underlay,
        service_key.path_id,
        service_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    let (shared_commands, _shared_receivers) = reliable_path_command_channels(8);
    let shared_binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        shared_key.underlay,
        shared_key.path_id,
        shared_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );

    let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
    let repair_commands_for_detach = repair_commands.clone();
    assert_eq!(
        binding.attach(
            shared_key.underlay,
            shared_key.path_id,
            repair_commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(
        lane_tracker.snapshot(session_id, shared_key).active_flows,
        1
    );
    binding.detach(shared_key, &repair_commands_for_detach);
    assert_eq!(
        lane_tracker.snapshot(session_id, shared_key).active_flows,
        1,
        "detaching passive Repair must not debit another stream's share"
    );

    let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);
    let validation_commands_for_promotion = validation_commands.clone();
    let validation_commands_for_repeat = validation_commands.clone();
    let validation_commands_for_detach = validation_commands.clone();
    assert_eq!(
        binding.attach(
            shared_key.underlay,
            shared_key.path_id,
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(
        lane_tracker.snapshot(session_id, shared_key).active_flows,
        1
    );
    assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 2);

    assert_eq!(
        binding.attach(
            shared_key.underlay,
            shared_key.path_id,
            validation_commands_for_promotion,
            FlowLane::Latency,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::RoleChanged
    );
    let service_load = lane_tracker.snapshot(session_id, service_key);
    assert_eq!(service_load.active_flows, 1);
    assert_eq!(service_load.active_latency_sensitive_flows, 1);
    let shared_load = lane_tracker.snapshot(session_id, shared_key);
    assert_eq!(shared_load.active_flows, 2);
    assert_eq!(
        shared_load.active_latency_sensitive_flows, 1,
        "promotion must add this stream in its new lane without moving the other stream"
    );

    assert_eq!(
        binding.attach(
            shared_key.underlay,
            shared_key.path_id,
            validation_commands_for_repeat,
            FlowLane::Latency,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let repeated_shared_load = lane_tracker.snapshot(session_id, shared_key);
    assert_eq!(repeated_shared_load.active_flows, shared_load.active_flows);
    assert_eq!(
        repeated_shared_load.active_latency_sensitive_flows,
        shared_load.active_latency_sensitive_flows
    );

    binding.detach(shared_key, &validation_commands_for_detach);
    let remaining_shared_load = lane_tracker.snapshot(session_id, shared_key);
    assert_eq!(remaining_shared_load.active_flows, 1);
    assert_eq!(remaining_shared_load.active_latency_sensitive_flows, 0);
    drop(binding);
    assert_eq!(
        lane_tracker.snapshot(session_id, service_key).active_flows,
        0
    );
    assert_eq!(
        lane_tracker.snapshot(session_id, shared_key).active_flows,
        1
    );
    drop(shared_binding);
    assert_eq!(
        lane_tracker.snapshot(session_id, shared_key).active_flows,
        0
    );
}

#[test]
fn closed_output_replacement_reconciles_role_flow_load() {
    let session_id = SessionId(98);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let (active_commands, active_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        key.underlay,
        key.path_id,
        active_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 1);
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
    drop(active_receivers);

    let (validation_commands, validation_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            validation_commands,
            FlowLane::Latency,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::ReplacedClosedOutput
    );
    assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 0);
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 0);
    drop(validation_receivers);

    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            replacement_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::ReplacedClosedOutput
    );
    let replacement_load = lane_tracker.snapshot(session_id, key);
    assert_eq!(replacement_load.active_flows, 1);
    assert_eq!(replacement_load.active_latency_sensitive_flows, 0);
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
    drop(binding);
    assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 0);
}
