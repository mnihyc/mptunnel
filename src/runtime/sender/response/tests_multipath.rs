use super::*;
use crate::model::capacity::reliable_path_startup_sample_limit_bytes;
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, OffsetRange, PathId, SessionId, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::stream::response::ResponseStreamBinding;
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;

fn data_frame(offset: u64, payload_bytes: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(17),
        offset,
        payload: Bytes::from(vec![0x5a; payload_bytes]),
    }
}

fn stream_with_output(output: ReliablePathStreamOutput) -> ReliablePathStream {
    let (_frames_tx, frames) = mpsc::channel(1);
    ReliablePathStream {
        stream_id: StreamId(17),
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
        output,
        frames: frames.into(),
    }
}

fn switchable_binding(
    limits: MuxLimits,
) -> (
    Arc<ResponseStreamBinding>,
    CarrierPathKey,
    crate::runtime::path::commands::ReliablePathCommandReceivers,
) {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (commands, receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(17),
        key.underlay,
        key.path_id,
        commands,
        TrafficClass::Throughput,
        limits,
    );
    (binding, key, receivers)
}

#[test]
fn fixed_output_plan_uses_the_data_traffic_class_queue() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let stream = stream_with_output(ReliablePathStreamOutput::fixed(
        UnderlayProtocol::Tcp,
        PathId(3),
        commands.clone(),
        MuxLimits::default(),
    ));

    let plan = plan_response_data_dispatch(&stream, TrafficClass::Throughput, 0, 1024)
        .expect("empty carrier queue admits data");
    assert!(matches!(
        plan,
        ResponseDataDispatchTarget::Fixed {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(3),
            }
        }
    ));

    commands
        .try_enqueue_stream_ordered_frame(data_frame(1024, 1024), TrafficClass::Throughput)
        .expect("fill the bulk queue slot");
    assert!(matches!(
        plan_response_data_dispatch(&stream, TrafficClass::Throughput, 0, 1024),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        plan_response_data_dispatch(&stream, TrafficClass::Latency, 0, 1024).is_ok(),
        "latency response data retains independent priority capacity",
    );
    assert!(
        crate::runtime::path::commands::try_recv_reliable_path_command(&mut receivers).is_some()
    );
}

#[test]
fn fixed_output_product_window_blocks_at_exact_debt_and_data_ack_reopens() {
    let (commands, _receivers) = reliable_path_command_channels(4);
    let stream = stream_with_output(ReliablePathStreamOutput::fixed(
        UnderlayProtocol::Tcp,
        PathId(3),
        commands,
        MuxLimits::default(),
    ));
    let product_limit = stream
        .output
        .send_path_snapshot(TrafficClass::Throughput, 1024)
        .expect("fixed output snapshot")
        .data_level_limit_bytes;
    assert!(product_limit > 0);
    let flight = data_frame(
        0,
        usize::try_from(product_limit).expect("test Product limit"),
    );
    let ReliablePathStreamOutput::Fixed(fixed) = &stream.output else {
        panic!("expected fixed output");
    };
    fixed.record_original_flight(&flight);

    assert!(matches!(
        plan_response_data_dispatch(&stream, TrafficClass::Throughput, product_limit, 1024),
        Err(RuntimeError::SenderServiceBlocked)
    ));

    stream
        .output
        .release_normalized_acked_ranges(&[crate::protocol::OffsetRange {
            start: 0,
            end: product_limit,
        }]);
    assert!(
        plan_response_data_dispatch(&stream, TrafficClass::Throughput, product_limit, 1024).is_ok(),
        "exact MPP Data ACK release restores Product assignment authority",
    );
}

#[test]
fn switchable_plan_uses_completion_time_not_attachment_order() {
    let (binding, initial, _initial_receivers) = switchable_binding(MuxLimits::default());
    let alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            alternate.underlay,
            alternate.path_id,
            alternate_commands,
            TrafficClass::Throughput,
        ),
        crate::runtime::stream::response::ResponseStreamAttachOutcome::Attached
    );
    binding.mark_output_path_proven_for_test(initial);
    binding.mark_output_path_proven_for_test(alternate);
    let qualification_floor = reliable_path_startup_sample_limit_bytes(MuxLimits::default());
    binding.record_original_flight(
        initial,
        &data_frame(
            0,
            usize::try_from(qualification_floor).expect("test qualification floor"),
        ),
    );
    binding.record_original_flight(
        alternate,
        &data_frame(
            qualification_floor,
            usize::try_from(qualification_floor).expect("test qualification floor"),
        ),
    );
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: qualification_floor * 2,
    }]);
    binding.set_output_product_model_for_test(initial, 10_000_000.0, 100.0);
    binding.set_output_product_model_for_test(alternate, 500_000_000.0, 10.0);
    let stream = stream_with_output(ReliablePathStreamOutput::Switchable(binding));

    let plan = plan_response_data_dispatch(&stream, TrafficClass::Throughput, 0, 64 * 1024)
        .expect("a live output is schedulable");
    assert!(matches!(
        plan,
        ResponseDataDispatchTarget::Switchable { target, .. } if target.key == alternate
    ));
}

#[test]
fn full_connection_receive_window_blocks_every_path() {
    let limits = MuxLimits {
        max_reorder_bytes: 128 * 1024,
        max_stream_window_bytes: 128 * 1024,
        ..MuxLimits::default()
    };
    let (binding, initial, _initial_receivers) = switchable_binding(limits);
    let alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        alternate.underlay,
        alternate.path_id,
        alternate_commands,
        TrafficClass::Throughput,
    );
    binding.set_output_product_model_for_test(initial, 10_000_000.0, 100.0);
    binding.set_output_product_model_for_test(alternate, 500_000_000.0, 1.0);
    binding.record_original_flight(initial, &data_frame(0, 128 * 1024));
    let stream = stream_with_output(ReliablePathStreamOutput::Switchable(binding));

    let result = plan_response_data_dispatch_with_data_ack_outstanding_impl(
        &stream,
        TrafficClass::Throughput,
        128 * 1024,
        64 * 1024,
        128 * 1024,
    );
    assert!(matches!(result, Err(RuntimeError::SenderServiceBlocked)));
}

#[test]
fn replacement_gets_bounded_progress_without_inheriting_old_flight() {
    let (binding, initial, initial_receivers) = switchable_binding(MuxLimits::default());
    let old_incarnation = binding.sender_path_targets(TrafficClass::Throughput, 4096)[0]
        .observation
        .incarnation;
    binding.record_original_flight(initial, &data_frame(0, 4096));
    drop(initial_receivers);

    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            initial.underlay,
            initial.path_id,
            replacement_commands,
            TrafficClass::Throughput,
        ),
        crate::runtime::stream::response::ResponseStreamAttachOutcome::ReplacedClosedOutput
    );
    let stream = stream_with_output(ReliablePathStreamOutput::Switchable(binding.clone()));

    let first = plan_response_data_dispatch_with_data_ack_outstanding_impl(
        &stream,
        TrafficClass::Throughput,
        4096,
        4096,
        4096,
    )
    .expect("replacement receives bounded work while the old range remains debt");
    assert!(matches!(
        first,
        ResponseDataDispatchTarget::Switchable {
            target,
            position: BulkCandidatePosition::AdditionalPath,
            ..
        }
            if target.key == initial && target.incarnation != old_incarnation
    ));

    binding.release_normalized_acked_ranges(&[crate::protocol::OffsetRange {
        start: 0,
        end: 2048,
    }]);
    let partial = plan_response_data_dispatch_with_data_ack_outstanding_impl(
        &stream,
        TrafficClass::Throughput,
        4096,
        4096,
        2048,
    )
    .expect("partial Data ACK does not create stop-and-wait failover");
    assert!(matches!(
        partial,
        ResponseDataDispatchTarget::Switchable {
            target,
            position: BulkCandidatePosition::AdditionalPath,
            ..
        }
            if target.key == initial && target.incarnation != old_incarnation
    ));

    binding.release_normalized_acked_ranges(&[crate::protocol::OffsetRange {
        start: 2048,
        end: 4096,
    }]);
    assert!(
        plan_response_data_dispatch_with_data_ack_outstanding_impl(
            &stream,
            TrafficClass::Throughput,
            4096,
            4096,
            0,
        )
        .is_ok(),
        "full Data ACK retirement removes the old incarnation's ordering debt",
    );
}

#[test]
fn readiness_preview_does_not_mutate_generation_or_queue() {
    let (binding, _initial, _receivers) = switchable_binding(MuxLimits::default());
    let stream = stream_with_output(ReliablePathStreamOutput::Switchable(binding.clone()));
    let generation = binding.response_model_generation();
    let before = binding.sender_path_targets(TrafficClass::Throughput, 4096);

    assert!(preview_response_data_payload_with_data_ack_outstanding(
        &stream,
        TrafficClass::Throughput,
        0,
        4096,
        0,
    ));

    assert_eq!(binding.response_model_generation(), generation);
    let after = binding.sender_path_targets(TrafficClass::Throughput, 4096);
    assert_eq!(before.len(), after.len());
    assert_eq!(
        before[0].can_enqueue_stream_data(TrafficClass::Throughput),
        after[0].can_enqueue_stream_data(TrafficClass::Throughput)
    );
}

#[test]
fn switchable_plan_carries_the_observed_model_generation() {
    let (binding, _initial, _receivers) = switchable_binding(MuxLimits::default());
    let stream = stream_with_output(ReliablePathStreamOutput::Switchable(binding.clone()));
    let expected = binding.response_model_generation();

    let plan = plan_response_data_dispatch(&stream, TrafficClass::Throughput, 0, 4096)
        .expect("initial output is schedulable");
    let ResponseDataDispatchTarget::Switchable {
        expected_model_generation,
        ..
    } = plan
    else {
        panic!("expected switchable target");
    };
    assert_eq!(expected_model_generation, expected);
}
