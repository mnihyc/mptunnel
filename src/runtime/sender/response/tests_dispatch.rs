use super::*;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, OffsetRange, PathId, SessionId, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, reliable_path_command_channels,
    try_recv_reliable_path_command,
};
use crate::runtime::sender::response::multipath::plan_response_data_dispatch;
use crate::runtime::stream::response::{ResponseStreamAttachOutcome, ResponseStreamBinding};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

fn data_frame(offset: u64, payload_bytes: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(23),
        offset,
        payload: Bytes::from(vec![0x5a; payload_bytes]),
    }
}

fn stream_with_output(output: ReliablePathStreamOutput) -> ReliablePathStream {
    let (_frames_tx, frames) = mpsc::channel(1);
    ReliablePathStream {
        stream_id: StreamId(23),
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
        output,
        frames: frames.into(),
    }
}

struct SwitchableFixture {
    binding: Arc<ResponseStreamBinding>,
    stream: ReliablePathStream,
    initial: crate::model::path::CarrierPathKey,
    initial_receivers: ReliablePathCommandReceivers,
}

fn switchable_fixture() -> SwitchableFixture {
    let initial = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (commands, initial_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(23),
        initial.underlay,
        initial.path_id,
        commands,
        TrafficClass::Throughput,
    );
    let stream = stream_with_output(ReliablePathStreamOutput::Switchable(binding.clone()));
    SwitchableFixture {
        binding,
        stream,
        initial,
        initial_receivers,
    }
}

fn assert_data_command(receivers: &mut ReliablePathCommandReceivers, offset: u64) {
    assert!(matches!(
        try_recv_reliable_path_command(receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: actual,
            ..
        })) if actual == offset
    ));
}

fn output_identity(
    binding: &ResponseStreamBinding,
    key: crate::model::path::CarrierPathKey,
) -> (crate::model::path::CarrierPathKey, u64) {
    binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key == key)
        .map(|target| (key, target.observation.incarnation))
        .expect("attached response output")
}

#[test]
fn fixed_data_commit_records_flight_before_publishing_command() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let stream = stream_with_output(ReliablePathStreamOutput::fixed(
        UnderlayProtocol::Tcp,
        PathId(4),
        commands,
        MuxLimits::default(),
    ));
    let frame = data_frame(0, 2048);
    let plan = plan_response_data_dispatch(&stream, TrafficClass::Throughput, 0, 2048)
        .expect("fixed output has queue credit");

    let selected_path =
        emit_planned_response_data_frame(&stream, plan, frame, TrafficClass::Throughput)
            .expect("fixed data commit");

    assert_eq!(selected_path.map(|key| key.path_id), Some(PathId(4)));
    assert_data_command(&mut receivers, 0);
}

#[test]
fn fixed_data_commit_rechecks_product_headroom_after_plan() {
    let (commands, mut receivers) = reliable_path_command_channels(2);
    let stream = stream_with_output(ReliablePathStreamOutput::fixed(
        UnderlayProtocol::Tcp,
        PathId(4),
        commands,
        MuxLimits::default(),
    ));
    let plan = plan_response_data_dispatch(&stream, TrafficClass::Throughput, 0, 1024)
        .expect("initial Product window has headroom");
    let ReliablePathStreamOutput::Fixed(fixed) = &stream.output else {
        panic!("expected fixed output");
    };
    let product_limit = stream
        .output
        .send_path_snapshot(TrafficClass::Throughput, 1024)
        .expect("fixed output snapshot")
        .data_level_limit_bytes;
    fixed.record_original_flight(&data_frame(
        0,
        usize::try_from(product_limit).expect("test Product limit"),
    ));

    assert!(matches!(
        emit_planned_response_data_frame(
            &stream,
            plan,
            data_frame(product_limit, 1024),
            TrafficClass::Throughput,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
}

#[test]
fn fixed_latency_data_dispatch_overtakes_queued_bulk() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(data_frame(4096, 1024), TrafficClass::Throughput)
        .expect("queue bulk response data");
    let stream = stream_with_output(ReliablePathStreamOutput::fixed(
        UnderlayProtocol::Tcp,
        PathId(4),
        commands,
        MuxLimits::default(),
    ));
    let frame = data_frame(0, 128);
    let plan = plan_response_data_dispatch(&stream, TrafficClass::Latency, 0, 128)
        .expect("latency response has priority capacity");

    emit_planned_response_data_frame(&stream, plan, frame, TrafficClass::Latency)
        .expect("dispatch latency response data");

    assert_data_command(&mut receivers, 0);
    assert_data_command(&mut receivers, 4096);
}

#[test]
fn fixed_reinjection_remains_charged_after_queue_drain_until_data_ack() {
    let mux_limits = MuxLimits {
        max_repair_bytes: 4096,
        max_path_flight_bytes: 4096,
        ..MuxLimits::default()
    };
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let stream = stream_with_output(ReliablePathStreamOutput::fixed(
        UnderlayProtocol::Tcp,
        PathId(4),
        commands,
        mux_limits,
    ));
    let ReliablePathStreamOutput::Fixed(fixed) = &stream.output else {
        panic!("expected fixed output");
    };
    fixed.record_reinjected_flight(&data_frame(0, 4096));
    let identity = fixed.reinjection_output_identity();
    assert_eq!(
        fixed.accepted_reinjected_data_in_flight_bytes_at(identity),
        4096,
    );

    // The sender queue has already drained this accepted repair. Its exact
    // fixed output still owns K until Product DataACK releases the range.
    let queue = ReliableRelaySenderQueue::default();
    let retry = data_frame(4096, 1);
    let emit = || {
        emit_response_frame_from_sender_service(
            &stream,
            retry.clone(),
            TrafficClass::Throughput,
            CarrierEmitMode::Classified,
            "tail_reinjection",
            Some(RelaySendCause::TailReinjection),
            Some(ResponseReinjectionServiceModel {
                queue: &queue,
                exclude_front_work: false,
                reinjection_debt_bytes: 1,
            }),
        )
    };

    assert!(matches!(emit(), Err(RuntimeError::SenderServiceBlocked)));
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());

    stream.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 4096,
    }]);
    assert_eq!(
        fixed.accepted_reinjected_data_in_flight_bytes_at(identity),
        0,
    );
    emit().expect("DataACK releases fixed-output recovery authority");
    assert_data_command(&mut receivers, 4096);
}

#[test]
fn switchable_data_commit_publishes_exact_range_and_command() {
    let mut fixture = switchable_fixture();
    let frame = data_frame(4096, 1536);
    let plan = plan_response_data_dispatch(&fixture.stream, TrafficClass::Throughput, 4096, 1536)
        .expect("initial response path has queue credit");

    let selected_path = emit_planned_response_data_frame(
        &fixture.stream,
        plan,
        frame.clone(),
        TrafficClass::Throughput,
    )
    .expect("switchable data commit");

    assert_eq!(selected_path, Some(fixture.initial));
    assert_eq!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&frame),
        vec![output_identity(&fixture.binding, fixture.initial)]
    );
    assert_data_command(&mut fixture.initial_receivers, 4096);
}

#[test]
fn stale_model_generation_rejects_without_carrier_publication() {
    let mut fixture = switchable_fixture();
    let frame = data_frame(0, 1024);
    let plan = plan_response_data_dispatch(&fixture.stream, TrafficClass::Throughput, 0, 1024)
        .expect("observe initial model");
    let (commands, _receivers) = reliable_path_command_channels(1);
    assert_eq!(
        fixture.binding.attach(
            UnderlayProtocol::Udp,
            PathId(9),
            commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached,
    );

    assert!(matches!(
        emit_planned_response_data_frame(
            &fixture.stream,
            plan,
            frame.clone(),
            TrafficClass::Throughput,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&frame)
            .is_empty()
    );
    assert!(try_recv_reliable_path_command(&mut fixture.initial_receivers).is_none());
}

#[test]
fn response_acquisition_observation_cannot_retarget_a_replacement_incarnation() {
    let fixture = switchable_fixture();
    let old_target = fixture
        .binding
        .sender_path_targets(TrafficClass::Throughput, 1024)
        .into_iter()
        .find(|target| target.observation.key == fixture.initial)
        .expect("old exact response output");
    let old_plan = plan_response_data_dispatch(&fixture.stream, TrafficClass::Throughput, 0, 1024)
        .expect("observe old exact response acquisition candidate");
    drop(fixture.initial_receivers);

    let (replacement_commands, mut replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        fixture.binding.attach(
            fixture.initial.underlay,
            fixture.initial.path_id,
            replacement_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::ReplacedClosedOutput,
    );
    let replacement = fixture
        .binding
        .sender_path_targets(TrafficClass::Throughput, 1024)
        .into_iter()
        .find(|target| target.observation.key == fixture.initial)
        .expect("replacement exact response output");
    assert_ne!(
        replacement.observation.path_instance_id,
        old_target.observation.path_instance_id
    );
    assert_ne!(
        replacement.observation.incarnation,
        old_target.observation.incarnation
    );

    let frame = data_frame(0, 1024);
    assert!(matches!(
        emit_planned_response_data_frame(
            &fixture.stream,
            old_plan,
            frame.clone(),
            TrafficClass::Throughput,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&frame)
            .is_empty(),
        "an old response acquisition observation cannot publish ownership on its successor",
    );
    assert!(try_recv_reliable_path_command(&mut replacement_receivers).is_none());

    let fresh = plan_response_data_dispatch(&fixture.stream, TrafficClass::Throughput, 0, 1024)
        .expect("replacement receives a fresh exact observation");
    assert!(matches!(
        fresh,
        ResponseDataDispatchTarget::Switchable { target, .. }
            if target.path_instance_id == replacement.observation.path_instance_id
                && target.incarnation == replacement.observation.incarnation
    ));
}

#[test]
fn closed_carrier_queue_cannot_overtake_ordered_detach() {
    let fixture = switchable_fixture();
    let alternate = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(9),
    };
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    fixture.binding.attach(
        alternate.underlay,
        alternate.path_id,
        alternate_commands,
        TrafficClass::Throughput,
    );

    let original = data_frame(0, 1024);
    fixture
        .binding
        .record_original_flight(fixture.initial, &original);
    let initial_identity = output_identity(&fixture.binding, fixture.initial);
    let next = data_frame(1024, 1024);
    let initial_target = fixture
        .binding
        .sender_path_targets(TrafficClass::Throughput, 1024)
        .into_iter()
        .find(|target| target.observation.key == fixture.initial)
        .expect("initial response target");
    let plan = ResponseDataDispatchTarget::Switchable {
        target: ResponseDispatchTarget::from(&initial_target),
        expected_model_generation: fixture.binding.response_model_generation(),
        position: crate::model::admission::BulkCandidatePosition::ContiguousFrontier,
    };

    drop(fixture.initial_receivers);
    assert!(matches!(
        emit_planned_response_data_frame(&fixture.stream, plan, next, TrafficClass::Throughput,),
        Err(RuntimeError::SenderServiceBlocked)
    ));

    assert!(
        fixture
            .binding
            .has_output_incarnation(initial_identity.0, initial_identity.1),
        "dispatch must leave ownership for the carrier's ordered detach event"
    );
    assert!(
        fixture
            .binding
            .uncovered_failed_original_ranges()
            .is_empty(),
        "queue closure alone must not convert accepted ACK order into failed work"
    );
}

#[test]
fn reinjection_prefers_a_path_without_the_original_range() {
    let mut fixture = switchable_fixture();
    let alternate = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (alternate_commands, mut alternate_receivers) = reliable_path_command_channels(8);
    fixture.binding.attach(
        alternate.underlay,
        alternate.path_id,
        alternate_commands,
        TrafficClass::Throughput,
    );
    fixture.binding.mark_output_path_proven_for_test(alternate);
    let frame = data_frame(0, 4096);
    fixture
        .binding
        .record_original_flight(fixture.initial, &frame);
    let queue = crate::runtime::sender::ReliableRelaySenderQueue::default();

    let selected = emit_response_frame_from_sender_service(
        &fixture.stream,
        frame.clone(),
        TrafficClass::Throughput,
        CarrierEmitMode::Classified,
        "tail_reinjection",
        Some(RelaySendCause::TailReinjection),
        Some(ResponseReinjectionServiceModel {
            queue: &queue,
            exclude_front_work: false,
            reinjection_debt_bytes: 4096,
        }),
    )
    .expect("alternate accepts reinjection");

    assert_eq!(selected.selected_path, Some(alternate));
    assert!(selected.accepted_copy_deadline.is_some());
    let mut outputs = fixture.binding.flight_outputs_overlapping_frame(&frame);
    outputs.sort_by_key(|(key, _)| key.path_id.0);
    assert_eq!(
        outputs,
        vec![
            output_identity(&fixture.binding, fixture.initial),
            output_identity(&fixture.binding, alternate),
        ]
    );
    assert_data_command(&mut alternate_receivers, 0);
    assert!(try_recv_reliable_path_command(&mut fixture.initial_receivers).is_none());
}

#[test]
fn timer_expiry_does_not_retry_an_unresolved_range_on_the_same_outputs() {
    let mut fixture = switchable_fixture();
    let first_alternate = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let second_alternate = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let (first_commands, mut first_receivers) = reliable_path_command_channels(8);
    let (second_commands, mut second_receivers) = reliable_path_command_channels(8);
    fixture.binding.attach(
        first_alternate.underlay,
        first_alternate.path_id,
        first_commands,
        TrafficClass::Throughput,
    );
    fixture.binding.attach(
        second_alternate.underlay,
        second_alternate.path_id,
        second_commands,
        TrafficClass::Throughput,
    );
    fixture
        .binding
        .mark_output_path_proven_for_test(first_alternate);
    fixture
        .binding
        .mark_output_path_proven_for_test(second_alternate);

    let frame = data_frame(0, 4096);
    fixture
        .binding
        .record_original_flight(fixture.initial, &frame);
    fixture
        .binding
        .record_reinjected_flight(first_alternate, &frame);
    fixture
        .binding
        .record_reinjected_flight(second_alternate, &frame);
    fixture
        .binding
        .age_reinjected_flights_for_test(Duration::from_secs(1));
    assert!(!fixture.binding.has_recent_reinjection_overlap(&frame));

    let queue = crate::runtime::sender::ReliableRelaySenderQueue::default();
    let result = emit_response_frame_from_sender_service(
        &fixture.stream,
        frame,
        TrafficClass::Throughput,
        CarrierEmitMode::Classified,
        "ack_gap_reinjection",
        Some(RelaySendCause::AckGapReinjection),
        Some(ResponseReinjectionServiceModel {
            queue: &queue,
            exclude_front_work: false,
            reinjection_debt_bytes: 4096,
        }),
    );

    assert!(matches!(result, Err(RuntimeError::SenderServiceBlocked)));
    assert!(try_recv_reliable_path_command(&mut first_receivers).is_none());
    assert!(try_recv_reliable_path_command(&mut second_receivers).is_none());
    assert!(try_recv_reliable_path_command(&mut fixture.initial_receivers).is_none());
}

#[test]
fn timer_expiry_can_move_unresolved_repair_to_a_different_exact_output() {
    let mut fixture = switchable_fixture();
    let first_alternate = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let second_alternate = crate::model::path::CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let (first_commands, mut first_receivers) = reliable_path_command_channels(8);
    let (second_commands, mut second_receivers) = reliable_path_command_channels(8);
    fixture.binding.attach(
        first_alternate.underlay,
        first_alternate.path_id,
        first_commands,
        TrafficClass::Throughput,
    );
    fixture.binding.attach(
        second_alternate.underlay,
        second_alternate.path_id,
        second_commands,
        TrafficClass::Throughput,
    );
    fixture
        .binding
        .mark_output_path_proven_for_test(first_alternate);
    fixture
        .binding
        .mark_output_path_proven_for_test(second_alternate);

    let frame = data_frame(0, 4096);
    fixture
        .binding
        .record_original_flight(fixture.initial, &frame);
    fixture
        .binding
        .record_reinjected_flight(first_alternate, &frame);
    fixture
        .binding
        .age_reinjected_flights_for_test(Duration::from_secs(1));
    let queue = crate::runtime::sender::ReliableRelaySenderQueue::default();
    let selected = emit_response_frame_from_sender_service(
        &fixture.stream,
        frame,
        TrafficClass::Throughput,
        CarrierEmitMode::Classified,
        "ack_gap_reinjection",
        Some(RelaySendCause::AckGapReinjection),
        Some(ResponseReinjectionServiceModel {
            queue: &queue,
            exclude_front_work: false,
            reinjection_debt_bytes: 4096,
        }),
    )
    .expect("a distinct exact output remains eligible after the recovery interval");

    assert_eq!(selected.selected_path, Some(second_alternate));
    assert!(try_recv_reliable_path_command(&mut first_receivers).is_none());
    assert_data_command(&mut second_receivers, 0);
    assert!(try_recv_reliable_path_command(&mut fixture.initial_receivers).is_none());
}

#[test]
fn control_frame_uses_the_same_live_target_validation() {
    let mut fixture = switchable_fixture();
    let frame = Frame::StreamMaxData {
        stream_id: StreamId(23),
        max_offset: 64 * 1024,
    };

    assert_eq!(
        emit_response_control_frame(&fixture.stream, frame).expect("emit control"),
        Some(fixture.initial)
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.initial_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamMaxData { .. }))
    ));
}
