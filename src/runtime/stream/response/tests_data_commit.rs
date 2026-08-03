use super::super::ResponseStreamBinding;
use super::super::attachment::ResponseDispatchTarget;
use super::super::test_support::{stream_data_frame, stream_data_frame_at};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathId, SessionId, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    reliable_path_command_channels, reliable_path_command_pending_bytes,
    try_recv_reliable_path_command,
};
use crate::scheduler::TrafficClass;
use std::sync::Arc;

struct Fixture {
    binding: Arc<ResponseStreamBinding>,
    key: CarrierPathKey,
    commands: ReliablePathCommandSender,
    receivers: ReliablePathCommandReceivers,
    target: ResponseDispatchTarget,
}

fn fixture(queue_capacity: usize) -> Fixture {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (commands, receivers) = reliable_path_command_channels(queue_capacity);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(188),
        key.underlay,
        key.path_id,
        commands.clone(),
        TrafficClass::Throughput,
        MuxLimits::default(),
    );
    let target = binding
        .sender_path_targets(TrafficClass::Throughput, 1024)
        .into_iter()
        .next()
        .expect("initial response path is schedulable")
        .into();
    Fixture {
        binding,
        key,
        commands,
        receivers,
        target,
    }
}

fn enqueue(
    fixture: &Fixture,
    target: &ResponseDispatchTarget,
    frame: &Frame,
    generation: u64,
) -> Result<(), RuntimeError> {
    enqueue_on_lane(fixture, target, frame, TrafficClass::Throughput, generation)
}

fn enqueue_on_lane(
    fixture: &Fixture,
    target: &ResponseDispatchTarget,
    frame: &Frame,
    lane: TrafficClass,
    generation: u64,
) -> Result<(), RuntimeError> {
    fixture
        .binding
        .try_enqueue_data_frame_for_dispatch_target(target, frame, lane, generation)
}

#[test]
fn stale_attachment_or_model_generation_cannot_commit() {
    let mut fixture = fixture(2);
    let frame = stream_data_frame(1024);
    let generation = fixture.binding.response_model_generation();
    let mut stale_target = fixture.target;
    stale_target.incarnation = stale_target.incarnation.wrapping_add(1);

    assert!(matches!(
        enqueue(&fixture, &stale_target, &frame, generation),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    fixture.binding.set_sender_queue_bytes(1);
    assert!(matches!(
        enqueue(&fixture, &fixture.target, &frame, generation),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&frame)
            .is_empty()
    );
    assert!(try_recv_reliable_path_command(&mut fixture.receivers).is_none());
}

#[test]
fn full_carrier_queue_rolls_back_without_publishing_flight() {
    let mut fixture = fixture(1);
    let frame = stream_data_frame(1024);
    fixture
        .commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame_at(1024, 1024),
            TrafficClass::Throughput,
        )
        .expect("fill carrier queue");
    let generation = fixture.binding.response_model_generation();

    assert!(matches!(
        enqueue(&fixture, &fixture.target, &frame, generation),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(fixture.binding.response_model_generation(), generation);
    assert!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&frame)
            .is_empty()
    );
    let filler = try_recv_reliable_path_command(&mut fixture.receivers)
        .expect("only the queue filler was published");
    fixture
        .receivers
        .release_pending_command_bytes(reliable_path_command_pending_bytes(&filler));
    assert!(try_recv_reliable_path_command(&mut fixture.receivers).is_none());
}

#[test]
fn successful_commit_publishes_exact_flight_before_carrier_work() {
    let mut fixture = fixture(1);
    let frame = stream_data_frame_at(4096, 1536);
    let generation = fixture.binding.response_model_generation();

    enqueue(&fixture, &fixture.target, &frame, generation).expect("commit response data");

    assert_eq!(
        fixture.binding.response_model_generation(),
        generation.wrapping_add(1)
    );
    assert_eq!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&frame),
        vec![(fixture.key, fixture.target.incarnation)]
    );
    let outputs = fixture
        .binding
        .outputs
        .lock()
        .expect("test response outputs lock");
    let output = outputs.entries.first().expect("initial response output");
    assert_eq!(output.original_data_in_flight_bytes, 1536);
    assert_eq!(output.bytes_in_flight, 1536);
    drop(outputs);
    let flights = fixture
        .binding
        .flights
        .lock()
        .expect("test response flight lock");
    let flight = flights
        .get(&4096)
        .and_then(|flights| flights.first())
        .expect("exact range flight");
    assert_eq!(flight.key, fixture.key);
    assert_eq!(flight.output_incarnation, fixture.target.incarnation);
    assert_eq!(flight.end, 4096 + 1536);
    assert_eq!(flight.bytes, 1536);
    drop(flights);
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 4096,
            ..
        }))
    ));
}

#[test]
fn latency_data_commit_is_not_blocked_behind_queued_bulk() {
    let mut fixture = fixture(1);
    let bulk = stream_data_frame_at(4096, 1024);
    fixture
        .commands
        .try_enqueue_admitted_frame(bulk, TrafficClass::Throughput)
        .expect("fill bulk queue");
    let latency = stream_data_frame_at(0, 128);
    let generation = fixture.binding.response_model_generation();

    enqueue_on_lane(
        &fixture,
        &fixture.target,
        &latency,
        TrafficClass::Latency,
        generation,
    )
    .expect("latency response uses independent priority capacity");

    let first =
        try_recv_reliable_path_command(&mut fixture.receivers).expect("latency response command");
    assert!(matches!(
        &first,
        ReliablePathCommand::SendFrame(Frame::StreamData { offset: 0, .. })
    ));
    fixture
        .receivers
        .release_pending_command_bytes(reliable_path_command_pending_bytes(&first));
    let second =
        try_recv_reliable_path_command(&mut fixture.receivers).expect("queued bulk command");
    assert!(matches!(
        second,
        ReliablePathCommand::SendFrame(Frame::StreamData { offset: 4096, .. })
    ));
}
