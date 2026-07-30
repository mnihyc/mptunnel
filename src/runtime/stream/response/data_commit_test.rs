use super::super::ResponseStreamBinding;
use super::super::ResponseTcpServiceObserverInstall;
use super::super::attachment::{ResponseDispatchTarget, next_server_carrier_path_instance_id};
use super::super::test_support::{stream_data_frame, stream_data_frame_at};
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::model::tcp_service::{
    TcpServiceCarrierFence, TcpServiceDataAckEvent, TcpServiceReleaseKind, TcpServiceStreamFence,
    TcpServiceWriterLifecycle,
};
use crate::mux::MuxLimits;
use crate::protocol::{
    AuthNonce, Frame, OffsetRange, PathId, PathMetricDirection, SessionId, StreamId,
    TcpCarrierAcceptedPath, UnderlayProtocol,
};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    reliable_path_command_channels, reliable_path_command_pending_bytes,
    try_recv_reliable_path_command,
};
use crate::runtime::stream::handle::{
    ReliablePathStream, ReliablePathStreamInput, ReliablePathStreamOutput,
    ServerReliableStreamEvent,
};
use crate::runtime::tcp_service::{
    TcpServiceAckDisposition, TcpServiceDataAckSink, TcpServiceFlightSidecarError,
    TcpServiceObserverRemoval, TcpServiceWriterCoordinator,
};
use crate::scheduler::TrafficClass;
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

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

#[derive(Debug, Default)]
struct ReentrantTcpServiceAckSink {
    events: Mutex<Vec<TcpServiceDataAckEvent>>,
    binding: Mutex<Option<Weak<ResponseStreamBinding>>>,
}

impl TcpServiceDataAckSink for ReentrantTcpServiceAckSink {
    fn apply_data_ack(
        &self,
        event: TcpServiceDataAckEvent,
        _now: Instant,
    ) -> Result<TcpServiceAckDisposition, TcpServiceFlightSidecarError> {
        let binding = self
            .binding
            .lock()
            .expect("TCP service sink binding")
            .as_ref()
            .and_then(Weak::upgrade)
            .expect("response binding remains live");
        assert!(
            binding.has_live_output(),
            "model sink can re-enter the response binding after ACK release"
        );
        self.events
            .lock()
            .expect("TCP service sink events")
            .push(event);
        Ok(TcpServiceAckDisposition::Continue)
    }
}

fn response_tcp_service_carrier(
    path_id: u16,
    nonce: u8,
    local_instance_id: CarrierPathInstanceId,
) -> TcpServiceCarrierFence {
    TcpServiceCarrierFence {
        accepted: TcpCarrierAcceptedPath {
            path_id: PathId(path_id),
            path_join_nonce: AuthNonce([nonce; 16]),
        },
        local_instance_id,
        eligibility_generation: 1,
    }
}

#[tokio::test]
async fn response_actor_preserves_install_ack_boundaries_and_partial_provenance() {
    let mut fixture = fixture(8);
    let stream_id = StreamId(7);
    let lifecycle = TcpServiceWriterLifecycle::for_runtime_test(
        SessionId(188),
        1,
        PathMetricDirection::ServerToClient,
    );
    let sink = Arc::new(ReentrantTcpServiceAckSink::default());
    *sink.binding.lock().expect("TCP service sink binding") =
        Some(Arc::downgrade(&fixture.binding));
    let coordinator = Arc::new(TcpServiceWriterCoordinator::new(lifecycle, sink.clone()));
    let accepted = response_tcp_service_carrier(
        fixture.target.key.path_id.0,
        9,
        fixture.target.path_instance_id,
    );
    let candidate = response_tcp_service_carrier(1, 19, next_server_carrier_path_instance_id());
    let stream_fence = TcpServiceStreamFence {
        stream_id,
        demand_generation: 2,
        attachment_incarnation: 3,
        data_ack_horizon_bytes: 1024,
    };

    let preinstall = stream_data_frame_at(0, 1024);
    enqueue(
        &fixture,
        &fixture.target,
        &preinstall,
        fixture.binding.response_model_generation(),
    )
    .expect("pre-install response publication");

    let (events, events_rx) = mpsc::channel(8);
    let mut stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
        output: ReliablePathStreamOutput::Switchable(fixture.binding.clone()),
        frames: ReliablePathStreamInput::server(events_rx),
    };
    let (install_receipt, installed) = oneshot::channel();
    events
        .try_send(ServerReliableStreamEvent::InstallTcpServiceObserver {
            install: Box::new(ResponseTcpServiceObserverInstall {
                stream: stream_fence,
                accepted: vec![accepted],
                candidate,
                coordinator: coordinator.clone(),
                max_flight_records: 8,
                max_ack_release_records: 8,
            }),
            receipt: install_receipt,
        })
        .expect("queue observer installation");
    events
        .try_send(ServerReliableStreamEvent::Frame(Frame::Ping { nonce: 1 }))
        .expect("queue installation ordering boundary");
    assert_eq!(
        stream.recv_frame().await.expect("installation boundary"),
        Frame::Ping { nonce: 1 }
    );
    assert_eq!(installed.await.expect("installation receipt"), Ok(true));
    {
        let mut transaction = coordinator.lock();
        transaction
            .initial_boundary()
            .expect("initial writer boundary");
        assert!(transaction.activate());
    }

    let active = stream_data_frame_at(1024, 3072);
    {
        let mut transaction = coordinator.lock();
        fixture
            .binding
            .try_enqueue_data_frame_for_dispatch_target_with_tcp_service(
                &fixture.target,
                &active,
                TrafficClass::Throughput,
                fixture.binding.response_model_generation(),
                &mut transaction,
            )
            .expect("observed response publication");
    }

    for ranges in [
        vec![OffsetRange {
            start: 0,
            end: 2048,
        }],
        vec![OffsetRange {
            start: 2048,
            end: 4096,
        }],
    ] {
        events
            .try_send(ServerReliableStreamEvent::Frame(Frame::StreamAck {
                stream_id,
                complete: false,
                ranges,
            }))
            .expect("queue exact ACK transaction");
    }
    for expected in [
        OffsetRange {
            start: 0,
            end: 2048,
        },
        OffsetRange {
            start: 2048,
            end: 4096,
        },
    ] {
        let frame = stream.recv_frame().await.expect("unmerged active ACK");
        let Frame::StreamAck { ranges, .. } = frame else {
            panic!("expected an active ACK transaction");
        };
        assert_eq!(ranges, vec![expected]);
        let coordinator = stream
            .tcp_service_coordinator()
            .expect("actor caches the active lifecycle");
        let mut transaction = coordinator.lock();
        stream.release_normalized_acked_ranges_for_tcp_service(&ranges, 4096, lifecycle);
        stream.finish_tcp_service_ack(&mut transaction);
    }

    let recorded = sink.events.lock().expect("TCP service sink events");
    assert_eq!(
        recorded.len(),
        2,
        "active ACK frames remain distinct model transactions"
    );
    assert_eq!(recorded[0].stream, stream_fence);
    assert_eq!(recorded[0].assigned_end, 4096);
    assert_eq!(recorded[0].releases.len(), 2);
    assert_eq!(
        recorded[0].releases[0].range,
        OffsetRange {
            start: 0,
            end: 1024,
        }
    );
    assert_eq!(recorded[0].releases[0].committed_at, None);
    assert_eq!(
        recorded[0].releases[1].range,
        OffsetRange {
            start: 1024,
            end: 2048,
        }
    );
    assert!(recorded[0].releases[1].committed_at.is_some());
    assert_eq!(recorded[1].releases.len(), 1);
    assert_eq!(
        recorded[1].releases[0].range,
        OffsetRange {
            start: 2048,
            end: 4096,
        }
    );
    assert!(
        recorded
            .iter()
            .flat_map(|event| &event.releases)
            .all(|release| {
                release.carrier == accepted
                    && release.kind == TcpServiceReleaseKind::Original
                    && release.unambiguous
            })
    );
    drop(recorded);

    for expected_offset in [0, 1024] {
        assert!(matches!(
            try_recv_reliable_path_command(&mut fixture.receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData {
                offset,
                ..
            })) if offset == expected_offset
        ));
    }

    {
        let mut transaction = coordinator.lock();
        transaction.stop();
    }
    let (remove_receipt, removed) = oneshot::channel();
    events
        .try_send(ServerReliableStreamEvent::RemoveTcpServiceObserver {
            lifecycle,
            receipt: remove_receipt,
        })
        .expect("queue exact lifecycle removal");
    events
        .try_send(ServerReliableStreamEvent::Frame(Frame::Ping { nonce: 2 }))
        .expect("queue removal ordering boundary");
    assert_eq!(
        stream.recv_frame().await.expect("removal boundary"),
        Frame::Ping { nonce: 2 }
    );
    assert_eq!(
        removed.await.expect("removal receipt"),
        TcpServiceObserverRemoval::Removed
    );
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
