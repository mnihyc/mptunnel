//! Response source ownership at the actual protected TCP writer boundary.
//!
//! These tests reuse the encrypted carrier fixture. Registry admission and
//! advertised receive credit are real; no rate, flight, or Native capacity is
//! injected. They prove ordered placement, not a wall-clock speed improvement.

use super::*;
use crate::model::capacity::{
    adaptive_reliable_relay_chunk_bytes_with_frame_limit, reliable_path_startup_sample_limit_bytes,
};
use crate::model::path::CarrierPathKey;
use crate::mux::{MuxLimits, stream::ReliableSendStream};
use crate::runtime::path::commands::{ReliablePathCommandSender, reliable_path_command_queue};
use crate::runtime::path::{ServerStreamOpenRequest, ServerStreamPathAttachment};
use crate::runtime::relay::io::{
    begin_reliable_stream_ack, update_reinjection_authoritative_ack_snapshot,
};
use crate::runtime::sender::{
    PreparedResponseSource, ResponseProductState, ServerResponseSenderService,
    SharedResponseProduct, publish_prepared_response_work,
};
use crate::runtime::stream::{
    AcceptedServerReliableStream, ReliablePathStream, ReliablePathStreamOutput,
    ServerReliableStreamOpen, ServerReliableStreamRegistry,
};

type ProtectedCarrier = (
    ServerTcpPathSession,
    EncryptedFramedStream<TcpStream>,
    ReliablePathCommandSender,
    mpsc::Sender<Result<Frame, EncryptedFramedTransportError>>,
    crate::runtime::relay::ServerReliableRelayService,
);

struct PreparedResponseFixture {
    a: ProtectedCarrier,
    b: ProtectedCarrier,
    owner: SharedResponseProduct,
    path_stream: ReliablePathStream,
    _accepted: AcceptedServerReliableStream,
    registry: Arc<ServerReliableStreamRegistry>,
    quantum: usize,
}

impl PreparedResponseFixture {
    async fn new() -> Self {
        let limits = MuxLimits::default();
        let lane = TrafficClass::Throughput;
        let session_id = SessionId(321);
        let stream_id = StreamId(321);
        let quantum = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
            None,
            lane,
            limits,
            limits.max_payload_bytes,
        )
        .min(usize::try_from(reliable_path_startup_sample_limit_bytes(limits)).unwrap());
        assert!(quantum > 0);
        assert!(2 * quantum <= limits.max_repair_bytes);
        let capacity = reliable_path_command_queue(limits);
        let mut a = server_tcp_test_session_with_mode(
            session_id,
            PathId(0),
            crate::config::ForwardingMode::L4,
            Some(capacity),
        )
        .await;
        let mut b = server_tcp_test_session_with_mode(
            session_id,
            PathId(1),
            crate::config::ForwardingMode::L4,
            Some(capacity),
        )
        .await;

        // Enroll both already-protected endpoints in one real stream registry
        // before opening Product source. Unlike the old dispatcher-only test
        // constructor, these are the physical IDs used by the native writers.
        let registry = Arc::new(ServerReliableStreamRegistry::new(limits.max_streams));
        let port = registry.path_port();
        for carrier in [&mut a, &mut b] {
            carrier.0.context.reliable_streams = port.clone();
            carrier.0.path_registration = port.register_test_carrier_path(
                session_id,
                UnderlayProtocol::Tcp,
                carrier.0.path_id,
                ServerLocalPathProperties::default(),
            );
        }
        // A remains a legal sole Backup; later Regular B supplies a
        // deterministic preference without a fabricated completion estimate.
        port.record_peer_path_usage(&a.0.path_registration, 1, PathUsage::Backup);
        let mut accepted = match registry
            .open_or_attach(Self::open_request(&a, stream_id, false))
            .expect("open response through its actual registered carrier")
        {
            ServerReliableStreamOpen::New(accepted, _) => *accepted,
            _ => panic!("expected a newly admitted logical stream"),
        };
        let mut path_stream = accepted.take_stream();
        let ReliablePathStreamOutput::Switchable(binding) = &path_stream.output else {
            panic!("response has a shared binding")
        };
        let binding = binding.clone();
        let mut send_stream = ReliableSendStream::new_with_initial_max_offset(stream_id, limits, 0);
        assert_eq!(send_stream.send_credit_bytes(), 0);
        port.route_frame(
            &a.0.path_registration,
            stream_id,
            Frame::StreamMaxData {
                stream_id,
                max_offset: limits.max_stream_window_bytes,
            },
        )
        .await
        .expect("route advertised response credit through exact attachment");
        let Frame::StreamMaxData {
            stream_id: credited,
            max_offset,
        } = path_stream
            .recv_frame()
            .await
            .expect("receive advertised credit")
        else {
            panic!("expected response credit")
        };
        assert_eq!(credited, stream_id);
        send_stream.update_max_offset(max_offset);
        assert!(send_stream.send_credit_bytes() >= 2 * quantum);
        let owner = SharedResponseProduct::new(
            ResponseProductState {
                sender: ServerResponseSenderService::new(session_id, stream_id),
                send_stream,
                last_send_ack: Default::default(),
                prepared: PreparedResponseSource::new(lane, quantum),
            },
            binding,
        );
        Self {
            a,
            b,
            owner,
            path_stream,
            _accepted: accepted,
            registry,
            quantum,
        }
    }

    fn open_request(
        carrier: &ProtectedCarrier,
        stream_id: StreamId,
        existing: bool,
    ) -> ServerStreamOpenRequest {
        ServerStreamOpenRequest {
            session_id: carrier.0.session_id,
            stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            initial_demand: StreamDemandHint::Throughput,
            return_plan: if existing {
                StreamReturnPlan {
                    phase: StreamAttachmentPhase::Ordinary,
                    ..Default::default()
                }
            } else {
                StreamReturnPlan::default()
            },
            attachment: ServerStreamPathAttachment {
                path_registration: carrier.0.path_registration.clone(),
                commands: carrier.2.clone(),
                max_frame_payload_bytes: carrier.0.context.mux_limits.max_payload_bytes,
            },
            mux_limits: carrier.0.context.mux_limits,
        }
    }

    fn publish(&self, payload: Bytes) {
        let mut state = self.owner.lock();
        state
            .sender
            .enqueue_data_for_lane(payload, TrafficClass::Throughput);
        publish_prepared_response_work(
            &mut state,
            &self.owner,
            TrafficClass::Throughput,
            self.quantum,
            true,
        );
    }

    fn attach_b(&self) {
        assert!(matches!(
            self.registry
                .open_or_attach(Self::open_request(
                    &self.b,
                    self.path_stream.stream_id,
                    true
                ))
                .expect("attach current Regular B to the same response"),
            ServerReliableStreamOpen::Existing(TrafficClass::Throughput)
        ));
        let mut state = self.owner.lock();
        publish_prepared_response_work(
            &mut state,
            &self.owner,
            TrafficClass::Throughput,
            self.quantum,
            true,
        );
        let b = self
            .owner
            .binding()
            .sender_path_targets(TrafficClass::Throughput, self.quantum)
            .into_iter()
            .find(|target| target.observation.key.path_id == PathId(1))
            .unwrap();
        assert_eq!(
            b.observation.path_instance_id,
            self.b.0.path_registration.path_instance_id()
        );
        assert!(b.product_admission_active);
        assert!(!b.observation.stale_for_original_data);
    }

    fn original_bytes(&self, path_id: PathId) -> u64 {
        self.owner
            .binding()
            .sender_path_targets(TrafficClass::Throughput, self.quantum)
            .into_iter()
            .find(|target| {
                target.observation.key
                    == CarrierPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        path_id,
                    }
            })
            .unwrap()
            .observation
            .original_data_in_flight_bytes
    }

    fn assert_source(&self, accepted: usize, claimed: usize, acked: usize) {
        let state = self.owner.lock();
        assert_eq!(state.sender.data_bytes(), accepted - claimed);
        assert_eq!(state.send_stream.next_offset(), claimed as u64);
        assert_eq!(state.send_stream.reinjection_bytes(), claimed - acked);
        assert_eq!(
            self.original_bytes(PathId(0)) + self.original_bytes(PathId(1)),
            (claimed - acked) as u64
        );
        assert!(state.prepared.pending_error.is_none());
    }

    async fn acknowledge(&mut self, end: u64) {
        let stream_id = self.path_stream.stream_id;
        self.registry
            .path_port()
            .route_frame(
                &self.a.0.path_registration,
                stream_id,
                Frame::StreamAck {
                    stream_id,
                    scope_start: Some(0),
                    ranges: vec![crate::protocol::OffsetRange { start: 0, end }],
                },
            )
            .await
            .expect("route received ACK through the actual attachment");
        let Frame::StreamAck {
            stream_id: received,
            scope_start,
            ranges,
        } = self
            .path_stream
            .recv_frame()
            .await
            .expect("receive routed ACK")
        else {
            panic!("expected routed response ACK")
        };
        assert_eq!(received, stream_id);
        // Exercise the production validator and paired cache/flight releases.
        // This writer fixture does not claim to execute the whole relay actor.
        let mut state = self.owner.lock();
        let ack = begin_reliable_stream_ack(&state.send_stream, scope_start, ranges).unwrap();
        if !state.last_send_ack.subsumes(&ack, &state.send_stream) {
            let applied = state.send_stream.apply_validated_ack(&ack).unwrap();
            state.sender.record_delivered_data(applied.released_bytes);
            self.owner
                .binding()
                .release_normalized_acked_ranges(ack.ranges());
            state
                .sender
                .release_normalized_acked_reinjections(ack.ranges());
            let ResponseProductState {
                last_send_ack,
                send_stream,
                ..
            } = &mut *state;
            update_reinjection_authoritative_ack_snapshot(last_send_ack, &ack, send_stream);
            publish_prepared_response_work(
                &mut state,
                &self.owner,
                TrafficClass::Throughput,
                self.quantum,
                true,
            );
        }
    }
}

async fn write_prepared_response(carrier: &mut ProtectedCarrier) -> Frame {
    let command = try_recv_reliable_path_command(&mut carrier.0.commands_rx)
        .expect("real prepared source notice is queued");
    assert!(
        matches!(&command, ReliablePathCommand::PreparedOriginal(_)),
        "prepared source must not be a private future Data command"
    );
    assert!(matches!(
        carrier
            .0
            .drain_commands(command)
            .await
            .expect("run actual protected writer"),
        ServerTcpSessionDisposition::Continue
    ));
    let frame = tokio::time::timeout(Duration::from_secs(5), carrier.1.read_frame())
        .await
        .expect("protected peer receipt completion guard")
        .expect("read protected response");
    assert_eq!(carrier.2.pending_bytes(), 0);
    assert_eq!(carrier.2.writer_pending_bytes(), 0);
    frame
}

#[tokio::test]
async fn prepared_response_ready_alternate_writes_lowest_unclaimed_prefix() {
    let mut fixture = PreparedResponseFixture::new().await;
    let first = Bytes::from(vec![0x51; fixture.quantum]);
    let second = Bytes::from(vec![0x62; fixture.quantum]);
    fixture.publish(first.clone());
    fixture.publish(second);
    fixture.attach_b();
    let before = {
        let state = fixture.owner.lock();
        (
            state.sender.data_bytes(),
            state.send_stream.next_offset(),
            state.send_stream.reinjection_bytes(),
            fixture.original_bytes(PathId(0)),
            fixture.a.2.pending_bytes(),
            fixture.a.2.writer_pending_bytes(),
        )
    };

    let received = write_prepared_response(&mut fixture.b).await;
    assert_eq!(
        received,
        Frame::StreamData {
            stream_id: fixture.path_stream.stream_id,
            offset: 0,
            payload: first,
        },
        "actual alternate peer must receive the shared lowest prefix, not a later source suffix"
    );
    assert_eq!(before, (2 * fixture.quantum, 0, 0, 0, 0, 0));
    fixture.assert_source(2 * fixture.quantum, fixture.quantum, 0);
    assert_eq!(fixture.original_bytes(PathId(0)), 0);
    assert_eq!(fixture.original_bytes(PathId(1)), fixture.quantum as u64);
}

#[tokio::test]
async fn prepared_response_started_original_stays_owned_when_alternate_writes() {
    let mut fixture = PreparedResponseFixture::new().await;
    let first = Bytes::from(vec![0x51; fixture.quantum]);
    let second = Bytes::from(vec![0x62; fixture.quantum]);
    fixture.publish(first.clone());
    fixture.publish(second.clone());
    assert_eq!(
        write_prepared_response(&mut fixture.a).await,
        Frame::StreamData {
            stream_id: fixture.path_stream.stream_id,
            offset: 0,
            payload: first,
        }
    );
    fixture.attach_b();
    assert_eq!(fixture.original_bytes(PathId(0)), fixture.quantum as u64);
    assert_eq!(
        write_prepared_response(&mut fixture.b).await,
        Frame::StreamData {
            stream_id: fixture.path_stream.stream_id,
            offset: fixture.quantum as u64,
            payload: second,
        }
    );
    fixture.assert_source(2 * fixture.quantum, 2 * fixture.quantum, 0);
    assert_eq!(fixture.original_bytes(PathId(0)), fixture.quantum as u64);
    assert_eq!(fixture.original_bytes(PathId(1)), fixture.quantum as u64);
}

#[tokio::test]
async fn prepared_response_ack_does_not_rewind_a_queued_shared_claim() {
    let mut fixture = PreparedResponseFixture::new().await;
    let first = Bytes::from(vec![0x51; fixture.quantum]);
    let second = Bytes::from(vec![0x62; fixture.quantum]);
    fixture.publish(first.clone());
    fixture.publish(second.clone());
    assert_eq!(
        write_prepared_response(&mut fixture.a).await,
        Frame::StreamData {
            stream_id: fixture.path_stream.stream_id,
            offset: 0,
            payload: first,
        }
    );
    fixture.attach_b();
    let quantum = fixture.quantum;
    fixture.acknowledge(quantum as u64).await;
    fixture.assert_source(2 * quantum, quantum, quantum);
    assert_eq!(fixture.original_bytes(PathId(0)), 0);
    assert_eq!(
        write_prepared_response(&mut fixture.b).await,
        Frame::StreamData {
            stream_id: fixture.path_stream.stream_id,
            offset: quantum as u64,
            payload: second,
        }
    );
    fixture.assert_source(2 * quantum, 2 * quantum, quantum);
    fixture.acknowledge((2 * quantum) as u64).await;
    fixture.acknowledge((2 * quantum) as u64).await;
    fixture.assert_source(2 * quantum, 2 * quantum, 2 * quantum);
}

#[tokio::test]
async fn prepared_response_losing_ready_writer_preserves_winner_and_control_service() {
    let mut fixture = PreparedResponseFixture::new().await;
    let payload = Bytes::from(vec![0x51; fixture.quantum]);
    fixture.publish(payload.clone());
    fixture.attach_b();
    let a_instance = fixture.a.0.path_registration.path_instance_id();
    let b_instance = fixture.b.0.path_registration.path_instance_id();
    let a_ready = fixture
        .a
        .0
        .commands_rx
        .writer_ready_boundary(a_instance)
        .unwrap()
        .receipt();
    let b_ready = fixture
        .b
        .0
        .commands_rx
        .writer_ready_boundary(b_instance)
        .unwrap()
        .receipt();
    let a_command = try_recv_reliable_path_command(&mut fixture.a.0.commands_rx).unwrap();
    assert!(matches!(
        &a_command,
        ReliablePathCommand::PreparedOriginal(_)
    ));
    assert!(matches!(
        fixture.a.0.drain_commands(a_command).await.unwrap(),
        ServerTcpSessionDisposition::Continue
    ));
    fixture.assert_source(fixture.quantum, 0, 0);
    assert!(
        a_ready.is_current(),
        "a refused metadata claim is not physical writer occupancy"
    );
    assert!(
        b_ready.is_current(),
        "an unselected writer cannot revoke B's exact Ready epoch"
    );
    assert_eq!(
        write_prepared_response(&mut fixture.b).await,
        Frame::StreamData {
            stream_id: fixture.path_stream.stream_id,
            offset: 0,
            payload,
        }
    );
    fixture.assert_source(fixture.quantum, fixture.quantum, 0);

    let ping = Frame::Ping { nonce: 321 };
    fixture
        .a
        .2
        .try_enqueue_admitted_frame(ping.clone(), TrafficClass::Control)
        .unwrap();
    let command = try_recv_reliable_path_command(&mut fixture.a.0.commands_rx).unwrap();
    assert!(matches!(&command, ReliablePathCommand::SendFrame(frame) if frame == &ping));
    fixture.a.0.drain_commands(command).await.unwrap();
    assert_eq!(fixture.a.1.read_frame().await.unwrap(), ping);
    assert!(
        !a_ready.is_current(),
        "actual protected control I/O withdraws idle Ready"
    );
    assert_eq!(fixture.a.2.pending_bytes(), 0);
    assert_eq!(fixture.a.2.writer_pending_bytes(), 0);
}

#[tokio::test]
async fn prepared_response_cancelled_source_notice_does_not_own_the_carrier() {
    let fixture = PreparedResponseFixture::new().await;
    let lifetime = fixture.owner.actor_lifetime();
    fixture.publish(Bytes::from(vec![0x51; fixture.quantum]));
    fixture.attach_b();
    drop(lifetime);
    fixture.assert_source(fixture.quantum, 0, 0);
    assert!(!fixture.owner.lock().prepared.claims_active);
    assert!(fixture.owner.lock().prepared.registrations.is_empty());
    let PreparedResponseFixture {
        mut a,
        mut b,
        owner,
        path_stream,
        _accepted,
        registry,
        ..
    } = fixture;
    let weak = owner.downgrade();
    drop(owner);
    assert!(
        weak.upgrade().is_none(),
        "carrier notices retain no strong source owner"
    );
    for carrier in [&mut a, &mut b] {
        if let Some(command) = try_recv_reliable_path_command(&mut carrier.0.commands_rx) {
            assert!(matches!(&command, ReliablePathCommand::PreparedOriginal(_)));
            carrier.0.drain_commands(command).await.unwrap();
        }
        let ping = Frame::Ping { nonce: 322 };
        carrier
            .2
            .try_enqueue_admitted_frame(ping.clone(), TrafficClass::Control)
            .unwrap();
        let command = try_recv_reliable_path_command(&mut carrier.0.commands_rx).unwrap();
        carrier.0.drain_commands(command).await.unwrap();
        assert_eq!(carrier.1.read_frame().await.unwrap(), ping);
        assert_eq!(carrier.2.pending_bytes(), 0);
        assert_eq!(carrier.2.writer_pending_bytes(), 0);
    }
    drop((path_stream, _accepted, registry));
}
