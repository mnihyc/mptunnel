use super::*;
use crate::protocol::{PathId, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::runtime::stream::response::ResponseStreamAttachOutcome;

#[test]
fn prepared_recovery_accepts_disjoint_due_quanta_without_ack_and_stops_at_young_assignment() {
    let lane = TrafficClass::Throughput;
    let limits = MuxLimits::default();
    let stream_id = StreamId(53);
    let owner = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(53),
        owner.underlay,
        owner.path_id,
        owner_commands,
        lane,
        limits,
    );
    let (commands, mut receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(alternate.underlay, alternate.path_id, commands, lane),
        ResponseStreamAttachOutcome::Attached
    );
    binding.set_output_product_model_for_test(owner, 5_000_000.0, 100.0);
    binding.set_output_product_model_for_test(alternate, 100_000_000.0, 10.0);
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let first = send_stream.send_data(Bytes::from(vec![1; 4096])).unwrap();
    let second = send_stream.send_data(Bytes::from(vec![2; 4096])).unwrap();
    binding.record_original_flight(owner, &first);
    binding.record_original_flight(owner, &second);
    binding.age_original_flights_for_test(Duration::from_secs(10));
    let young = send_stream.send_data(Bytes::from(vec![3; 4096])).unwrap();
    binding.record_original_flight(owner, &young);
    let ack = AuthoritativeStreamAckSnapshot::default();
    let mut sender = ServerResponseSenderService::new(SessionId(53), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"unassigned source"), lane);
    let source_bytes = sender.data_bytes();
    let retained_bytes = send_stream.reinjection_bytes();
    let original_end = send_stream.next_offset();
    let identity = binding
        .sender_path_targets(lane, 4096)
        .iter()
        .find(|target| target.observation.key == alternate)
        .map(ResponseAcquisitionOutputId::from)
        .unwrap();

    // Provisional Product intent is a barrier. It must not license selection
    // of the next range before the first has an accepted copy.
    sender.enqueue_critical_reinjection_frame_with_cause(
        first.clone(),
        RelaySendCause::TailReinjection,
    );
    assert!(
        sender
            .next_prepared_recovery(
                &binding,
                &send_stream,
                &ack,
                lane,
                &binding.sender_path_targets(lane, 4096),
                &[identity],
                Instant::now()
            )
            .candidate
            .is_none()
    );
    sender.queue.commit_front_reinjection().unwrap();
    let counted_before = sender.optional_reinjection.reinjected_bytes();

    for expected in [&first, &second] {
        let ready = receivers
            .writer_ready_boundary(identity.path_instance_id)
            .unwrap();
        let observation = sender.next_prepared_recovery(
            &binding,
            &send_stream,
            &ack,
            lane,
            &binding.sender_path_targets(lane, 4096),
            &[identity],
            Instant::now(),
        );
        let candidate = observation
            .candidate
            .expect("independently due next extent");
        assert_eq!(&candidate.frame, expected);
        assert_eq!(candidate.target.key, alternate);
        sender
            .commit_prepared_recovery(&binding, &send_stream, &candidate, ready, None)
            .expect("same Ready target receives normal carrier repair Apply");
        assert!(!ready.receipt().is_current());
        let ReliablePathCommand::SendFrame(sent) =
            try_recv_reliable_path_command(&mut receivers).unwrap()
        else {
            panic!("normal repair queue frame");
        };
        assert_eq!(&sent, expected);
        receivers.release_pending_command_bytes(
            crate::protocol::frame::reliable_path_frame_pacing_bytes(&sent),
        );
        assert_eq!(
            send_stream.data_ack_frontier(),
            0,
            "no Product ACK was applied"
        );
        assert_eq!(send_stream.reinjection_bytes(), retained_bytes);
        assert_eq!(send_stream.next_offset(), original_end);
        assert_eq!(sender.data_bytes(), source_bytes);
    }
    assert_eq!(
        sender.optional_reinjection.reinjected_bytes() - counted_before,
        8192
    );
    assert_eq!(
        binding.accepted_reinjected_data_in_flight_bytes_at(ServerReinjectionOutputIdentity {
            key: identity.key,
            incarnation: identity.incarnation
        }),
        8192
    );
    let observed_at = Instant::now();
    let stopped = sender.next_prepared_recovery(
        &binding,
        &send_stream,
        &ack,
        lane,
        &binding.sender_path_targets(lane, 4096),
        &[identity],
        observed_at,
    );
    assert!(
        stopped.candidate.is_none(),
        "accepted older copies do not age the young assignment"
    );
    assert!(
        stopped
            .next_deadline
            .is_some_and(|deadline| deadline > observed_at)
    );
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
}
