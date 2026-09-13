//! Actual prepared callback -> normal repair queue -> protected TCP producer.
//! Original wire frames are deliberately withheld from the Product receiver;
//! the receiver advances on copies without returning a Product ACK to sender.

use super::*;
use crate::mux::stream::ReliableRecvStream;
use crate::protocol::frame::{reliable_path_frame_pacing_bytes, reliable_stream_frame_extent};
use crate::runtime::sender::{PreparedOriginalClaim, ServerReinjectionOutputIdentity};
use crate::runtime::stream::response::{ResponseAcquisitionOutputId, ResponsePreparedNativeInputs};

async fn drain_one(carrier: &mut ProtectedCarrier, command: ReliablePathCommand) {
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), carrier.0.drain_commands(command))
            .await
            .expect("existing protected writer completion guard")
            .expect("protected writer turn"),
        ServerTcpSessionDisposition::Continue
    ));
}

async fn receive_one(carrier: &mut ProtectedCarrier) -> Frame {
    tokio::time::timeout(Duration::from_secs(5), carrier.1.read_frame())
        .await
        .expect("existing protected peer completion guard")
        .expect("protected peer frame")
}

#[tokio::test]
async fn prepared_tcp_stale_original_waits_for_occupied_nonstale_writer() {
    let mut fixture = PreparedResponseFixture::new().await;
    fixture.attach_b();
    let stream_id = fixture.path_stream.stream_id;
    let bytes = 4096.min(fixture.quantum);
    let first_payload = Bytes::from(vec![0x61; bytes]);
    let second_payload = Bytes::from(vec![0x62; bytes]);
    let first = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: first_payload.clone(),
    };
    let second = Frame::StreamData {
        stream_id,
        offset: bytes as u64,
        payload: second_payload.clone(),
    };

    // B owns a real claimed Original and its protected writer transaction.
    // Its missing Ready receipt is occupancy, not an injected capacity sample.
    fixture.publish(first_payload);
    let ReliablePathCommand::PreparedOriginal(b_work) =
        try_recv_reliable_path_command(&mut fixture.b.0.commands_rx).unwrap()
    else {
        panic!("actual B source notice")
    };
    let b_instance = fixture.b.0.path_registration.path_instance_id();
    let b_ready = fixture
        .b
        .0
        .commands_rx
        .writer_ready_boundary(b_instance)
        .unwrap();
    let b_receipt = b_ready.receipt();
    let PreparedOriginalClaim::Claimed(owned) = b_work.try_claim(b_ready) else {
        panic!("current Regular B must acquire the first Original")
    };
    assert_eq!(owned, first);
    assert!(!b_receipt.is_current());
    let mut owned_charge = fixture
        .b
        .0
        .commands_rx
        .register_claimed_writer_frame(&owned);
    fixture.b.0.writer.begin_transaction().unwrap();
    fixture.b.0.writer.stage_transaction_frame(owned).unwrap();
    b_work.requeue();
    assert!(owned_charge > 0);
    assert_eq!(fixture.b.2.writer_pending_bytes(), owned_charge as u64);

    let a_identity = fixture
        .owner
        .binding()
        .sender_path_targets(fixture.lane, bytes)
        .into_iter()
        .find(|target| target.observation.key.path_id == PathId(0))
        .map(|target| ServerReinjectionOutputIdentity {
            key: target.observation.key,
            incarnation: target.observation.incarnation,
        })
        .unwrap();
    // Exercise the existing stale consumer state. The separate persistence
    // clock is not under test, and no assignment timestamp is aged here.
    assert!(
        fixture
            .owner
            .binding()
            .mark_output_stale(a_identity, fixture.lane)
    );
    assert!(fixture.owner.binding().output_is_stale(a_identity));
    assert!(
        fixture
            .path_stream
            .has_nonstale_reinjection_alternative(a_identity, fixture.lane),
        "B remains a structural alternative while its writer is occupied"
    );
    fixture.publish(second_payload);
    fixture.assert_source(2 * bytes, bytes, 0);

    let ReliablePathCommand::PreparedOriginal(a_work) =
        try_recv_reliable_path_command(&mut fixture.a.0.commands_rx).unwrap()
    else {
        panic!("actual A source notice")
    };
    let a_instance = fixture.a.0.path_registration.path_instance_id();
    let a_ready = fixture
        .a
        .0
        .commands_rx
        .writer_ready_boundary(a_instance)
        .unwrap();
    let a_receipt = a_ready.receipt();
    match a_work.try_claim(a_ready) {
        PreparedOriginalClaim::Blocked(_) => {}
        PreparedOriginalClaim::Claimed(frame) => panic!(
            "stale A acquired a fresh Original while non-stale B was occupied: {:?}",
            reliable_stream_frame_extent(&frame)
        ),
        _ => panic!("stale A must refuse this current source opportunity"),
    }
    assert!(
        a_receipt.is_current(),
        "stale placement refusal does not consume physical Ready"
    );
    assert!(!b_receipt.is_current());
    assert_eq!(fixture.a.2.pending_bytes(), 0);
    assert_eq!(fixture.a.2.writer_pending_bytes(), 0);
    assert_eq!(fixture.b.2.writer_pending_bytes(), owned_charge as u64);
    fixture.assert_source(2 * bytes, bytes, 0);
    assert_eq!(fixture.original_bytes(PathId(0)), 0);
    a_work.requeue();

    // Complete only B's already-owned transaction, then let the same real
    // writer acquire the still-shared second prefix through normal arbitration.
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(5),
            fixture
                .b
                .0
                .commit_transaction_respecting_deferred_input(&mut owned_charge)
        )
        .await
        .unwrap()
        .unwrap(),
        ServerTcpSessionDisposition::Continue
    ));
    assert_eq!(owned_charge, 0);
    assert_eq!(receive_one(&mut fixture.b).await, first);
    assert_eq!(write_prepared_response(&mut fixture.b).await, second);
    fixture.assert_source(2 * bytes, 2 * bytes, 0);
    assert_eq!(fixture.original_bytes(PathId(0)), 0);
    fixture.acknowledge((2 * bytes) as u64).await;
    fixture.assert_source(2 * bytes, 2 * bytes, 2 * bytes);

    // A genuine sole-stale survivor retains liveness. Retire the exact B
    // membership only after both of its Originals have received Product ACKs.
    let b_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let b_incarnation = fixture
        .owner
        .binding()
        .sender_path_targets(fixture.lane, bytes)
        .into_iter()
        .find(|target| target.observation.key == b_key)
        .unwrap()
        .observation
        .incarnation;
    assert!(
        fixture
            .owner
            .binding()
            .begin_path_detach(b_key, b_instance)
            .is_some()
    );
    fixture
        .owner
        .binding()
        .complete_path_detach(b_key, b_instance, b_incarnation);
    assert!(fixture.owner.binding().output_is_stale(a_identity));
    assert!(
        !fixture
            .path_stream
            .has_nonstale_reinjection_alternative(a_identity, fixture.lane)
    );
    let fallback_payload = Bytes::from(vec![0x63; bytes]);
    fixture.publish(fallback_payload.clone());
    assert_eq!(
        write_prepared_response(&mut fixture.a).await,
        Frame::StreamData {
            stream_id,
            offset: (2 * bytes) as u64,
            payload: fallback_payload,
        }
    );
    fixture.assert_source(3 * bytes, 3 * bytes, 2 * bytes);
    assert!(
        fixture.owner.binding().output_is_stale(a_identity),
        "fallback use does not reactivate Product qualification"
    );
    fixture.acknowledge((3 * bytes) as u64).await;
    fixture.assert_source(3 * bytes, 3 * bytes, 3 * bytes);
}

#[tokio::test]
async fn prepared_tcp_recovery_serves_two_due_ranges_before_fresh_original_without_ack() {
    let mut fixture = PreparedResponseFixture::new().await;
    let stream_id = fixture.path_stream.stream_id;
    let limits = fixture.a.0.context.mux_limits;
    // Keep the fixture below existing startup/resource ceilings. These are
    // three distinct producer assignments, not a test-specific service gain.
    let bytes = 4096.min(fixture.quantum);
    let first_payload = Bytes::from(vec![0x11; bytes]);
    let second_payload = Bytes::from(vec![0x22; bytes]);
    let young_payload = Bytes::from(vec![0x33; bytes]);
    let fresh_payload = Bytes::from(vec![0x44; bytes]);
    let first = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: first_payload.clone(),
    };
    let second = Frame::StreamData {
        stream_id,
        offset: bytes as u64,
        payload: second_payload.clone(),
    };
    let young = Frame::StreamData {
        stream_id,
        offset: (2 * bytes) as u64,
        payload: young_payload.clone(),
    };
    let fresh = Frame::StreamData {
        stream_id,
        offset: (3 * bytes) as u64,
        payload: fresh_payload.clone(),
    };

    fixture.publish(first_payload.clone());
    assert_eq!(write_prepared_response(&mut fixture.a).await, first);
    fixture.publish(second_payload.clone());
    assert_eq!(write_prepared_response(&mut fixture.a).await, second);
    // The existing fixture clock helper makes only these two assignments old.
    // It does not fabricate native service, maturity for the next assignment,
    // or receiver progress. No runtime timeout is modified.
    fixture
        .owner
        .binding()
        .age_original_flights_for_test(Duration::from_secs(10));

    // Exercise the actual callback and its real charge at the intermediate
    // state owned by the TCP writer. The ordinary producer commits one frame
    // before another notice; attempting a second turn here must reject it.
    fixture.publish(young_payload.clone());
    let ReliablePathCommand::PreparedOriginal(work) =
        try_recv_reliable_path_command(&mut fixture.a.0.commands_rx).unwrap()
    else {
        panic!("actual shared source notice");
    };
    let instance = fixture.a.0.path_registration.path_instance_id();
    let ready = fixture
        .a
        .0
        .commands_rx
        .writer_ready_boundary(instance)
        .unwrap();
    let PreparedOriginalClaim::Claimed(owned) = work.try_claim(ready) else {
        panic!("sole owner acquires the young Original");
    };
    assert_eq!(owned, young);
    assert!(!ready.receipt().is_current());
    let mut owned_charge = fixture
        .a
        .0
        .commands_rx
        .register_claimed_writer_frame(&owned);
    fixture.a.0.writer.begin_transaction().unwrap();
    fixture.a.0.writer.stage_transaction_frame(owned).unwrap();
    work.requeue();
    let deferred = Frame::StreamRequalifyData {
        stream_id,
        probe_id: 7,
        offset: 0,
        payload: Bytes::from_static(b"retained inbound probe"),
    };
    fixture.a.0.deferred_input = Some(deferred.clone());
    fixture.publish(fresh_payload.clone());
    fixture.assert_source(4 * bytes, 3 * bytes, 0);
    let attempted = try_recv_reliable_path_command(&mut fixture.a.0.commands_rx).unwrap();
    assert!(matches!(
        &attempted,
        ReliablePathCommand::PreparedOriginal(_)
    ));
    assert!(matches!(
        fixture.a.0.drain_commands(attempted).await,
        Err(RuntimeError::Protocol(
            "server TCP writer cannot replace an uncommitted transaction"
        ))
    ));
    assert_eq!(fixture.a.2.pending_bytes(), owned_charge as u64);
    assert_eq!(fixture.a.2.writer_pending_bytes(), owned_charge as u64);
    assert_eq!(fixture.a.0.deferred_input, Some(deferred.clone()));
    fixture.assert_source(4 * bytes, 3 * bytes, 0);
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(5),
            fixture
                .a
                .0
                .commit_transaction_respecting_deferred_input(&mut owned_charge)
        )
        .await
        .unwrap()
        .unwrap(),
        ServerTcpSessionDisposition::Continue
    ));
    assert_eq!(owned_charge, 0);
    assert_eq!(receive_one(&mut fixture.a).await, young);
    assert_eq!(fixture.a.2.pending_bytes(), 0);
    assert_eq!(fixture.a.2.writer_pending_bytes(), 0);
    assert_eq!(fixture.a.0.deferred_input, Some(deferred.clone()));

    fixture.attach_b();
    fixture.b.0.deferred_input = Some(deferred.clone());
    let target = fixture
        .owner
        .binding()
        .sender_path_targets(TrafficClass::Throughput, bytes)
        .into_iter()
        .find(|target| target.observation.key.path_id == PathId(1))
        .unwrap();
    let identity = ServerReinjectionOutputIdentity {
        key: target.observation.key,
        incarnation: target.observation.incarnation,
    };
    let mut receiver = ReliableRecvStream::new_with_initial_max_offset(
        stream_id,
        limits,
        limits.max_stream_window_bytes,
    );
    for (index, expected) in [&first, &second].into_iter().enumerate() {
        let notice = try_recv_reliable_path_command(&mut fixture.b.0.commands_rx).unwrap();
        assert!(matches!(&notice, ReliablePathCommand::PreparedOriginal(_)));
        let b_instance = fixture.b.0.path_registration.path_instance_id();
        let ready_receipt = fixture
            .b
            .0
            .commands_rx
            .writer_ready_boundary(b_instance)
            .expect("actual idle TCP receiver publishes Ready")
            .receipt();
        assert_eq!(ready_receipt.instance(), b_instance);
        assert!(ready_receipt.is_current());
        let binding = fixture.owner.binding().clone();
        let outputs = binding.prepared_outputs();
        // Resolve using the same supplied-input API as the actual prepared
        // adapter, outside Product ownership. No fixture rate is supplied.
        let inputs = ResponsePreparedNativeInputs::resolve(outputs.clone());
        let ready_outputs = outputs
            .iter()
            .filter(|output| {
                output
                    .commands
                    .writer_boundary()
                    .snapshot()
                    .is_some_and(|receipt| {
                        receipt.instance() == output.identity.path_instance_id
                            && receipt.is_current()
                    })
            })
            .map(|output| output.identity)
            .collect::<Vec<_>>();
        let model_facts = {
            let state = fixture.owner.lock();
            let observed_at = Instant::now();
            let observation = binding
                .observe_prepared_original(
                    &inputs,
                    TrafficClass::Throughput,
                    state.send_stream.next_offset(),
                )
                .expect("exact current supplied output observation");
            let first_query = state.sender.next_prepared_recovery(
                &binding,
                &state.send_stream,
                &state.last_send_ack,
                TrafficClass::Throughput,
                &observation.targets,
                &ready_outputs,
                observed_at,
            );
            let repeated_query = state.sender.next_prepared_recovery(
                &binding,
                &state.send_stream,
                &state.last_send_ack,
                TrafficClass::Throughput,
                &observation.targets,
                &ready_outputs,
                Instant::now(),
            );
            let first_candidate = first_query.candidate.as_ref().map(|candidate| {
                (
                    reliable_stream_frame_extent(&candidate.frame),
                    candidate.target,
                    candidate.cause,
                )
            });
            let repeated_candidate = repeated_query.candidate.as_ref().map(|candidate| {
                (
                    reliable_stream_frame_extent(&candidate.frame),
                    candidate.target,
                    candidate.cause,
                )
            });
            let targets = observation.targets.iter().map(|target| {
                let id = ResponseAcquisitionOutputId::from(target);
                let path = &target.observation;
                format!("id={id:?} ready={} active={} stale={} measured={} qualified={} enqueue={} state={:?} usage={:?} srtt_ms={} rate_bps={} P={} O={} queue={} native_flight={}",
                    ready_outputs.contains(&id), target.product_admission_active,
                    path.stale_for_original_data, path.has_bulk_rate_evidence,
                    path.product_assignment_qualified, target.can_enqueue_reinjection_frame(expected),
                    path.snapshot.state, path.snapshot.peer_usage, path.snapshot.srtt_ms,
                    path.snapshot.delivery_rate_bps, path.snapshot.data_level_limit_bytes,
                    path.original_data_in_flight_bytes, path.snapshot.queue_bytes,
                    path.snapshot.bytes_in_flight)
            }).collect::<Vec<_>>();
            let facts = format!(
                "query={:?} repeat={:?} next_deadline={:?} repeat_deadline={:?} ready={ready_outputs:?} targets={targets:?}",
                first_candidate,
                repeated_candidate,
                first_query.next_deadline,
                repeated_query.next_deadline
            );
            let candidate = first_query
                .candidate
                .as_ref()
                .unwrap_or_else(|| panic!("no pre-drain recovery candidate: {facts}"));
            assert_eq!(&candidate.frame, expected, "{facts}");
            assert_eq!(candidate.target.path_instance_id, b_instance, "{facts}");
            assert_eq!(
                candidate.target.incarnation, identity.incarnation,
                "{facts}"
            );
            let repeated = repeated_query
                .candidate
                .as_ref()
                .unwrap_or_else(|| panic!("same supplied model lost its candidate: {facts}"));
            assert_eq!(candidate.frame, repeated.frame, "{facts}");
            assert_eq!(candidate.target, repeated.target, "{facts}");
            assert_eq!(
                std::mem::discriminant(&candidate.cause),
                std::mem::discriminant(&repeated.cause),
                "{facts}"
            );
            // Each bound tail query refreshes its batch expiry. Full cause
            // equality must not reject this otherwise identical authority.
            assert_ne!(candidate.cause, repeated.cause, "{facts}");
            facts
        };
        drain_one(&mut fixture.b, notice).await;
        fixture.assert_source(4 * bytes, 3 * bytes, 0);
        assert_eq!(fixture.owner.lock().send_stream.data_ack_frontier(), 0);
        assert_eq!(
            fixture
                .owner
                .binding()
                .accepted_reinjected_data_in_flight_bytes_at(identity),
            (index + 1) * bytes,
            "a valid same-Ready query must reach exact copy Apply: {model_facts}"
        );
        assert_eq!(
            fixture.b.2.pending_bytes(),
            reliable_path_frame_pacing_bytes(expected) as u64
        );
        assert_eq!(
            fixture.b.2.writer_pending_bytes(),
            0,
            "RecoveryQueued owns a normal queue command, not an uncharged native frame"
        );
        assert_eq!(fixture.b.0.deferred_input, Some(deferred.clone()));
        let command = try_recv_reliable_path_command(&mut fixture.b.0.commands_rx).unwrap();
        assert!(
            matches!(&command, ReliablePathCommand::SendFrame(frame) if frame == expected),
            "accepted recovery has priority over the requeued fresh source notice"
        );
        drain_one(&mut fixture.b, command).await;
        let received = receive_one(&mut fixture.b).await;
        assert_eq!(&received, expected);
        let Frame::StreamData {
            offset, payload, ..
        } = received
        else {
            unreachable!()
        };
        let expected_payload = payload.clone();
        let delivered = receiver.receive_data(offset, payload).unwrap();
        assert_eq!(delivered.delivered.as_slice(), &[expected_payload]);
        assert_eq!(receiver.next_offset(), ((index + 1) * bytes) as u64);
        assert_eq!(
            fixture.owner.lock().send_stream.data_ack_frontier(),
            0,
            "receiver progress has not been returned as Product ACK authority"
        );
        assert_eq!(fixture.b.2.pending_bytes(), 0);
        assert_eq!(fixture.b.2.writer_pending_bytes(), 0);
        assert_eq!(fixture.b.0.deferred_input, Some(deferred.clone()));
    }

    // Independently young coverage cannot borrow the older ranges' age. The
    // same real writer may now acquire ordinary source under existing credit.
    assert_eq!(write_prepared_response(&mut fixture.b).await, fresh);
    assert_eq!(
        fixture
            .owner
            .binding()
            .accepted_reinjected_data_in_flight_bytes_at(identity),
        2 * bytes
    );
    assert_eq!(fixture.original_bytes(PathId(0)), (3 * bytes) as u64);
    assert_eq!(fixture.original_bytes(PathId(1)), bytes as u64);
    fixture.assert_source(4 * bytes, 4 * bytes, 0);
    assert_eq!(fixture.b.0.deferred_input, Some(deferred));
    assert!(
        receiver
            .receive_data((3 * bytes) as u64, fresh_payload.clone())
            .unwrap()
            .delivered
            .is_empty()
    );
    let delivered = receiver
        .receive_data((2 * bytes) as u64, young_payload.clone())
        .unwrap();
    assert_eq!(
        delivered.delivered.as_slice(),
        &[young_payload, fresh_payload]
    );
    assert_eq!(receiver.next_offset(), (4 * bytes) as u64);
    assert_eq!(fixture.owner.lock().send_stream.data_ack_frontier(), 0);
}

#[tokio::test]
async fn prepared_tcp_latency_completion_reaches_peer_before_owner_fallback() {
    let mut fixture = PreparedResponseFixture::new_with_lane(TrafficClass::Latency).await;
    let payload = Bytes::from_static(b"quiet exchange with no later ACK evidence");
    fixture.publish(payload.clone());
    let original = write_prepared_response(&mut fixture.a).await;
    assert!(
        matches!(&original, Frame::StreamData { offset: 0, payload: sent, .. } if sent == &payload)
    );
    fixture.attach_b();
    let range = crate::protocol::OffsetRange {
        start: 0,
        end: payload.len() as u64,
    };
    let targets = fixture
        .owner
        .binding()
        .sender_path_targets(TrafficClass::Latency, payload.len());
    let (timing, _) = fixture
        .owner
        .binding()
        .observe_prepared_recovery_timing(range, |owner| {
            targets
                .iter()
                .find(|target| target.observation.key == owner.key)
                .map(|target| target.observation.snapshot)
        })
        .unwrap();
    assert!(
        timing.fallback_at > Instant::now(),
        "fresh actual assignment is not yet due"
    );
    let b_instance = fixture.b.0.path_registration.path_instance_id();
    fixture
        .b
        .0
        .commands_rx
        .writer_ready_boundary(b_instance)
        .unwrap();
    let outputs = fixture.owner.binding().prepared_outputs();
    let inputs = ResponsePreparedNativeInputs::resolve(outputs.clone());
    let expected = {
        let state = fixture.owner.lock();
        let observed = fixture
            .owner
            .binding()
            .observe_prepared_original(
                &inputs,
                TrafficClass::Latency,
                state.send_stream.next_offset(),
            )
            .unwrap();
        let ready = outputs
            .iter()
            .filter_map(|output| {
                output
                    .commands
                    .writer_boundary()
                    .snapshot()
                    .map(|_| output.identity)
            })
            .collect::<Vec<_>>();
        let query = state.sender.next_prepared_recovery(
            fixture.owner.binding(),
            &state.send_stream,
            &state.last_send_ack,
            TrafficClass::Latency,
            &observed.targets,
            &ready,
            Instant::now(),
        );
        let facts = observed
            .targets
            .iter()
            .map(|target| {
                (
                    target.observation.key,
                    target.product_admission_active,
                    target.observation.has_bulk_rate_evidence,
                    target.observation.snapshot.srtt_ms,
                    target.observation.snapshot.delivery_rate_bps,
                )
            })
            .collect::<Vec<_>>();
        let candidate = query.candidate.unwrap_or_else(|| {
            panic!(
                "fresh Latency completion: ready={ready:?} targets={facts:?} deadline={:?}",
                query.next_deadline
            )
        });
        assert!(candidate.early_completion_copy);
        assert_eq!(candidate.target.path_instance_id, b_instance);
        candidate.frame
    };
    let Frame::StreamData {
        offset: expected_offset,
        payload: expected_payload,
        ..
    } = &expected
    else {
        panic!("positive completion data");
    };
    assert_eq!(*expected_offset, 0);
    assert!(!expected_payload.is_empty());
    assert!(expected_payload.len() <= payload.len());
    assert_eq!(*expected_payload, payload.slice(..expected_payload.len()));
    let notice = try_recv_reliable_path_command(&mut fixture.b.0.commands_rx).unwrap();
    assert!(matches!(&notice, ReliablePathCommand::PreparedOriginal(_)));
    drain_one(&mut fixture.b, notice).await;
    // RecoveryQueued commits normal queue work. The native writer services
    // that command in its next turn; API acceptance is not peer receipt.
    let target = fixture
        .owner
        .binding()
        .sender_path_targets(TrafficClass::Latency, payload.len())
        .into_iter()
        .find(|target| target.observation.path_instance_id == b_instance)
        .unwrap();
    assert_eq!(
        fixture
            .owner
            .binding()
            .accepted_reinjected_data_in_flight_bytes_at(ServerReinjectionOutputIdentity {
                key: target.observation.key,
                incarnation: target.observation.incarnation
            }),
        expected_payload.len()
    );
    // The successful claim requeues its payload-free notice in the Latency
    // lane, whose existing priority precedes repair. Its spent prefix must
    // yield exactly once before the already accepted copy reaches the writer.
    let notice = try_recv_reliable_path_command(&mut fixture.b.0.commands_rx).unwrap();
    assert!(matches!(&notice, ReliablePathCommand::PreparedOriginal(_)));
    drain_one(&mut fixture.b, notice).await;
    let command = try_recv_reliable_path_command(&mut fixture.b.0.commands_rx).unwrap();
    assert_eq!(
        match &command {
            ReliablePathCommand::SendFrame(frame) => Some(frame),
            _ => None,
        },
        Some(&expected)
    );
    drain_one(&mut fixture.b, command).await;
    let copy = receive_one(&mut fixture.b).await;
    assert_eq!(copy, expected);
    let mut receiver = ReliableRecvStream::new(
        fixture.path_stream.stream_id,
        fixture.a.0.context.mux_limits,
    );
    let Frame::StreamData {
        offset,
        payload: received,
        ..
    } = copy
    else {
        panic!("data copy")
    };
    assert_eq!(
        receiver
            .receive_data(offset, received)
            .unwrap()
            .delivered
            .as_slice(),
        std::slice::from_ref(expected_payload)
    );
    fixture.assert_source(payload.len(), payload.len(), 0);
    assert_eq!(
        fixture.owner.binding().uncopied_completion_prefix(range),
        None
    );
}
