use super::*;
use crate::runtime::path::prepared::PreparedOriginalClaim;
use crate::runtime::sender::request::{
    RequestPreparedSource, RequestProductState, SharedRequestProduct,
};

struct CreditFrontierFixture {
    f: PreparedCompletionFixture,
    copy: RelayPathInstance,
    _copy_receivers: crate::runtime::path::commands::ReliablePathCommandReceivers,
    copy_deadline: Instant,
}

impl CreditFrontierFixture {
    fn new(credit_exhausted: bool, copy_bytes: usize, quic_target: bool) -> Self {
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10441?initial-srtt-s=0.02&initial-rate-mbps=200",
            "tcp://127.0.0.1:10442?initial-srtt-s=0.02&initial-rate-mbps=200",
            "tcp://127.0.0.1:10443?initial-srtt-s=1&initial-rate-mbps=200",
            "quic://127.0.0.1:10444?initial-srtt-s=0.02&initial-rate-mbps=500",
        ]);
        let mut f = PreparedCompletionFixture::with_initial_credit(
            context,
            if credit_exhausted { 4096 } else { 4097 },
            4096,
        );
        seed_client_bulk_evidence_for_test(&f.context, f.owner);
        if quic_target {
            drop(f.remotes.remove_path_instance(f.target));
            let (commands, mut receivers) = reliable_path_command_channels(8);
            let (opened, _) = opened_test_relay_stream_with_native_source(
                f.remotes.stream_id(),
                UnderlayProtocol::Udp,
                0,
                commands,
                crate::transport::RateHint::BitsPerSecond(500_000_000),
                31,
                Some(500_000_000),
            );
            f.remotes.attach_candidate(opened);
            consume_client_path_proof_for_test(&mut receivers);
            f.target = f
                .remotes
                .paths
                .iter()
                .find(|path| path.key().underlay == UnderlayProtocol::Udp)
                .unwrap()
                .instance();
            f.target_receivers = receivers;
        }
        seed_client_bulk_evidence_for_test(&f.context, f.target);
        let (commands, mut copy_receivers) = reliable_path_command_channels(8);
        f.remotes
            .attach_candidate(opened_test_relay_stream(f.remotes.stream_id(), 2, commands));
        consume_client_path_proof_for_test(&mut copy_receivers);
        let copy = f
            .remotes
            .paths
            .iter()
            .find(|path| path.key().underlay == UnderlayProtocol::Tcp && path.key().index == 2)
            .unwrap()
            .instance();
        f.context.install_relay_path_instance_for_test(copy);
        f.context
            .mark_tcp_path_open_success(2, Duration::from_secs(1), TrafficClass::Latency);
        let mut candidate = f
            .query(TrafficClass::Latency, &[copy], Instant::now())
            .candidate
            .expect("normal initial completion may use the distinct slow copy target");
        assert!(candidate.early_completion_copy);
        // Exercise a real smaller accepted range using the same single-target
        // plan, without manufacturing copy ledger state or reserved byte debt.
        candidate.frame = data_frame(f.remotes.stream_id(), 0, copy_bytes);
        let ready = copy_receivers
            .writer_ready_boundary(copy.path_instance_id)
            .unwrap();
        let copy_deadline = f
            .sender
            .commit_prepared_recovery(
                &f.context,
                &mut f.remotes,
                &f.send_stream,
                &f.queue,
                &candidate,
                ready,
                None,
            )
            .unwrap();
        assert_eq!(
            f.sender.multipath.accepted_reinjected_data_bytes(copy),
            copy_bytes
        );
        let command = try_recv_reliable_path_command(&mut copy_receivers)
            .expect("actual copy command committed");
        assert!(
            matches!(&command, ReliablePathCommand::SendFrame(frame) if *frame == candidate.frame)
        );
        copy_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
        Self {
            f,
            copy,
            _copy_receivers: copy_receivers,
            copy_deadline,
        }
    }

    async fn mature(&mut self) {
        let target = self.f.target;
        let query = self
            .f
            .query(TrafficClass::Throughput, &[target], Instant::now());
        assert!(query.candidate.is_none());
        let fallback = query
            .next_deadline
            .expect("covered frontier arms original fallback before copy suppression");
        assert!(fallback < self.copy_deadline);
        tokio::time::sleep_until(tokio::time::Instant::from_std(fallback)).await;
        seed_client_bulk_evidence_for_test(&self.f.context, target);
        assert!(
            Instant::now() < self.copy_deadline,
            "old copy remains globally suppressed in this fixture"
        );
    }

    fn candidate(&mut self) -> super::super::super::RequestPreparedRecoveryCandidate {
        let target = self.f.target;
        self.f
            .query(TrafficClass::Throughput, &[target], Instant::now())
            .candidate
            .expect(
                "mature covered frontier with no assignment credit has a vacant measured target",
            )
    }
}

#[tokio::test]
async fn request_credit_frontier_requires_zero_assignment_credit_and_original_fallback() {
    let mut positive = CreditFrontierFixture::new(false, 4096, false);
    let target = positive.f.target;
    let later = Instant::now() + Duration::from_millis(500);
    assert!(
        positive
            .f
            .query(TrafficClass::Throughput, &[target], later)
            .candidate
            .is_none(),
        "one byte of true assignment credit retains ordinary global suppression"
    );
    let mut exhausted = CreditFrontierFixture::new(true, 4096, false);
    let target = exhausted.f.target;
    let before = exhausted
        .f
        .query(TrafficClass::Latency, &[target], Instant::now());
    assert!(
        before.candidate.is_none(),
        "Latency cannot borrow early completion for an already copied credit frontier"
    );
    assert!(
        before
            .next_deadline
            .is_some_and(|deadline| deadline < exhausted.copy_deadline)
    );
    exhausted.mature().await;
    let candidate = exhausted.candidate();
    assert!(candidate.credit_frontier.is_some());
    assert!(!candidate.early_completion_copy);
    assert_eq!(
        reliable_stream_frame_extent(&candidate.frame),
        Some((0, 4096, 4096))
    );
    assert_eq!(candidate.target, target);
}

#[tokio::test]
async fn request_credit_frontier_real_prepared_claim_preserves_original_and_copy_debt() {
    for quic in [false, true] {
        let mut fixture = CreditFrontierFixture::new(true, 4096, quic);
        fixture.mature().await;
        let CreditFrontierFixture {
            mut f,
            copy,
            _copy_receivers,
            ..
        } = fixture;
        let target = f.target;
        let stream_id = f.send_stream.stream_id();
        let prepared = RequestPreparedSource::new(TrafficClass::Throughput, 4096);
        let shared = SharedRequestProduct::new(RequestProductState {
            sender: f.sender,
            sender_queue: f.queue,
            send_stream: f.send_stream,
            last_send_ack: f.ack,
            remotes: f.remotes,
            prepared,
        });
        let _actor_lifetime = shared.actor_lifetime();
        crate::runtime::relay::control::publish_prepared_request_work(
            &mut shared.lock(),
            &shared,
            &f.context,
            TrafficClass::Throughput,
            4096,
            true,
        );
        let work = loop {
            let command = try_recv_reliable_path_command(&mut f.target_receivers)
                .expect("actual prepared writer notice");
            match command {
                ReliablePathCommand::PreparedOriginal(work) => break work,
                command @ ReliablePathCommand::SendFrame(Frame::PathProofData { .. }) => f
                    .target_receivers
                    .release_pending_command_bytes(reliable_path_command_pending_bytes(&command)),
                _ => panic!("only a proof may precede the prepared notice"),
            }
        };
        let ready = f
            .target_receivers
            .writer_ready_boundary(target.path_instance_id)
            .unwrap();
        assert!(matches!(
            work.try_claim(ready),
            PreparedOriginalClaim::RecoveryQueued
        ));
        let state = shared.lock();
        assert_eq!(state.send_stream.next_offset(), 4096);
        assert_eq!(state.send_stream.peer_max_offset(), 4096);
        assert_eq!(state.send_stream.reinjection_bytes(), 4096);
        assert_eq!(state.sender_queue.data_bytes(), 0);
        assert_eq!(
            state.sender.multipath.accepted_reinjected_data_bytes(copy),
            4096
        );
        assert_eq!(
            state
                .sender
                .multipath
                .accepted_reinjected_data_bytes(target),
            4096
        );
        assert_eq!(state.sender.optional_reinjection.reinjected_bytes(), 8192);
        drop(state);
        let command = loop {
            let command = try_recv_reliable_path_command(&mut f.target_receivers)
                .expect("actual frontier repair carrier command");
            if matches!(
                &command,
                ReliablePathCommand::SendFrame(Frame::PathProofData { .. })
            ) {
                f.target_receivers
                    .release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
                continue;
            }
            break command;
        };
        assert!(
            matches!(&command, ReliablePathCommand::SendFrame(Frame::StreamData { stream_id: actual, offset: 0, payload }) if *actual == stream_id && payload.len() == 4096)
        );
        f.target_receivers
            .release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    }
}

#[tokio::test]
async fn request_credit_frontier_final_apply_rejects_reopened_credit_and_changed_frontier() {
    for change in ["credit", "ack", "owner"] {
        let mut fixture = CreditFrontierFixture::new(true, 4096, false);
        fixture.mature().await;
        let candidate = fixture.candidate();
        let f = &mut fixture.f;
        match change {
            "credit" => f.send_stream.update_max_offset(4097),
            "ack" => f.ack(
                Some(0),
                vec![OffsetRange {
                    start: 0,
                    end: 1024,
                }],
            ),
            "owner" => drop(f.remotes.remove_path_instance(f.owner)),
            _ => unreachable!(),
        }
        let before = f.send_stream.clone();
        let before_copy = f
            .sender
            .multipath
            .accepted_reinjected_data_bytes(fixture.copy);
        let ready = f
            .target_receivers
            .writer_ready_boundary(f.target.path_instance_id)
            .unwrap();
        assert!(
            matches!(
                f.sender.commit_prepared_recovery(
                    &f.context,
                    &mut f.remotes,
                    &f.send_stream,
                    &f.queue,
                    &candidate,
                    ready,
                    None
                ),
                Err(RuntimeError::SenderServiceBlocked)
            ),
            "stale {change} proof cannot publish"
        );
        assert!(ready.receipt().is_current());
        assert_eq!(f.send_stream, before);
        assert_eq!(
            f.sender.multipath.accepted_reinjected_data_bytes(f.target),
            0
        );
        assert_eq!(
            f.sender
                .multipath
                .accepted_reinjected_data_bytes(fixture.copy),
            before_copy
        );
        assert!(try_recv_reliable_path_command(&mut f.target_receivers).is_none());
    }
}

#[tokio::test]
async fn request_credit_frontier_respects_ready_and_never_renews_occupied_target() {
    let mut fixture = CreditFrontierFixture::new(true, 4096, false);
    fixture.mature().await;
    let candidate = fixture.candidate();
    let f = &mut fixture.f;
    let wrong = f
        .owner_receivers
        .writer_ready_boundary(f.owner.path_instance_id)
        .unwrap();
    assert!(matches!(
        f.sender.commit_prepared_recovery(
            &f.context,
            &mut f.remotes,
            &f.send_stream,
            &f.queue,
            &candidate,
            wrong,
            None
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(wrong.receipt().is_current());
    let ready = f
        .target_receivers
        .writer_ready_boundary(f.target.path_instance_id)
        .unwrap();
    f.sender
        .commit_prepared_recovery(
            &f.context,
            &mut f.remotes,
            &f.send_stream,
            &f.queue,
            &candidate,
            ready,
            None,
        )
        .unwrap();
    let command = try_recv_reliable_path_command(&mut f.target_receivers).unwrap();
    f.target_receivers
        .release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    let ready = f
        .target_receivers
        .writer_ready_boundary(f.target.path_instance_id)
        .unwrap();
    assert!(matches!(
        f.sender.commit_prepared_recovery(
            &f.context,
            &mut f.remotes,
            &f.send_stream,
            &f.queue,
            &candidate,
            ready,
            None
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(ready.receipt().is_current());
    assert_eq!(
        f.sender.multipath.accepted_reinjected_data_bytes(f.target),
        4096
    );
    assert_eq!(
        f.sender
            .multipath
            .accepted_reinjected_data_bytes(fixture.copy),
        4096
    );
    let target = f.target;
    assert!(
        f.query(TrafficClass::Throughput, &[target], Instant::now())
            .candidate
            .is_none()
    );
}

#[tokio::test]
async fn request_credit_frontier_immaturity_preserves_normal_later_completion() {
    let mut fixture = CreditFrontierFixture::new(true, 1024, false);
    let target = fixture.f.target;
    let candidate = fixture
        .f
        .query(TrafficClass::Latency, &[target], Instant::now())
        .candidate
        .expect("immature covered head cannot block ordinary uncovered completion");
    assert!(candidate.credit_frontier.is_none());
    assert!(candidate.early_completion_copy);
    assert_eq!(
        reliable_stream_frame_extent(&candidate.frame),
        Some((1024, 4096, 3072))
    );
}

#[tokio::test]
async fn request_credit_frontier_occupied_head_preserves_normal_later_recovery() {
    let mut fixture = CreditFrontierFixture::new(true, 1024, false);
    fixture.mature().await;
    let candidate = fixture.candidate();
    assert!(candidate.credit_frontier.is_some());
    assert_eq!(
        reliable_stream_frame_extent(&candidate.frame),
        Some((0, 1024, 1024))
    );
    let f = &mut fixture.f;
    let target = f.target;
    let ready = f
        .target_receivers
        .writer_ready_boundary(target.path_instance_id)
        .unwrap();
    f.sender
        .commit_prepared_recovery(
            &f.context,
            &mut f.remotes,
            &f.send_stream,
            &f.queue,
            &candidate,
            ready,
            None,
        )
        .unwrap();
    let command = try_recv_reliable_path_command(&mut f.target_receivers).unwrap();
    f.target_receivers
        .release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    let later = f
        .query(TrafficClass::Throughput, &[target], Instant::now())
        .candidate
        .expect("occupied head cannot hide a later independently due normal repair");
    assert!(later.credit_frontier.is_none());
    assert!(!later.early_completion_copy);
    assert_eq!(
        reliable_stream_frame_extent(&later.frame),
        Some((1024, 4096, 3072))
    );
    let ready = f
        .target_receivers
        .writer_ready_boundary(target.path_instance_id)
        .unwrap();
    f.sender
        .commit_prepared_recovery(
            &f.context,
            &mut f.remotes,
            &f.send_stream,
            &f.queue,
            &later,
            ready,
            None,
        )
        .unwrap();
    assert_eq!(
        f.sender.multipath.accepted_reinjected_data_bytes(target),
        4096
    );
    assert_eq!(
        f.sender
            .multipath
            .accepted_reinjected_data_bytes(fixture.copy),
        1024
    );
}
