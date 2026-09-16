//! New-model RED/GREEN witness using the actual sender, scoped ACK application,
//! prepared source callback, Ready receipt, native fence and command consumer.
use super::*;

#[tokio::test]
async fn model_trial_reported_frontier_request_partial_ack_refuses_and_max_keeps_evidence() {
    for change in ["interior_ack", "prefix_ack", "owner", "max"] {
        let mut fixture = CreditFrontierFixture::new(true, 4096, false);
        fixture.mature().await;
        fixture.f.send_stream.update_max_offset(4097);
        fixture.f.ack(
            Some(0),
            vec![OffsetRange {
                start: 2048,
                end: 4096,
            }],
        );
        let f = &mut fixture.f;
        let target = f.target;
        let candidate = f
            .query(TrafficClass::Throughput, &[target], Instant::now())
            .candidate
            .unwrap();
        assert!(
            candidate
                .credit_frontier
                .as_ref()
                .unwrap()
                .reported_gap
                .is_some()
        );
        match change {
            "interior_ack" => f.ack(
                Some(0),
                vec![OffsetRange {
                    start: 512,
                    end: 1024,
                }],
            ),
            "prefix_ack" => f.ack(
                Some(0),
                vec![OffsetRange {
                    start: 0,
                    end: 1024,
                }],
            ),
            "owner" => drop(f.remotes.remove_path_instance(f.owner)),
            "max" => f.send_stream.update_max_offset(8192),
            _ => unreachable!(),
        }
        if change == "interior_ack" {
            assert_eq!(f.send_stream.data_ack_frontier(), 0);
        }
        let before = f.sender.optional_reinjection.reinjected_bytes();
        let copy_before = f
            .sender
            .multipath
            .accepted_reinjected_data_bytes(fixture.copy);
        let ready = f
            .target_receivers
            .writer_ready_boundary(target.path_instance_id)
            .unwrap();
        let result = f.sender.commit_prepared_recovery(
            &f.context,
            &mut f.remotes,
            &f.send_stream,
            &f.queue,
            &candidate,
            ready,
            None,
        );
        if change == "max" {
            result.expect("MAX growth cannot erase the explicitly reported retained head");
            let command = try_recv_reliable_path_command(&mut f.target_receivers).unwrap();
            assert!(
                matches!(&command,ReliablePathCommand::SendFrame(frame) if *frame==candidate.frame)
            );
            f.target_receivers
                .release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
            let ready = f
                .target_receivers
                .writer_ready_boundary(target.path_instance_id)
                .unwrap();
            assert!(
                f.sender
                    .commit_prepared_recovery(
                        &f.context,
                        &mut f.remotes,
                        &f.send_stream,
                        &f.queue,
                        &candidate,
                        ready,
                        None
                    )
                    .is_err()
            );
            assert!(ready.receipt().is_current());
        } else {
            assert!(result.is_err(), "stale {change} proof cannot publish");
            assert!(ready.receipt().is_current());
            assert_eq!(f.sender.optional_reinjection.reinjected_bytes(), before);
            assert!(try_recv_reliable_path_command(&mut f.target_receivers).is_none());
        }
        assert_eq!(
            f.sender
                .multipath
                .accepted_reinjected_data_bytes(fixture.copy),
            copy_before
        );
    }
}

#[tokio::test]
async fn model_trial_reported_frontier_request_preserves_actual_apply_and_slot_debt() {
    for quic in [false, true] {
        // Establish the retained immutable fallback through the existing real
        // credit-frontier query, then reopen credit. No timer is manufactured.
        let mut fixture = CreditFrontierFixture::new(true, 4096, quic);
        fixture.mature().await;
        fixture.f.send_stream.update_max_offset(4097);
        let target = fixture.f.target;
        assert!(
            fixture
                .f
                .query(TrafficClass::Throughput, &[target], Instant::now())
                .candidate
                .is_none(),
            "spare credit alone does not authorize another copy"
        );
        fixture.f.ack(
            Some(0),
            vec![OffsetRange {
                start: 2048,
                end: 4096,
            }],
        );
        assert_eq!(
            fixture.f.ack.gap_at(0),
            Some(OffsetRange {
                start: 0,
                end: 2048
            })
        );
        assert_eq!(fixture.f.send_stream.data_ack_frontier(), 0);
        assert_eq!(fixture.f.send_stream.peer_max_offset(), 4097);
        assert!(Instant::now() < fixture.copy_deadline);
        let candidate = fixture.f.query(TrafficClass::Throughput, &[target], Instant::now())
            .candidate.expect("model trial: mature reported head uses an otherwise-vacant target despite spare credit");
        assert_eq!(
            reliable_stream_frame_extent(&candidate.frame),
            Some((0, 2048, 2048))
        );
        assert!(!candidate.early_completion_copy);
        assert!(candidate.credit_frontier.is_some());
        let CreditFrontierFixture {
            mut f,
            copy,
            _copy_receivers,
            ..
        } = fixture;
        let stream_id = f.send_stream.stream_id();
        let before_accounted = f.sender.optional_reinjection.reinjected_bytes();
        let shared = SharedRequestProduct::new(RequestProductState {
            sender: f.sender,
            sender_queue: f.queue,
            send_stream: f.send_stream,
            last_send_ack: f.ack,
            remotes: f.remotes,
            prepared: RequestPreparedSource::new(TrafficClass::Throughput, 4096),
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
                .expect("real prepared writer notice");
            match command {
                ReliablePathCommand::PreparedOriginal(work) => break work,
                command @ ReliablePathCommand::SendFrame(Frame::PathProofData { .. }) => f
                    .target_receivers
                    .release_pending_command_bytes(reliable_path_command_pending_bytes(&command)),
                _ => panic!("only a proof may precede prepared work"),
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
        assert_eq!(state.send_stream.peer_max_offset(), 4097);
        assert_eq!(state.send_stream.reinjection_bytes(), 2048);
        assert_eq!(
            state.sender.multipath.accepted_reinjected_data_bytes(copy),
            2048
        );
        assert_eq!(
            state
                .sender
                .multipath
                .accepted_reinjected_data_bytes(target),
            2048
        );
        assert_eq!(
            state.sender.optional_reinjection.reinjected_bytes() - before_accounted,
            2048
        );
        drop(state);
        let command = loop {
            let command = try_recv_reliable_path_command(&mut f.target_receivers)
                .expect("the prepared success published a real native command");
            if matches!(
                &command,
                ReliablePathCommand::SendFrame(Frame::PathProofData { .. })
            ) {
                f.target_receivers
                    .release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
            } else {
                break command;
            }
        };
        assert!(matches!(&command,
            ReliablePathCommand::SendFrame(Frame::StreamData { stream_id: actual, offset: 0, payload })
            if *actual==stream_id && payload.len()==2048));
        f.target_receivers
            .release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    }
}
