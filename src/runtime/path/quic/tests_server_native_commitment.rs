use super::*;
use crate::mux::stream::ReliableRecvStream;
use crate::runtime::path::prepared::PreparedOriginalClaim;
use std::task::Poll;

/// Diagnostic counterexample for the retained wait's notification, not a
/// throughput test. An independent observer proves the real native crossing;
/// neither arm polls the command receiver again before recording its wakes.
#[tokio::test(flavor = "current_thread")]
async fn server_quic_native_wait_noop_repoll_diagnostic_loses_writer_wake() {
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};

    #[derive(Default)]
    struct WriterWake(AtomicUsize);

    impl Wake for WriterWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn observe_crossing(overwrite: bool) -> usize {
        let stream_id = StreamId(410);
        let (mut fixture, _accepted_rx) =
            ServerUdpTerminalWriterFixture::open_with_native_authority(stream_id, None, true).await;
        fixture.drain_zero_credit_admission().await;
        let commitment = fixture
            .server_send
            .as_mut()
            .unwrap()
            .bind_native_commitment()
            .unwrap();
        fixture
            .commands_rx
            .as_mut()
            .unwrap()
            .bind_native_commitment(commitment.clone())
            .unwrap();
        let payload = Bytes::from_static(b"source retained through the native wait");
        let (owner, _stream) = fixture.publish_latency_source(payload.clone()).await;

        // A real H3 control predecessor creates E > P without an Original
        // flight, so no Product recovery deadline can supply an unrelated wake.
        // This fixture starts no Product reader or periodic metrics actor.
        let control = Frame::Ping { nonce: 410 };
        let mut write = Box::pin(udp_path_write_frame(
            fixture.server_send.as_mut().unwrap(),
            &control,
            fixture.context.codec_limits,
        ));
        assert!(matches!(futures::poll!(&mut write), Poll::Ready(Ok(()))));
        drop(write);
        let barrier = commitment
            .capture()
            .unwrap()
            .barrier()
            .expect("accepted H3 predecessor");
        let native_end = barrier.native_end();

        let authority = fixture.commands_tx.native_rate_authority().unwrap();
        let scope = crate::model::carrier_rate_authority::CarrierRateAuthorityScope::new(
            fixture._path_registration.path_instance_id(),
            crate::protocol::PathMetricDirection::ServerToClient,
        );
        let shape = authority.refresh_scheduling_shape(scope).unwrap();
        assert!(
            authority
                .commit_if_current(shape.stamp(), || {
                    fixture
                        .context
                        .reliable_streams
                        .stage_native_scheduling_shape(&fixture._path_registration, shape)
                })
                .unwrap()
        );
        fixture
            .context
            .reliable_streams
            .fanout_native_scheduling_shape(&fixture._path_registration, shape);
        let commands = fixture.commands_rx.as_mut().unwrap();
        let ReliablePathCommand::PreparedOriginal(work) =
            try_recv_reliable_path_priority_command(commands).expect("actual source notice")
        else {
            panic!("expected prepared Original notice");
        };
        let ready = commands
            .writer_ready_boundary(fixture._path_registration.path_instance_id())
            .unwrap();
        let receipt = ready.receipt();
        let PreparedOriginalClaim::Blocked(wait) = work.try_claim(ready) else {
            panic!("native predecessor must block this actual Original claim");
        };
        commands.defer_prepared_work(work, wait);

        // A real async receiver poll arms the actual deferred work. Cancelling
        // this receive only releases its borrow: the queue retains the wait,
        // just as when the actor's competing input branch wins its select.
        let wakes = Arc::new(WriterWake::default());
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let mut receive = Box::pin(recv_reliable_path_command(commands));
        assert!(receive.as_mut().poll(&mut cx).is_pending());
        drop(receive);
        assert!(
            tokio::task::coop::has_budget_remaining(),
            "isolate wake forwarding from coop suspension"
        );
        wakes.0.store(0, Ordering::SeqCst);
        if overwrite {
            assert!(try_recv_reliable_path_priority_command(commands).is_none());
        }
        assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
        assert!(receipt.is_current());
        {
            let state = owner.lock();
            assert_eq!(state.sender.data_bytes(), payload.len());
            assert_eq!(state.send_stream.next_offset(), 0);
            assert_eq!(state.send_stream.reinjection_bytes(), 0);
        }

        // No yield has occurred since acceptance, so neither arm can miss the
        // transition before its counting waker is installed. This separate
        // native waiter uses the test task's waker, never the writer counter.
        let progress =
            tokio::time::timeout(Duration::from_secs(5), barrier.wait_until_packetized())
                .await
                .expect("existing fixture native crossing watchdog")
                .unwrap();
        assert_eq!(progress.accepted_end, native_end);
        assert_eq!(progress.first_unpacketized, native_end);
        let crossing_wakes = wakes.0.load(Ordering::SeqCst);

        // Only now inspect the receiver. Even the overwritten arm retains a
        // ready wait and the same unclaimed source; this manual poll must not
        // be mistaken for evidence that native packetization woke its writer.
        let notice = try_recv_reliable_path_priority_command(fixture.commands_rx.as_mut().unwrap())
            .expect("manual repoll recovers the retained prepared notice");
        assert!(matches!(notice, ReliablePathCommand::PreparedOriginal(_)));
        assert!(receipt.is_current());
        let state = owner.lock();
        assert_eq!(state.sender.data_bytes(), payload.len());
        assert_eq!(state.send_stream.next_offset(), 0);
        assert_eq!(state.send_stream.reinjection_bytes(), 0);
        assert_eq!(fixture.commands_tx.pending_bytes(), 0);
        assert_eq!(fixture.commands_tx.writer_pending_bytes(), 0);
        crossing_wakes
    }

    let retained_real_waker = observe_crossing(false).await;
    let overwritten_by_noop = observe_crossing(true).await;
    assert!(
        retained_real_waker > 0,
        "real native crossing must wake the armed writer"
    );
    // This diagnostic deliberately documents the current defect. A correction
    // must change this arm to require a positive wake, not discard its wait.
    assert_eq!(
        overwritten_by_noop, 0,
        "synchronous noop repoll loses writer notification"
    );
}

/// Exercise the actual shared-source notice and ordinary H3 writer. This child
/// uses the existing server fixture; all native coordinates come from its real
/// observer and successful adapter write, without reconstructing framing sizes.
#[tokio::test(flavor = "current_thread")]
async fn server_quic_prepared_original_waits_for_unacknowledged_native_bytes() {
    let stream_id = StreamId(409);
    let (mut fixture, _accepted_rx) =
        ServerUdpTerminalWriterFixture::open_with_native_authority(stream_id, None, true).await;
    fixture.drain_zero_credit_admission().await;
    let commitment = fixture
        .server_send
        .as_mut()
        .unwrap()
        .bind_native_commitment()
        .unwrap();
    fixture
        .commands_rx
        .as_mut()
        .unwrap()
        .bind_native_commitment(commitment.clone())
        .unwrap();
    let payload = Bytes::from_static(b"fresh source after packetization");
    let prefix = Bytes::from_static(b"still unacknowledged predecessor");
    let (owner, _stream) = fixture.publish_latency_source(payload.clone()).await;
    let binding = owner.binding();
    let key = binding.sender_path_targets(TrafficClass::Latency, payload.len())[0]
        .observation
        .key;
    let mut receiver = ReliableRecvStream::new(stream_id, fixture.context.mux_limits);

    // An earlier accepted Product assignment is still unacknowledged. Native
    // acceptance must close the next Original opportunity before Product ACK.
    let late_original = {
        let mut state = owner.lock();
        let frame = state.send_stream.send_data(prefix.clone()).unwrap();
        binding.record_original_flight(key, &frame);
        assert_eq!(state.send_stream.data_ack_frontier(), 0);
        assert_eq!(state.send_stream.reinjection_bytes(), prefix.len());
        frame
    };

    // Do not yield between acceptance and the prepared claim: the actual Quinn
    // driver cannot packetize this new H3 operation on a current-thread runtime.
    let mut write = Box::pin(udp_path_write_frame(
        fixture.server_send.as_mut().unwrap(),
        &late_original,
        fixture.context.codec_limits,
    ));
    assert!(matches!(futures::poll!(&mut write), Poll::Ready(Ok(()))));
    drop(write);
    let barrier = commitment
        .capture()
        .unwrap()
        .barrier()
        .expect("unpacketized native work blocks even without a Product ACK");
    let predecessor_native_end = barrier.native_end();
    let mut crossing = Box::pin(barrier.wait_until_packetized());
    assert!(futures::poll!(&mut crossing).is_pending());

    // Publish the actual current Native shape through the same stage/fanout
    // authorities as the ordinary cadence service. No synthetic rate, stamp,
    // or pacing allowance is introduced to make Original selection succeed.
    let authority = fixture.commands_tx.native_rate_authority().unwrap();
    let scope = crate::model::carrier_rate_authority::CarrierRateAuthorityScope::new(
        fixture._path_registration.path_instance_id(),
        crate::protocol::PathMetricDirection::ServerToClient,
    );
    let shape = authority.refresh_scheduling_shape(scope).unwrap();
    assert!(
        authority
            .commit_if_current(shape.stamp(), || {
                fixture
                    .context
                    .reliable_streams
                    .stage_native_scheduling_shape(&fixture._path_registration, shape)
            })
            .unwrap()
    );
    fixture
        .context
        .reliable_streams
        .fanout_native_scheduling_shape(&fixture._path_registration, shape);

    let instance = fixture._path_registration.path_instance_id();
    let commands = fixture.commands_rx.as_mut().unwrap();
    let notice = try_recv_reliable_path_priority_command(commands)
        .expect("the actual publisher queued a payload-free latency notice");
    let ReliablePathCommand::PreparedOriginal(work) = notice else {
        panic!("expected actual shared-source notice");
    };
    let ready = commands.writer_ready_boundary(instance).unwrap();
    let receipt = ready.receipt();
    let PreparedOriginalClaim::Blocked(wait) = work.try_claim(ready) else {
        panic!("unacknowledged native predecessor must refuse the actual Original claim");
    };
    assert!(receipt.is_current(), "refusal does not consume Ready");
    commands.defer_prepared_work(work, wait);
    assert!(try_recv_reliable_path_priority_command(commands).is_none());
    {
        let state = owner.lock();
        assert_eq!(state.sender.data_bytes(), payload.len());
        assert_eq!(state.send_stream.next_offset(), prefix.len() as u64);
        assert_eq!(state.send_stream.data_ack_frontier(), 0);
        assert_eq!(state.send_stream.reinjection_bytes(), prefix.len());
        assert!(state.prepared.pending_error.is_none());
    }
    assert_eq!(
        binding.sender_path_targets(TrafficClass::Latency, payload.len())[0]
            .observation
            .original_data_in_flight_bytes,
        prefix.len() as u64
    );
    assert_eq!(fixture.commands_tx.pending_bytes(), 0);
    assert_eq!(fixture.commands_tx.writer_pending_bytes(), 0);
    assert!(futures::poll!(&mut crossing).is_pending());
    drop(crossing);

    // The existing one-command control escape remains executable while fresh
    // source is blocked. Normal batching deliberately yields to coalesce, so it
    // cannot be used to keep Quinn unpolled for this isolated crossing proof.
    let control = Frame::Ping { nonce: 409 };
    fixture
        .commands_tx
        .try_enqueue_admitted_frame(control.clone(), TrafficClass::Control)
        .unwrap();
    let command = try_recv_reliable_path_priority_command(fixture.commands_rx.as_mut().unwrap())
        .expect("queued control bypasses the parked Original notice");
    assert!(matches!(&command, ReliablePathCommand::SendFrame(frame) if frame == &control));
    let mut control_proofs = PathProofTracker::default();
    let mut control_write = Box::pin(drain_one_server_udp_command_while_input_deferred(
        command,
        fixture.commands_rx.as_mut().unwrap(),
        fixture.server_send.as_mut().unwrap(),
        &fixture.context,
        stream_id,
        &fixture._path_registration,
        &fixture.retirement,
        &mut control_proofs,
    ));
    let control_poll = futures::poll!(&mut control_write);
    assert!(
        matches!(control_poll, Poll::Ready(Ok(false))),
        "single-command control producer: {control_poll:?}"
    );
    drop(control_write);
    assert_eq!(fixture.commands_tx.pending_bytes(), 0);
    assert_eq!(fixture.commands_tx.writer_pending_bytes(), 0);
    {
        let state = owner.lock();
        assert_eq!(state.sender.data_bytes(), payload.len());
        assert_eq!(state.send_stream.next_offset(), prefix.len() as u64);
        assert_eq!(state.send_stream.data_ack_frontier(), 0);
        assert_eq!(state.send_stream.reinjection_bytes(), prefix.len());
    }
    let barrier = commitment.capture().unwrap().barrier().unwrap();
    assert!(barrier.native_end() > predecessor_native_end);
    let mut crossing = Box::pin(barrier.wait_until_packetized());
    assert!(futures::poll!(&mut crossing).is_pending());
    // Settle a possible control-service notification before testing the Native
    // crossing wake. An unchanged unpacketized queue must still park the source.
    let commands = fixture.commands_rx.as_mut().unwrap();
    if let Some(command) = try_recv_reliable_path_priority_command(commands) {
        let ReliablePathCommand::PreparedOriginal(work) = command else {
            panic!("only the existing weak source notice can remain");
        };
        let ready = commands.writer_ready_boundary(instance).unwrap();
        let PreparedOriginalClaim::Blocked(wait) = work.try_claim(ready) else {
            panic!("control completion does not make unpacketized data an empty FIFO");
        };
        commands.defer_prepared_work(work, wait);
    }
    assert!(try_recv_reliable_path_priority_command(commands).is_none());

    tokio::time::timeout(Duration::from_secs(5), crossing)
        .await
        .expect("actual native packetization crossing")
        .unwrap();
    assert!(commitment.capture().unwrap().barrier().is_none());
    let mut notice = Some(
        try_recv_reliable_path_priority_command(fixture.commands_rx.as_mut().unwrap())
            .expect("crossing wakes and requeues the same weak prepared notice"),
    );
    assert!(matches!(
        notice,
        Some(ReliablePathCommand::PreparedOriginal(_))
    ));

    // Use the existing Native cadence while the actual writer completes any
    // genuine publication retry, then verify the new offset and receiver data.
    {
        let metrics = crate::runtime::path::quic::metrics::run_server_quic_path_metrics(
            fixture.context.clone(),
            fixture._path_registration.clone(),
            fixture._server_connection.clone(),
        );
        let service = async {
            let expected_end = (prefix.len() + payload.len()) as u64;
            while owner.lock().send_stream.next_offset() != expected_end {
                let command = match notice.take() {
                    Some(command) => command,
                    None => recv_reliable_path_command(fixture.commands_rx.as_mut().unwrap())
                        .await
                        .expect("Native cadence retries the same source notice"),
                };
                assert!(matches!(&command, ReliablePathCommand::PreparedOriginal(_)));
                assert!(!fixture.drain_normal_command(command).await.unwrap());
            }
            assert_eq!(
                udp_path_read_frame(
                    fixture.client_recv.as_mut().unwrap(),
                    fixture.context.codec_limits
                )
                .await
                .unwrap(),
                late_original
            );
            assert_eq!(
                receiver
                    .receive_data(0, prefix.clone())
                    .unwrap()
                    .delivered
                    .as_slice(),
                std::slice::from_ref(&prefix)
            );
            assert_eq!(
                udp_path_read_frame(
                    fixture.client_recv.as_mut().unwrap(),
                    fixture.context.codec_limits
                )
                .await
                .unwrap(),
                control
            );
            let fresh = udp_path_read_frame(
                fixture.client_recv.as_mut().unwrap(),
                fixture.context.codec_limits,
            )
            .await
            .unwrap();
            assert_eq!(
                fresh,
                Frame::StreamData {
                    stream_id,
                    offset: prefix.len() as u64,
                    payload: payload.clone(),
                }
            );
            assert_eq!(
                receiver
                    .receive_data(prefix.len() as u64, payload.clone())
                    .unwrap()
                    .delivered
                    .as_slice(),
                std::slice::from_ref(&payload)
            );
        };
        tokio::pin!(metrics, service);
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                () = &mut service => {}
                () = &mut metrics => panic!("Native cadence ended before actual H3 completion"),
            }
        })
        .await
        .expect("existing server fixture completion watchdog");
    }
    assert!(
        !receipt.is_current(),
        "subsequent carrier service consumes Ready"
    );
    let state = owner.lock();
    assert_eq!(state.sender.data_bytes(), 0);
    assert_eq!(
        state.send_stream.next_offset(),
        (prefix.len() + payload.len()) as u64
    );
    assert_eq!(state.send_stream.data_ack_frontier(), 0);
    assert_eq!(
        state.send_stream.reinjection_bytes(),
        prefix.len() + payload.len()
    );
    assert_eq!(fixture.commands_tx.pending_bytes(), 0);
    assert_eq!(fixture.commands_tx.writer_pending_bytes(), 0);
}
