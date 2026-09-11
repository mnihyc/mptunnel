use super::*;
use crate::mux::stream::ReliableRecvStream;
use crate::runtime::path::prepared::PreparedOriginalClaim;
use std::task::Poll;

/// Exercise the actual shared-source notice and ordinary H3 writer. This child
/// uses the existing server fixture; all native coordinates come from its real
/// observer and successful adapter write, without reconstructing framing sizes.
#[tokio::test(flavor = "current_thread")]
async fn server_quic_prepared_original_waits_for_settled_native_operation_crossing() {
    let stream_id = StreamId(409);
    let (mut fixture, _accepted_rx) =
        ServerUdpTerminalWriterFixture::open_with_native_authority(stream_id, None, true).await;
    fixture.drain_zero_credit_admission().await;
    let commitment = fixture
        .server_send
        .as_mut()
        .unwrap()
        .bind_product_commitment(stream_id)
        .unwrap();
    fixture
        .commands_rx
        .as_mut()
        .unwrap()
        .bind_native_commitment(commitment.clone())
        .unwrap();
    let payload = Bytes::from_static(b"fresh source after packetization");
    let prefix = Bytes::from_static(b"already received predecessor");
    let (owner, _stream) = fixture.publish_latency_source(payload.clone()).await;
    let binding = owner.binding();
    let key = binding.sender_path_targets(TrafficClass::Latency, payload.len())[0]
        .observation
        .key;
    let mut receiver = ReliableRecvStream::new(stream_id, fixture.context.mux_limits);

    // Seed an earlier accepted Product assignment whose alternate has already
    // delivered it. Its Original can still complete this native FIFO operation.
    let late_original = {
        let mut state = owner.lock();
        let frame = state.send_stream.send_data(prefix.clone()).unwrap();
        binding.record_original_flight(key, &frame);
        assert_eq!(
            receiver
                .receive_data(0, prefix.clone())
                .unwrap()
                .delivered
                .as_slice(),
            &[prefix.clone()]
        );
        let ack = receiver.ack_ranges();
        state.send_stream.apply_ack(&ack).unwrap();
        binding.release_normalized_acked_ranges(&ack);
        assert_eq!(state.send_stream.data_ack_frontier(), prefix.len() as u64);
        assert_eq!(state.send_stream.reinjection_bytes(), 0);
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
        .barrier(prefix.len() as u64)
        .unwrap()
        .expect("positive Product ACK settles the still-unpacketized real H3 operation");
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
        panic!("settled native predecessor must refuse the actual Original claim");
    };
    assert!(receipt.is_current(), "refusal does not consume Ready");
    commands.defer_prepared_work(work, wait);
    assert!(try_recv_reliable_path_priority_command(commands).is_none());
    {
        let state = owner.lock();
        assert_eq!(state.sender.data_bytes(), payload.len());
        assert_eq!(state.send_stream.next_offset(), prefix.len() as u64);
        assert_eq!(state.send_stream.data_ack_frontier(), prefix.len() as u64);
        assert_eq!(state.send_stream.reinjection_bytes(), 0);
        assert!(state.prepared.pending_error.is_none());
    }
    assert_eq!(
        binding.sender_path_targets(TrafficClass::Latency, payload.len())[0]
            .observation
            .original_data_in_flight_bytes,
        0
    );
    assert_eq!(fixture.commands_tx.pending_bytes(), 0);
    assert_eq!(fixture.commands_tx.writer_pending_bytes(), 0);
    assert!(futures::poll!(&mut crossing).is_pending());

    tokio::time::timeout(Duration::from_secs(5), crossing)
        .await
        .expect("actual native packetization crossing")
        .unwrap();
    assert!(
        commitment
            .capture()
            .unwrap()
            .barrier(prefix.len() as u64)
            .unwrap()
            .is_none()
    );
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
            assert!(
                receiver
                    .receive_data(0, prefix.clone())
                    .unwrap()
                    .delivered
                    .is_empty()
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
                &[payload.clone()]
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
    assert!(!receipt.is_current(), "the successful claim consumes Ready");
    let state = owner.lock();
    assert_eq!(state.sender.data_bytes(), 0);
    assert_eq!(
        state.send_stream.next_offset(),
        (prefix.len() + payload.len()) as u64
    );
    assert_eq!(state.send_stream.data_ack_frontier(), prefix.len() as u64);
    assert_eq!(state.send_stream.reinjection_bytes(), payload.len());
    assert_eq!(fixture.commands_tx.pending_bytes(), 0);
    assert_eq!(fixture.commands_tx.writer_pending_bytes(), 0);
}
