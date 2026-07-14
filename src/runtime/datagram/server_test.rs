use super::*;

#[tokio::test]
async fn datagram_response_queue_full_is_realtime_backpressure() {
    let flow_id = DatagramFlowId(12);
    let (commands, mut command_rx) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::DatagramData {
                flow_id,
                datagram_id: DatagramId(1),
                ttl_ms: 1000,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::RealtimeDatagram,
        )
        .expect("prefill realtime queue");

    let err = try_send_server_datagram_realtime_frame(
        &commands,
        Frame::DatagramData {
            flow_id,
            datagram_id: DatagramId(2),
            ttl_ms: 1000,
            payload: Bytes::from_static(b"later"),
        },
    )
    .expect_err("full realtime queue should be sender-service backpressure");

    assert!(matches!(err, RuntimeError::SenderServiceBlocked));
    assert!(matches!(
        recv_reliable_path_command(&mut command_rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::DatagramData {
            datagram_id,
            payload,
            ..
        })) if datagram_id == DatagramId(1) && payload == Bytes::from_static(b"queued")
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut command_rx)
        )
        .await
        .is_err(),
        "blocked datagram response must not enqueue another frame"
    );
}

#[tokio::test]
async fn datagram_close_queue_full_is_realtime_backpressure() {
    let flow_id = DatagramFlowId(13);
    let (commands, mut command_rx) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::DatagramData {
                flow_id,
                datagram_id: DatagramId(1),
                ttl_ms: 1000,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::RealtimeDatagram,
        )
        .expect("prefill realtime queue");

    let err = try_send_server_datagram_realtime_frame(&commands, Frame::DatagramClose { flow_id })
        .expect_err("full realtime queue should be sender-service backpressure");

    assert!(matches!(err, RuntimeError::SenderServiceBlocked));
    assert!(matches!(
        recv_reliable_path_command(&mut command_rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::DatagramData {
            datagram_id,
            payload,
            ..
        })) if datagram_id == DatagramId(1) && payload == Bytes::from_static(b"queued")
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut command_rx)
        )
        .await
        .is_err(),
        "blocked datagram close must not wait or enqueue behind a full realtime queue"
    );
}
