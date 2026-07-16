use super::*;
use crate::outbound::{DnsConfig, OutboundConfig};
use crate::runtime::path::commands::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_channels,
};
use crate::runtime::path::{
    ServerDatagramOpenRequest, ServerDatagramRequest, ServerDatagramSendOutcome,
};
use crate::runtime::stream::ServerReliableStreamRegistry;
use crate::runtime::telemetry::RuntimeTelemetry;
use bytes::Bytes;
use std::sync::Arc;
use tokio::net::UdpSocket;

#[tokio::test]
async fn server_datagram_port_owns_target_connection_and_worker() {
    let target = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP target");
    let target_addr = target.local_addr().expect("UDP target address");
    let target_task = tokio::spawn(async move {
        let mut payload = [0_u8; 64];
        let (len, peer) = target
            .recv_from(&mut payload)
            .await
            .expect("target request");
        assert_eq!(&payload[..len], b"request");
        target
            .send_to(b"response", peer)
            .await
            .expect("target response");
    });
    let streams = Arc::new(ServerReliableStreamRegistry::new(8)).path_port();
    let telemetry = RuntimeTelemetry::new(8);
    let datagrams = ServerDatagramService::path_port(
        OutboundConfig::Direct,
        DnsConfig::default(),
        Duration::from_secs(1),
        MuxLimits::default(),
        streams,
        telemetry.clone(),
    );
    let (commands, mut command_rx) = reliable_path_command_channels(8);
    let flow_id = DatagramFlowId(21);
    let datagram_id = DatagramId(22);
    let flow = datagrams
        .open(ServerDatagramOpenRequest {
            session_id: crate::protocol::SessionId(20),
            flow_id,
            target: crate::protocol::TargetAddr::Ip(target_addr),
            commands,
        })
        .await
        .expect("open target-side datagram flow");

    assert_eq!(
        flow.try_send(ServerDatagramRequest {
            datagram_id,
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"request"),
        }),
        ServerDatagramSendOutcome::Accepted,
    );
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(1),
            recv_reliable_path_command(&mut command_rx),
        )
        .await
        .expect("target response timeout"),
        Some(ReliablePathCommand::SendFrame(Frame::DatagramData {
            flow_id: response_flow_id,
            datagram_id: response_datagram_id,
            ttl_ms: 1_000,
            payload,
        })) if response_flow_id == flow_id
            && response_datagram_id == datagram_id
            && payload == Bytes::from_static(b"response")
    ));
    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.datagram.io.from_peer_bytes, 7);
    assert_eq!(snapshot.datagram.io.from_peer_packets, 1);
    assert_eq!(snapshot.datagram.io.to_peer_bytes, 8);
    assert_eq!(snapshot.datagram.io.to_peer_packets, 1);
    assert_eq!(snapshot.datagram.flows.opened, 1);
    assert_eq!(snapshot.datagram.flows.active, 1);
    target_task.await.expect("UDP target task");
}

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
            TrafficClass::RealtimeDatagram,
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
            TrafficClass::RealtimeDatagram,
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
