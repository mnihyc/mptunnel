use super::*;
use crate::config::ProductPolicyConfig;
use crate::outbound::OutboundConfig;
use crate::product::{
    EgressAction, InboundId, InitialDemand, RouteAction, RouteMatchSpec, RouteRuleSpec, RuleId,
};
use crate::runtime::outbound_registry::{RuntimeOutboundLeaf, RuntimeOutboundRegistry};
use crate::runtime::path::commands::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_channels,
    reliable_path_command_pending_bytes,
};
use crate::runtime::path::{
    ServerDatagramOpenFailure, ServerDatagramOpenRequest, ServerDatagramRequest,
    ServerDatagramSendOutcome,
};
use crate::runtime::stream::ServerReliableStreamRegistry;
use crate::runtime::telemetry::RuntimeTelemetry;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::UdpSocket;
use tokio::sync::oneshot;

struct TestServerDatagramPort {
    inner: ServerDatagramPort,
    reliable_streams: crate::runtime::path::ServerStreamPort,
    carriers: Mutex<HashMap<SessionId, crate::runtime::path::ServerCarrierPathRegistration>>,
}

impl TestServerDatagramPort {
    async fn open(
        &self,
        request: ServerDatagramOpenRequest,
    ) -> Result<AcceptedServerDatagramFlow, ServerDatagramOpenError> {
        {
            let mut carriers = self.carriers.lock().expect("test carrier registry");
            carriers.entry(request.session_id).or_insert_with(|| {
                self.reliable_streams
                    .register_carrier_path_with_observed_peer(
                        request.session_id,
                        crate::protocol::UnderlayProtocol::Udp,
                        crate::protocol::PathId(0),
                        crate::runtime::path::ServerLocalPathProperties::default(),
                        request.principal_permit.clone(),
                        crate::runtime::path::ServerCarrierPeer::fixed(
                            "203.0.113.7:51000"
                                .parse()
                                .expect("authenticated test carrier peer"),
                        ),
                        Some(Arc::from("test-quic")),
                    )
                    .expect("register authenticated test carrier")
            });
        }
        self.inner.open(request).await
    }
}

fn test_ingress(session_id: SessionId) -> crate::runtime::path::ServerMppIngress {
    crate::runtime::path::ServerMppIngress::for_test(
        session_id,
        "203.0.113.7:51000"
            .parse()
            .expect("authenticated test carrier peer"),
        crate::protocol::UnderlayProtocol::Udp,
        Some("test-quic"),
        crate::protocol::PathId(0),
        crate::model::path::CarrierPathInstanceId::from_raw(1),
    )
}

fn test_server_datagram_port(telemetry: RuntimeTelemetry) -> TestServerDatagramPort {
    test_server_datagram_port_with_retention(telemetry, Duration::from_secs(60))
}

fn test_server_datagram_port_with_retention(
    telemetry: RuntimeTelemetry,
    retention: Duration,
) -> TestServerDatagramPort {
    test_server_datagram_port_with_limits(telemetry, retention, MuxLimits::default())
}

fn test_server_datagram_port_with_limits(
    telemetry: RuntimeTelemetry,
    retention: Duration,
    mux_limits: MuxLimits,
) -> TestServerDatagramPort {
    let outbound = OutboundConfig::Direct;
    let id = crate::product::OutboundId::parse("test-direct").expect("outbound");
    let outbound_registry = RuntimeOutboundRegistry::compile(
        [RuntimeOutboundLeaf::Local {
            id: id.clone(),
            config: outbound,
            connect_timeout: Duration::from_secs(1),
            native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
        }],
        &[],
        crate::runtime::outbound_registry::test_dns_generation(),
    )
    .expect("registry");
    let router = ClientIngressRouter::new(
        &ProductPolicyConfig {
            generation: 1,
            routes: vec![RouteRuleSpec::new(
                RuleId::parse("default").expect("route ID"),
                RouteMatchSpec::default(),
                RouteAction::allow_restricted(
                    EgressAction::Outbound(id),
                    None,
                    InitialDemand::Automatic,
                ),
            )],
        },
        outbound_registry,
    )
    .expect("router");
    let reliable_streams = Arc::new(ServerReliableStreamRegistry::new(8)).path_port();
    let inner = ServerDatagramService::path_port(ServerDatagramServiceConfig {
        router,
        inbound: InboundId::parse("test-inbound").expect("inbound ID"),
        session_retention_timeout: retention,
        mux_limits,
        reliable_streams: reliable_streams.clone(),
        telemetry,
    });
    TestServerDatagramPort {
        inner,
        reliable_streams,
        carriers: Mutex::new(HashMap::new()),
    }
}

#[tokio::test]
async fn shared_session_registry_saturation_is_an_explicit_capacity_failure() {
    let telemetry = RuntimeTelemetry::new(8);
    let datagrams = test_server_datagram_port_with_limits(
        telemetry,
        Duration::from_secs(60),
        MuxLimits {
            max_streams: 1,
            ..MuxLimits::default()
        },
    );
    let session_id = SessionId(18);
    let (carrier_a_commands, _carrier_a_rx) = reliable_path_command_channels(8);
    let (carrier_b_commands, _carrier_b_rx) = reliable_path_command_channels(8);
    let first = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id,
            flow_id: DatagramFlowId(1),
            target: TargetAddr::Ip("127.0.0.1:9".parse().expect("first target")),
            commands: carrier_a_commands,
            ingress: test_ingress(session_id),
        })
        .await
        .expect("occupy the shared session registry from carrier A");

    let failure = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id,
            flow_id: DatagramFlowId(2),
            target: TargetAddr::Ip("127.0.0.1:10".parse().expect("second target")),
            commands: carrier_b_commands,
            ingress: test_ingress(session_id),
        })
        .await
        .expect_err("carrier B must observe the shared session limit");
    assert!(matches!(
        failure.into_failure(),
        ServerDatagramOpenFailure::Capacity
    ));
    drop(first);
}

async fn await_test_signal(signal: oneshot::Receiver<()>, context: &str) {
    tokio::time::timeout(Duration::from_secs(1), signal)
        .await
        .expect(context)
        .expect(context);
}

async fn await_datagram_to_peer_packets(
    telemetry: &RuntimeTelemetry,
    expected: u64,
    context: &str,
) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if telemetry.snapshot().datagram.io.to_peer_packets >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(context);
}

async fn await_active_datagram_flows(telemetry: &RuntimeTelemetry, expected: u64, context: &str) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if telemetry.snapshot().datagram.flows.active == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(context);
}

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
    let telemetry = RuntimeTelemetry::new(8);
    let datagrams = test_server_datagram_port(telemetry.clone());
    let (commands, mut command_rx) = reliable_path_command_channels(8);
    let flow_id = DatagramFlowId(21);
    let datagram_id = DatagramId(22);
    let flow = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id: crate::protocol::SessionId(20),
            flow_id,
            target: crate::protocol::TargetAddr::Ip(target_addr),
            commands,
            ingress: test_ingress(SessionId(20)),
        })
        .await
        .expect("open target-side datagram flow");

    assert_eq!(
        flow.send(ServerDatagramRequest {
            datagram_id,
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"request"),
        })
        .await
        .expect("admit target request"),
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
            ttl_ms,
            payload,
        })) if response_flow_id == flow_id
            && response_datagram_id == DatagramId(0)
            && ttl_ms > 0
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
async fn distinct_client_datagrams_are_admitted_without_waiting_for_target_data() {
    let target = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP target");
    let target_addr = target.local_addr().expect("UDP target address");
    let (first_seen, first_seen_rx) = oneshot::channel();
    let (second_seen, second_seen_rx) = oneshot::channel();
    let target_task = tokio::spawn(async move {
        let mut payload = [0_u8; 64];
        let (first_len, _) = target
            .recv_from(&mut payload)
            .await
            .expect("first target request");
        assert_eq!(&payload[..first_len], b"first-request");
        let _ = first_seen.send(());

        let (second_len, _) = target
            .recv_from(&mut payload)
            .await
            .expect("second target request");
        assert_eq!(&payload[..second_len], b"second-request");
        let _ = second_seen.send(());
    });

    let telemetry = RuntimeTelemetry::new(8);
    let datagrams = test_server_datagram_port(telemetry.clone());
    let (commands, _command_rx) = reliable_path_command_channels(8);
    let flow = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id: SessionId(25),
            flow_id: DatagramFlowId(26),
            target: TargetAddr::Ip(target_addr),
            commands,
            ingress: test_ingress(SessionId(25)),
        })
        .await
        .expect("open target-side datagram flow");

    assert_eq!(
        flow.send(ServerDatagramRequest {
            datagram_id: DatagramId(70),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"first-request"),
        })
        .await
        .expect("admit first request"),
        ServerDatagramSendOutcome::Accepted,
    );
    await_test_signal(first_seen_rx, "first request observation timeout").await;
    assert_eq!(
        flow.send(ServerDatagramRequest {
            datagram_id: DatagramId(71),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"second-request"),
        })
        .await
        .expect("admit second request"),
        ServerDatagramSendOutcome::Accepted,
    );
    await_test_signal(second_seen_rx, "second request observation timeout").await;
    target_task.await.expect("UDP target task");

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.datagram.io.from_peer_packets, 2);
    assert_eq!(snapshot.datagram.io.to_peer_packets, 0);
}

#[tokio::test]
async fn one_client_datagram_forwards_all_target_datagrams_with_direction_local_ids() {
    let target = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP target");
    let target_addr = target.local_addr().expect("UDP target address");
    let (request_seen, request_seen_rx) = oneshot::channel();
    let (release_responses, release_responses_rx) = oneshot::channel();
    let target_task = tokio::spawn(async move {
        let mut payload = [0_u8; 64];
        let (len, peer) = target
            .recv_from(&mut payload)
            .await
            .expect("target request");
        assert_eq!(&payload[..len], b"request-with-two-responses");
        let _ = request_seen.send(());
        release_responses_rx
            .await
            .expect("release target responses");
        target
            .send_to(b"first-response", peer)
            .await
            .expect("first target response");
        target
            .send_to(b"second-response", peer)
            .await
            .expect("second target response");
    });

    let telemetry = RuntimeTelemetry::new(8);
    let datagrams = test_server_datagram_port(telemetry.clone());
    let (commands, mut command_rx) = reliable_path_command_channels(8);
    let flow_id = DatagramFlowId(28);
    let flow = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id: SessionId(27),
            flow_id,
            target: TargetAddr::Ip(target_addr),
            commands,
            ingress: test_ingress(SessionId(27)),
        })
        .await
        .expect("open target-side datagram flow");

    assert_eq!(
        flow.send(ServerDatagramRequest {
            datagram_id: DatagramId(90),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"request-with-two-responses"),
        })
        .await
        .expect("admit target request"),
        ServerDatagramSendOutcome::Accepted,
    );
    await_test_signal(request_seen_rx, "target request observation timeout").await;
    release_responses
        .send(())
        .expect("release target responses");

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), recv_reliable_path_command(&mut command_rx))
            .await
            .expect("first target response timeout"),
        Some(ReliablePathCommand::SendFrame(Frame::DatagramData {
            flow_id: response_flow_id,
            datagram_id: DatagramId(0),
            payload,
            ..
        })) if response_flow_id == flow_id
            && payload == Bytes::from_static(b"first-response")
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), recv_reliable_path_command(&mut command_rx))
            .await
            .expect("second target response timeout"),
        Some(ReliablePathCommand::SendFrame(Frame::DatagramData {
            flow_id: response_flow_id,
            datagram_id: DatagramId(1),
            payload,
            ..
        })) if response_flow_id == flow_id
            && payload == Bytes::from_static(b"second-response")
    ));
    target_task.await.expect("UDP target task");

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.datagram.io.from_peer_packets, 1);
    assert_eq!(snapshot.datagram.io.to_peer_packets, 2);
}

#[tokio::test]
async fn delayed_target_datagram_after_later_request_is_not_mislabeled() {
    let target = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP target");
    let target_addr = target.local_addr().expect("UDP target address");
    let (first_seen, first_seen_rx) = oneshot::channel();
    let (second_seen, second_seen_rx) = oneshot::channel();
    let (release_response, release_response_rx) = oneshot::channel();
    let target_task = tokio::spawn(async move {
        let mut payload = [0_u8; 64];
        let (first_len, peer) = target
            .recv_from(&mut payload)
            .await
            .expect("first target request");
        assert_eq!(&payload[..first_len], b"first-request");
        let _ = first_seen.send(());

        let (second_len, second_peer) = target
            .recv_from(&mut payload)
            .await
            .expect("second target request");
        assert_eq!(second_peer, peer);
        assert_eq!(&payload[..second_len], b"second-request");
        let _ = second_seen.send(());

        release_response_rx
            .await
            .expect("release delayed target response");
        target
            .send_to(b"delayed-first-response", peer)
            .await
            .expect("delayed target response");
    });

    let telemetry = RuntimeTelemetry::new(8);
    let datagrams = test_server_datagram_port(telemetry);
    let (commands, mut command_rx) = reliable_path_command_channels(8);
    let flow_id = DatagramFlowId(30);
    let flow = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id: SessionId(29),
            flow_id,
            target: TargetAddr::Ip(target_addr),
            commands,
            ingress: test_ingress(SessionId(29)),
        })
        .await
        .expect("open target-side datagram flow");

    assert_eq!(
        flow.send(ServerDatagramRequest {
            datagram_id: DatagramId(100),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"first-request"),
        })
        .await
        .expect("admit first request"),
        ServerDatagramSendOutcome::Accepted,
    );
    await_test_signal(first_seen_rx, "first request observation timeout").await;
    assert_eq!(
        flow.send(ServerDatagramRequest {
            datagram_id: DatagramId(101),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"second-request"),
        })
        .await
        .expect("admit second request"),
        ServerDatagramSendOutcome::Accepted,
    );
    await_test_signal(second_seen_rx, "second request observation timeout").await;
    release_response
        .send(())
        .expect("release delayed target response");

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), recv_reliable_path_command(&mut command_rx))
            .await
            .expect("delayed target response timeout"),
        Some(ReliablePathCommand::SendFrame(Frame::DatagramData {
            flow_id: response_flow_id,
            datagram_id: DatagramId(0),
            payload,
            ..
        })) if response_flow_id == flow_id
            && payload == Bytes::from_static(b"delayed-first-response")
    ));
    target_task.await.expect("UDP target task");
}

#[tokio::test]
async fn same_client_datagram_across_attachments_is_forwarded_once() {
    let target = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP target");
    let target_addr = target.local_addr().expect("UDP target address");
    let (request_seen, request_seen_rx) = oneshot::channel();
    let (release_response, release_response_rx) = oneshot::channel();
    let target_task = tokio::spawn(async move {
        let mut payload = [0_u8; 64];
        let (len, peer) = target
            .recv_from(&mut payload)
            .await
            .expect("target request");
        assert_eq!(&payload[..len], b"same-request");
        let _ = request_seen.send(());
        release_response_rx.await.expect("release target response");
        target
            .send_to(b"same-response", peer)
            .await
            .expect("target response");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), target.recv_from(&mut payload))
                .await
                .is_err(),
            "cross-carrier retry must not execute the target twice"
        );
    });

    let telemetry = RuntimeTelemetry::new(8);
    let datagrams = test_server_datagram_port(telemetry.clone());
    let (commands_a, mut command_rx_a) = reliable_path_command_channels(8);
    let (commands_b, mut command_rx_b) = reliable_path_command_channels(8);
    let session_id = SessionId(30);
    let flow_id = DatagramFlowId(31);
    let datagram_id = DatagramId(32);
    let target = TargetAddr::Ip(target_addr);
    let flow_a = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id,
            flow_id,
            target: target.clone(),
            commands: commands_a,
            ingress: test_ingress(session_id),
        })
        .await
        .expect("open first carrier attachment");
    let flow_b = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id,
            flow_id,
            target,
            commands: commands_b,
            ingress: test_ingress(session_id),
        })
        .await
        .expect("open retry carrier attachment");

    assert_eq!(
        flow_a
            .send(ServerDatagramRequest {
                datagram_id,
                ttl_ms: 1_000,
                payload: Bytes::from_static(b"same-request"),
            })
            .await
            .expect("admit first carrier request"),
        ServerDatagramSendOutcome::Accepted,
    );
    await_test_signal(request_seen_rx, "target request observation timeout").await;
    assert_eq!(
        flow_b
            .send(ServerDatagramRequest {
                datagram_id,
                ttl_ms: 900,
                payload: Bytes::from_static(b"same-request"),
            })
            .await
            .expect("admit retry carrier request"),
        ServerDatagramSendOutcome::Accepted,
    );
    release_response.send(()).expect("release target response");

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), recv_reliable_path_command(&mut command_rx_b))
            .await
            .expect("retry response timeout"),
        Some(ReliablePathCommand::SendFrame(Frame::DatagramData {
            flow_id: response_flow_id,
            datagram_id: response_datagram_id,
            payload,
            ..
        })) if response_flow_id == flow_id
            && response_datagram_id == DatagramId(0)
            && payload == Bytes::from_static(b"same-response")
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut command_rx_a),
        )
        .await
        .is_err(),
        "the response should use the most recently admitted carrier"
    );
    target_task.await.expect("target task");
    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.datagram.io.from_peer_packets, 1);
    assert_eq!(snapshot.datagram.io.to_peer_packets, 1);
    assert_eq!(
        snapshot.datagram.flows.opened, 1,
        "carrier reattachment must reuse one logical Product UDP flow",
    );
}

#[tokio::test]
async fn cached_server_datagram_replay_preserves_direction_local_id() {
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
        assert_eq!(&payload[..len], b"cached-request");
        target
            .send_to(b"cached-response", peer)
            .await
            .expect("target response");
    });

    let telemetry = RuntimeTelemetry::new(8);
    let datagrams = test_server_datagram_port(telemetry.clone());
    let (commands_a, mut command_rx_a) = reliable_path_command_channels(8);
    let (commands_b, mut command_rx_b) = reliable_path_command_channels(8);
    let session_id = SessionId(40);
    let flow_id = DatagramFlowId(41);
    let datagram_id = DatagramId(42);
    let target = TargetAddr::Ip(target_addr);
    let flow_a = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id,
            flow_id,
            target: target.clone(),
            commands: commands_a,
            ingress: test_ingress(session_id),
        })
        .await
        .expect("open first carrier attachment");

    assert_eq!(
        flow_a
            .send(ServerDatagramRequest {
                datagram_id,
                ttl_ms: 1_000,
                payload: Bytes::from_static(b"cached-request"),
            })
            .await
            .expect("admit first request"),
        ServerDatagramSendOutcome::Accepted,
    );
    let first = tokio::time::timeout(
        Duration::from_secs(1),
        recv_reliable_path_command(&mut command_rx_a),
    )
    .await
    .expect("first response timeout");
    let first_response_id = match first {
        Some(ReliablePathCommand::SendFrame(Frame::DatagramData {
            datagram_id,
            payload,
            ..
        })) if payload == Bytes::from_static(b"cached-response") => datagram_id,
        _ => panic!("unexpected first target response"),
    };
    assert_eq!(first_response_id, DatagramId(0));

    let _flow_b = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id,
            flow_id,
            target,
            commands: commands_b,
            ingress: test_ingress(session_id),
        })
        .await
        .expect("open retry carrier attachment");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), recv_reliable_path_command(&mut command_rx_b))
            .await
            .expect("cached response timeout"),
        Some(ReliablePathCommand::SendFrame(Frame::DatagramData {
            flow_id: response_flow_id,
            datagram_id: response_datagram_id,
            payload,
            ..
        })) if response_flow_id == flow_id
            && response_datagram_id == first_response_id
            && payload == Bytes::from_static(b"cached-response")
    ));
    target_task.await.expect("target task");
    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.datagram.io.from_peer_packets, 1);
    assert_eq!(snapshot.datagram.io.to_peer_packets, 1);
}

#[tokio::test]
async fn same_client_datagram_id_with_different_payload_is_rejected() {
    let target = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP target");
    let target_addr = target.local_addr().expect("UDP target address");
    let (request_seen, request_seen_rx) = oneshot::channel();
    let (check_duplicate, check_duplicate_rx) = oneshot::channel();
    let target_task = tokio::spawn(async move {
        let mut payload = [0_u8; 64];
        let (len, _) = target
            .recv_from(&mut payload)
            .await
            .expect("target request");
        assert_eq!(&payload[..len], b"original-payload");
        let _ = request_seen.send(());
        check_duplicate_rx
            .await
            .expect("check for duplicate target request");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), target.recv_from(&mut payload))
                .await
                .is_err(),
            "payload-mismatched retry must not execute the target"
        );
    });

    let telemetry = RuntimeTelemetry::new(8);
    let datagrams = test_server_datagram_port(telemetry.clone());
    let (commands_a, _command_rx_a) = reliable_path_command_channels(8);
    let (commands_b, _command_rx_b) = reliable_path_command_channels(8);
    let session_id = SessionId(45);
    let flow_id = DatagramFlowId(46);
    let datagram_id = DatagramId(47);
    let target = TargetAddr::Ip(target_addr);
    let flow_a = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id,
            flow_id,
            target: target.clone(),
            commands: commands_a,
            ingress: test_ingress(session_id),
        })
        .await
        .expect("open first carrier attachment");
    let flow_b = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id,
            flow_id,
            target,
            commands: commands_b,
            ingress: test_ingress(session_id),
        })
        .await
        .expect("open second carrier attachment");

    assert_eq!(
        flow_a
            .send(ServerDatagramRequest {
                datagram_id,
                ttl_ms: 1_000,
                payload: Bytes::from_static(b"original-payload"),
            })
            .await
            .expect("admit original request"),
        ServerDatagramSendOutcome::Accepted,
    );
    await_test_signal(request_seen_rx, "target request observation timeout").await;
    let error = flow_b
        .send(ServerDatagramRequest {
            datagram_id,
            ttl_ms: 900,
            payload: Bytes::from_static(b"different-payload"),
        })
        .await
        .expect_err("payload-mismatched datagram ID reuse must fail");
    assert!(matches!(
        error,
        RuntimeError::Protocol("datagram ID reused with a different payload")
    ));
    check_duplicate
        .send(())
        .expect("check for duplicate target request");
    target_task.await.expect("UDP target task");

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.datagram.io.from_peer_packets, 1);
}

#[tokio::test]
async fn cached_target_datagram_is_delivered_when_live_route_capacity_returns() {
    let target = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP target");
    let target_addr = target.local_addr().expect("UDP target address");
    let (request_seen, request_seen_rx) = oneshot::channel();
    let (release_response, release_response_rx) = oneshot::channel();
    let target_task = tokio::spawn(async move {
        let mut payload = [0_u8; 64];
        let (len, peer) = target
            .recv_from(&mut payload)
            .await
            .expect("target request");
        assert_eq!(&payload[..len], b"capacity-request");
        let _ = request_seen.send(());
        release_response_rx.await.expect("release target response");
        target
            .send_to(b"cached-until-capacity", peer)
            .await
            .expect("target response");
    });

    let telemetry = RuntimeTelemetry::new(8);
    let datagrams = test_server_datagram_port(telemetry.clone());
    let (commands, mut command_rx) = reliable_path_command_channels(1);
    let flow_id = DatagramFlowId(48);
    let flow = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id: SessionId(47),
            flow_id,
            target: TargetAddr::Ip(target_addr),
            commands: commands.clone(),
            ingress: test_ingress(SessionId(47)),
        })
        .await
        .expect("open target-side datagram flow");
    assert_eq!(
        flow.send(ServerDatagramRequest {
            datagram_id: DatagramId(49),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"capacity-request"),
        })
        .await
        .expect("admit target request"),
        ServerDatagramSendOutcome::Accepted,
    );
    await_test_signal(request_seen_rx, "target request observation timeout").await;

    commands
        .try_enqueue_admitted_frame(
            Frame::DatagramData {
                flow_id,
                datagram_id: DatagramId(900),
                ttl_ms: 1_000,
                payload: Bytes::from_static(b"queue-filler"),
            },
            TrafficClass::RealtimeDatagram,
        )
        .expect("fill live response route");
    release_response.send(()).expect("release target response");
    await_datagram_to_peer_packets(&telemetry, 1, "target response was not cached").await;
    tokio::task::yield_now().await;

    let filler = recv_reliable_path_command(&mut command_rx)
        .await
        .expect("queued route filler");
    assert!(matches!(
        &filler,
        ReliablePathCommand::SendFrame(Frame::DatagramData {
            datagram_id: DatagramId(900),
            payload,
            ..
        }) if payload == &Bytes::from_static(b"queue-filler")
    ));
    command_rx.release_pending_command_bytes(reliable_path_command_pending_bytes(&filler));

    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(1),
            recv_reliable_path_command(&mut command_rx),
        )
        .await
        .expect("cached target response capacity wakeup"),
        Some(ReliablePathCommand::SendFrame(Frame::DatagramData {
            flow_id: response_flow_id,
            datagram_id: DatagramId(0),
            payload,
            ..
        })) if response_flow_id == flow_id
            && payload == Bytes::from_static(b"cached-until-capacity")
    ));
    target_task.await.expect("UDP target task");
}

#[tokio::test]
async fn attachment_drop_starts_full_retention_and_target_traffic_does_not_extend_it() {
    let retention = Duration::from_millis(200);
    let target = Arc::new(
        UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind UDP target"),
    );
    let target_addr = target.local_addr().expect("UDP target address");
    let target_receiver = target.clone();
    let (peer_tx, peer_rx) = oneshot::channel();
    let target_task = tokio::spawn(async move {
        let mut payload = [0_u8; 64];
        let (len, peer) = target_receiver
            .recv_from(&mut payload)
            .await
            .expect("initial target request");
        assert_eq!(&payload[..len], b"initial-request");
        let _ = peer_tx.send(peer);
    });

    let telemetry = RuntimeTelemetry::new(8);
    let datagrams = test_server_datagram_port_with_retention(telemetry.clone(), retention);
    let (commands, _command_rx) = reliable_path_command_channels(8);
    let session_id = SessionId(50);
    let flow_id = DatagramFlowId(51);
    let target_address = TargetAddr::Ip(target_addr);
    let flow = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id,
            flow_id,
            target: target_address.clone(),
            commands,
            ingress: test_ingress(session_id),
        })
        .await
        .expect("open target-side datagram flow");
    assert_eq!(
        flow.send(ServerDatagramRequest {
            datagram_id: DatagramId(52),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"initial-request"),
        })
        .await
        .expect("admit initial target request"),
        ServerDatagramSendOutcome::Accepted,
    );
    let peer = tokio::time::timeout(Duration::from_secs(1), peer_rx)
        .await
        .expect("initial target request timeout")
        .expect("initial target peer");
    target_task.await.expect("UDP target task");

    tokio::time::sleep(retention + Duration::from_millis(20)).await;
    assert_eq!(
        flow.send(ServerDatagramRequest {
            datagram_id: DatagramId(53),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"post-timer-request"),
        })
        .await
        .expect("admit request after original retention timer"),
        ServerDatagramSendOutcome::Accepted,
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    let dropped_at = tokio::time::Instant::now();
    drop(flow);

    tokio::time::sleep_until(dropped_at + Duration::from_millis(30)).await;
    target
        .send_to(b"detached-target-traffic-one", peer)
        .await
        .expect("first detached target datagram");
    await_datagram_to_peer_packets(&telemetry, 1, "first detached target datagram timeout").await;

    tokio::time::sleep_until(dropped_at + Duration::from_millis(100)).await;
    target
        .send_to(b"detached-target-traffic-two", peer)
        .await
        .expect("second detached target datagram");
    await_datagram_to_peer_packets(&telemetry, 2, "second detached target datagram timeout").await;

    tokio::time::sleep_until(dropped_at + Duration::from_millis(140)).await;
    let retained = telemetry.snapshot();
    assert_eq!(retained.datagram.flows.opened, 1);
    assert_eq!(retained.datagram.flows.active, 1);

    tokio::time::sleep_until(dropped_at + Duration::from_millis(260)).await;
    await_active_datagram_flows(&telemetry, 0, "detached datagram flow did not expire").await;
    let expired = telemetry.snapshot();
    assert_eq!(expired.datagram.flows.opened, 1);
    assert_eq!(expired.datagram.flows.completed, 1);

    let (replacement_commands, _replacement_command_rx) = reliable_path_command_channels(8);
    let _replacement = datagrams
        .open(ServerDatagramOpenRequest {
            principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
            session_id,
            flow_id,
            target: target_address,
            commands: replacement_commands,
            ingress: test_ingress(session_id),
        })
        .await
        .expect("open replacement after retained flow expiry");
    let replacement = telemetry.snapshot();
    assert_eq!(replacement.datagram.flows.opened, 2);
    assert_eq!(replacement.datagram.flows.active, 1);
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
async fn datagram_response_waits_for_queue_capacity_before_its_deadline() {
    let flow_id = DatagramFlowId(14);
    let (commands, mut command_rx) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::DatagramData {
                flow_id,
                datagram_id: DatagramId(1),
                ttl_ms: 1_000,
                payload: Bytes::from_static(b"queued"),
            },
            TrafficClass::RealtimeDatagram,
        )
        .expect("prefill realtime queue");

    let waiting_commands = commands.clone();
    let waiting = tokio::spawn(async move {
        send_server_datagram_realtime_frame_until(
            &waiting_commands,
            Frame::DatagramData {
                flow_id,
                datagram_id: DatagramId(2),
                ttl_ms: 1_000,
                payload: Bytes::from_static(b"later"),
            },
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
    });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());

    let queued = recv_reliable_path_command(&mut command_rx)
        .await
        .expect("first queued response");
    command_rx.release_pending_command_bytes(reliable_path_command_pending_bytes(&queued));
    tokio::time::timeout(Duration::from_millis(200), waiting)
        .await
        .expect("queue capacity wakeup")
        .expect("response admission task")
        .expect("response admitted before deadline");
    assert!(matches!(
        recv_reliable_path_command(&mut command_rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::DatagramData {
            datagram_id: DatagramId(2),
            payload,
            ..
        })) if payload == Bytes::from_static(b"later")
    ));
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
