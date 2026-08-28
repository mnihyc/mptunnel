use super::service::server_packet_delivery_rate;
use super::*;
use crate::config::{MppPerformanceConfig, ResourceLimits, ServerSecurityConfig, SharedSecret};
use crate::outbound::OutboundConfig;
use crate::product::{PrincipalId, TunL3AddressPlan, TunL3AllocationSpec, TunL3ServerSpec};
use crate::protocol::{CloseReason, IpPacketId, IpTunnelId, PathId, SessionId, UnderlayProtocol};
use crate::runtime::node::server::{ServerIdentityRuntime, new_identity_runtime};
use crate::runtime::path::{ServerLocalPath, ServerLocalPathProperties};
use bytes::Bytes;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
enum TestCarrierEvent {
    Packet {
        tunnel_id: IpTunnelId,
        packet_id: IpPacketId,
        payload: Bytes,
    },
    Close {
        tunnel_id: IpTunnelId,
        reason: crate::protocol::CloseReason,
    },
}

#[derive(Debug)]
struct TestCarrier {
    events: tokio::sync::mpsc::UnboundedSender<TestCarrierEvent>,
}

impl ServerIpTunnelCarrier for TestCarrier {
    fn try_send_packet(
        &self,
        tunnel_id: IpTunnelId,
        packet_id: IpPacketId,
        payload: Bytes,
        _budget: &IpPacketQueueBudget,
    ) -> Result<IpTunnelPacketSendOutcome, crate::runtime::error::RuntimeError> {
        self.events
            .send(TestCarrierEvent::Packet {
                tunnel_id,
                packet_id,
                payload,
            })
            .map(|()| IpTunnelPacketSendOutcome::Accepted)
            .map_err(|_| crate::runtime::error::RuntimeError::ReliablePathRetired)
    }

    fn close(&self, tunnel_id: IpTunnelId, reason: crate::protocol::CloseReason) {
        let _ = self
            .events
            .send(TestCarrierEvent::Close { tunnel_id, reason });
    }
}

#[derive(Debug)]
struct RetiredTestCarrier;

impl ServerIpTunnelCarrier for RetiredTestCarrier {
    fn try_send_packet(
        &self,
        _tunnel_id: IpTunnelId,
        _packet_id: IpPacketId,
        _payload: Bytes,
        _budget: &IpPacketQueueBudget,
    ) -> Result<IpTunnelPacketSendOutcome, crate::runtime::error::RuntimeError> {
        Ok(IpTunnelPacketSendOutcome::Retired)
    }

    fn close(&self, _tunnel_id: IpTunnelId, _reason: CloseReason) {}
}

fn server_context() -> (
    crate::runtime::path::ServerPathContext,
    ServerSecurityConfig,
) {
    let security = ServerSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let ServerIdentityRuntime {
        paths,
        reliable_relay: _,
    } = new_identity_runtime(
        Vec::new(),
        OutboundConfig::Direct,
        crate::config::DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        security.clone(),
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
    );
    (paths, security)
}

fn session_reference_count(
    context: &crate::runtime::path::ServerPathContext,
    session_id: SessionId,
) -> u32 {
    context
        .reliable_streams
        .management_snapshot()
        .sessions
        .iter()
        .find(|session| session.session_id == session_id)
        .map_or(0, |session| session.reference_count)
}

fn plan(security: &ServerSecurityConfig) -> TunL3AddressPlan {
    TunL3AddressPlan::compile(
        TunL3ServerSpec {
            interface_name: Some("test-tun".to_string()),
            ipv4_pool: Some("10.88.0.0/24".parse().expect("pool")),
            ipv4: Some(Ipv4Addr::new(10, 88, 0, 1)),
            ipv6_pool: None,
            ipv6: None,
            mtu: 1_500,
            allocations: vec![TunL3AllocationSpec {
                principal_id: PrincipalId::parse("test-peer").expect("principal"),
                ipv4: Some(Ipv4Addr::new(10, 88, 0, 2)),
                ipv6: None,
                allowed_ips: Vec::new(),
            }],
        },
        &security.credential_authority,
    )
    .expect("address plan")
}

fn ipv4_packet(source: [u8; 4], destination: [u8; 4]) -> Bytes {
    let mut packet = vec![
        0x45,
        0,
        0,
        24,
        0,
        1,
        0,
        0,
        64,
        17,
        0,
        0,
        source[0],
        source[1],
        source[2],
        source[3],
        destination[0],
        destination[1],
        destination[2],
        destination[3],
        0x12,
        0x34,
        0,
        53,
    ];
    let mut sum = 0_u32;
    for chunk in packet[..20].chunks_exact(2) {
        sum += u32::from(u16::from_be_bytes([chunk[0], chunk[1]]));
    }
    while sum > u32::from(u16::MAX) {
        sum = (sum & u32::from(u16::MAX)) + (sum >> 16);
    }
    packet[10..12].copy_from_slice(&(!(sum as u16)).to_be_bytes());
    Bytes::from(packet)
}

#[tokio::test]
async fn packet_service_enforces_ownership_and_preserves_packets() {
    let (context, security) = server_context();
    let (port, mut device) = ServerIpTunnelService::build(
        plan(&security),
        context.reliable_streams.clone(),
        4,
        16 * 1_500,
        context.session_retention_timeout,
    );
    let local_path: crate::transport::PathSpec =
        "tcp://127.0.0.1:9000?initial-srtt-ms=20&initial-rate-mbps=500"
            .parse()
            .expect("path");
    let local = ServerLocalPath::new(0, local_path);
    let registration = context.reliable_streams.register_test_carrier_path(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties {
            config_ordinal: 0,
            policy: local.policy(),
            initial_metrics: Some(local.startup_metrics(PathId(0))),
        },
    );
    let (carrier_events, mut carrier_commands) = tokio::sync::mpsc::unbounded_channel();
    let attachment = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(7),
            path: &registration,
            carrier: Arc::new(TestCarrier {
                events: carrier_events,
            }),
        })
        .expect("open IP tunnel");
    assert_eq!(
        attachment.allocation().ipv4(),
        Some(Ipv4Addr::new(10, 88, 0, 2))
    );

    let request = ipv4_packet([10, 88, 0, 2], [10, 88, 0, 1]);
    assert!(
        attachment
            .receive(IpPacketId(1), request.clone())
            .expect("admit packet")
    );
    assert_eq!(device.receive_from_peer().await, Some(request.clone()));
    assert!(
        !attachment
            .receive(IpPacketId(2), ipv4_packet([10, 88, 0, 99], [10, 88, 0, 1]),)
            .expect("reject spoofed packet")
    );

    let reply = ipv4_packet([10, 88, 0, 1], [10, 88, 0, 2]);
    assert!(
        device
            .try_send_to_peer(reply.clone())
            .expect("dispatch reply")
    );
    match carrier_commands.recv().await.expect("carrier packet") {
        TestCarrierEvent::Packet {
            tunnel_id,
            packet_id,
            payload,
        } => {
            assert_eq!(tunnel_id, IpTunnelId(7));
            assert_eq!(packet_id, IpPacketId(1));
            assert_eq!(payload, reply);
        }
        TestCarrierEvent::Close { .. } => panic!("unexpected close"),
    }

    let (replacement_events, mut replacement_carrier_commands) =
        tokio::sync::mpsc::unbounded_channel();
    let replacement = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(7),
            path: &registration,
            carrier: Arc::new(TestCarrier {
                events: replacement_events,
            }),
        })
        .expect("replace exact carrier attachment");
    assert!(matches!(
        carrier_commands.recv().await,
        Some(TestCarrierEvent::Close {
            tunnel_id: IpTunnelId(7),
            reason: crate::protocol::CloseReason::Normal,
        })
    ));
    assert!(
        !attachment
            .receive(IpPacketId(3), request)
            .expect("superseded attachment is stale")
    );
    drop(attachment);
    assert!(
        device
            .try_send_to_peer(reply.clone())
            .expect("replacement remains current")
    );
    assert!(matches!(
        replacement_carrier_commands.recv().await,
        Some(TestCarrierEvent::Packet {
            packet_id: IpPacketId(2),
            payload,
            ..
        }) if payload == reply
    ));

    drop(replacement);
    assert!(
        !device
            .try_send_to_peer(ipv4_packet([10, 88, 0, 1], [10, 88, 0, 2]))
            .expect("detached route")
    );
}

#[tokio::test(start_paused = true)]
async fn last_attachment_drop_releases_tun_session_owner_at_retention_deadline() {
    let (context, security) = server_context();
    let retention = Duration::from_millis(100);
    let (port, _device) = ServerIpTunnelService::build(
        plan(&security),
        context.reliable_streams.clone(),
        4,
        16 * 1_500,
        retention,
    );
    let session_id = SessionId(71);
    let path = context.reliable_streams.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let attachment = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(71),
            path: &path,
            carrier: Arc::new(TestCarrier {
                events: tokio::sync::mpsc::unbounded_channel().0,
            }),
        })
        .expect("open retained logical tunnel");

    drop(attachment);
    drop(path);
    assert_eq!(session_reference_count(&context, session_id), 1);
    tokio::time::advance(retention - Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        session_reference_count(&context, session_id),
        1,
        "logical TUN owner expired before its absolute retention deadline",
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        session_reference_count(&context, session_id),
        0,
        "the exact carrierless logical TUN owner must release at its deadline",
    );
}

#[tokio::test(start_paused = true)]
async fn retention_starts_only_on_true_last_attachment_transition() {
    let (context, security) = server_context();
    let retention = Duration::from_millis(100);
    let (port, _device) = ServerIpTunnelService::build(
        plan(&security),
        context.reliable_streams.clone(),
        4,
        16 * 1_500,
        retention,
    );
    let session_id = SessionId(72);
    let first_path = context.reliable_streams.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let second_path = context.reliable_streams.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(1),
        ServerLocalPathProperties::default(),
    );
    let first = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(72),
            path: &first_path,
            carrier: Arc::new(TestCarrier {
                events: tokio::sync::mpsc::unbounded_channel().0,
            }),
        })
        .expect("open first attachment");
    let second = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(72),
            path: &second_path,
            carrier: Arc::new(TestCarrier {
                events: tokio::sync::mpsc::unbounded_channel().0,
            }),
        })
        .expect("open second attachment");

    drop(first);
    drop(first_path);
    tokio::time::advance(retention / 2).await;
    drop(second);
    drop(second_path);
    assert_eq!(session_reference_count(&context, session_id), 1);

    // Reaching one full timeout since 2 -> 1 must not consume half of the
    // later true 1 -> 0 retention epoch.
    tokio::time::advance(retention / 2).await;
    tokio::task::yield_now().await;
    assert_eq!(
        session_reference_count(&context, session_id),
        1,
        "a non-last attachment removal incorrectly started the absolute timer",
    );
    tokio::time::advance(retention / 2).await;
    tokio::task::yield_now().await;
    assert_eq!(session_reference_count(&context, session_id), 0);
}

#[tokio::test(start_paused = true)]
async fn successful_reattach_invalidates_old_epoch_and_later_detach_gets_a_new_epoch() {
    let (context, security) = server_context();
    let retention = Duration::from_millis(100);
    let (port, _device) = ServerIpTunnelService::build(
        plan(&security),
        context.reliable_streams.clone(),
        4,
        16 * 1_500,
        retention,
    );
    let session_id = SessionId(73);
    let tunnel_id = IpTunnelId(73);
    let first_path = context.reliable_streams.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let first = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id,
            path: &first_path,
            carrier: Arc::new(TestCarrier {
                events: tokio::sync::mpsc::unbounded_channel().0,
            }),
        })
        .expect("open first attachment");
    drop(first);
    drop(first_path);

    tokio::time::advance(retention / 2).await;
    let second_path = context.reliable_streams.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(1),
        ServerLocalPathProperties::default(),
    );
    let second = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id,
            path: &second_path,
            carrier: Arc::new(TestCarrier {
                events: tokio::sync::mpsc::unbounded_channel().0,
            }),
        })
        .expect("reattach before old deadline");

    tokio::time::advance(retention / 2).await;
    tokio::task::yield_now().await;
    assert_eq!(
        session_reference_count(&context, session_id),
        2,
        "stale first epoch removed a successfully reattached tunnel",
    );
    drop(second);
    drop(second_path);
    tokio::time::advance(retention - Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(session_reference_count(&context, session_id), 1);
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(session_reference_count(&context, session_id), 0);
}

#[tokio::test(start_paused = true)]
async fn stale_expiry_cannot_remove_same_principal_successor_incarnation() {
    let (context, security) = server_context();
    let retention = Duration::from_millis(100);
    let (port, _device) = ServerIpTunnelService::build(
        plan(&security),
        context.reliable_streams.clone(),
        4,
        16 * 1_500,
        retention,
    );
    let predecessor_session = SessionId(76);
    let predecessor_path = context.reliable_streams.register_test_carrier_path(
        predecessor_session,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let predecessor = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(76),
            path: &predecessor_path,
            carrier: Arc::new(TestCarrier {
                events: tokio::sync::mpsc::unbounded_channel().0,
            }),
        })
        .expect("open predecessor incarnation");
    drop(predecessor);
    drop(predecessor_path);

    tokio::time::advance(retention / 2).await;
    let successor_session = SessionId(77);
    let successor_path = context.reliable_streams.register_test_carrier_path(
        successor_session,
        UnderlayProtocol::Tcp,
        PathId(1),
        ServerLocalPathProperties::default(),
    );
    let successor = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(77),
            path: &successor_path,
            carrier: Arc::new(TestCarrier {
                events: tokio::sync::mpsc::unbounded_channel().0,
            }),
        })
        .expect("same principal takes over with a new session/tunnel generation");

    tokio::time::advance(retention / 2).await;
    tokio::task::yield_now().await;
    assert_eq!(
        session_reference_count(&context, successor_session),
        2,
        "predecessor timer removed a different session/tunnel/generation owner",
    );
    drop(successor);
    drop(successor_path);
    tokio::time::advance(retention).await;
    tokio::task::yield_now().await;
    assert_eq!(session_reference_count(&context, successor_session), 0);
}

#[tokio::test(start_paused = true)]
async fn retired_send_removal_arms_once_and_stale_handle_drop_does_not_extend_it() {
    let (context, security) = server_context();
    let retention = Duration::from_millis(100);
    let (port, device) = ServerIpTunnelService::build(
        plan(&security),
        context.reliable_streams.clone(),
        4,
        16 * 1_500,
        retention,
    );
    let session_id = SessionId(74);
    let path = context.reliable_streams.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let attachment = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(74),
            path: &path,
            carrier: Arc::new(RetiredTestCarrier),
        })
        .expect("open carrier that reports exact retirement");
    assert!(
        !device
            .try_send_to_peer(ipv4_packet([10, 88, 0, 1], [10, 88, 0, 2]))
            .expect("retired carrier is attachment-local")
    );
    drop(path);

    tokio::time::advance(retention / 2).await;
    drop(attachment);
    tokio::time::advance(retention / 2).await;
    tokio::task::yield_now().await;
    assert_eq!(
        session_reference_count(&context, session_id),
        0,
        "the stale accepted-handle Drop restarted the retired-send deadline",
    );
}

#[tokio::test(start_paused = true)]
async fn failed_reopen_does_not_extend_the_existing_carrierless_epoch() {
    let (context, security) = server_context();
    let retention = Duration::from_millis(100);
    let (port, _device) = ServerIpTunnelService::build(
        plan(&security),
        context.reliable_streams.clone(),
        4,
        16 * 1_500,
        retention,
    );
    let session_id = SessionId(75);
    let path = context.reliable_streams.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let attachment = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(75),
            path: &path,
            carrier: Arc::new(TestCarrier {
                events: tokio::sync::mpsc::unbounded_channel().0,
            }),
        })
        .expect("open initial tunnel attachment");
    drop(attachment);

    tokio::time::advance(retention / 2).await;
    context
        .reliable_streams
        .retire_session(session_id, CloseReason::PolicyRejected);
    assert!(matches!(
        port.open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(75),
            path: &path,
            carrier: Arc::new(TestCarrier {
                events: tokio::sync::mpsc::unbounded_channel().0,
            }),
        }),
        Err(crate::runtime::RuntimeError::RemoteClosed(
            CloseReason::PolicyRejected
        ))
    ));
    drop(path);
    assert_eq!(session_reference_count(&context, session_id), 1);

    tokio::time::advance(retention / 2).await;
    tokio::task::yield_now().await;
    assert_eq!(
        session_reference_count(&context, session_id),
        0,
        "a failed reopen restarted or replaced the established no-attachment epoch",
    );
}

#[tokio::test]
async fn carrierless_same_identity_reconnect_keeps_current_session_fence() {
    let (context, security) = server_context();
    let (port, device) = ServerIpTunnelService::build(
        plan(&security),
        context.reliable_streams.clone(),
        4,
        16 * 1_500,
        context.session_retention_timeout,
    );
    let session_id = SessionId(31);
    let tunnel_id = IpTunnelId(31);
    let first_path = context.reliable_streams.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let first = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id,
            path: &first_path,
            carrier: Arc::new(TestCarrier {
                events: tokio::sync::mpsc::unbounded_channel().0,
            }),
        })
        .expect("open first IP tunnel attachment");

    // Carrier loss removes only the attachment. RFC 9.3 keeps the logical
    // tunnel available, so that retained owner must also keep the session's
    // principal and retirement channel alive.
    drop(first);
    drop(first_path);
    assert!(
        context
            .reliable_streams
            .management_snapshot()
            .sessions
            .iter()
            .any(|session| session.session_id == session_id && session.reference_count == 1)
    );

    let replacement_path = context.reliable_streams.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(1),
        ServerLocalPathProperties::default(),
    );
    let (replacement_events, mut replacement_commands) = tokio::sync::mpsc::unbounded_channel();
    let replacement = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id,
            path: &replacement_path,
            carrier: Arc::new(TestCarrier {
                events: replacement_events,
            }),
        })
        .expect("reattach retained logical IP tunnel");

    // Split publication from the owner sweep to prove both packet directions
    // consult the current session incarnation, not the first carrier's closed
    // watch channel.
    context
        .reliable_streams
        .retire_session(session_id, CloseReason::PolicyRejected);
    assert!(
        !replacement
            .receive(IpPacketId(1), ipv4_packet([10, 88, 0, 2], [10, 88, 0, 1]),)
            .expect("replacement peer packet is fenced")
    );
    assert!(
        !device
            .try_send_to_peer(ipv4_packet([10, 88, 0, 1], [10, 88, 0, 2]))
            .expect("replacement device packet is fenced")
    );
    assert!(matches!(
        replacement_commands.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    port.retire_session(session_id, CloseReason::PolicyRejected);
    assert!(matches!(
        replacement_commands.recv().await,
        Some(TestCarrierEvent::Close {
            tunnel_id: IpTunnelId(31),
            reason: CloseReason::PolicyRejected,
        })
    ));
}

#[test]
fn carrierless_logical_tunnel_preserves_session_principal_identity() {
    let (context, security) = server_context();
    let (port, _device) = ServerIpTunnelService::build(
        plan(&security),
        context.reliable_streams.clone(),
        4,
        16 * 1_500,
        context.session_retention_timeout,
    );
    let session_id = SessionId(32);
    let path = context.reliable_streams.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let attachment = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(32),
            path: &path,
            carrier: Arc::new(TestCarrier {
                events: tokio::sync::mpsc::unbounded_channel().0,
            }),
        })
        .expect("open retained logical tunnel");
    drop(attachment);
    drop(path);

    assert!(matches!(
        context.reliable_streams.register_carrier_path(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(1),
            ServerLocalPathProperties::default(),
            crate::product::PrincipalPermit::for_test("other-peer"),
        ),
        Err(crate::runtime::RuntimeError::AuthenticationRejected(
            "session principal changed across carrier paths"
        ))
    ));

    // The rejection is scoped to reuse of the retained SessionId; the same
    // authenticated principal remains valid for an unrelated session.
    context
        .reliable_streams
        .register_carrier_path(
            SessionId(33),
            UnderlayProtocol::Tcp,
            PathId(2),
            ServerLocalPathProperties::default(),
            crate::product::PrincipalPermit::for_test("other-peer"),
        )
        .expect("different session admits an unrelated principal");
}

#[tokio::test]
async fn complete_session_retirement_closes_only_matching_ip_tunnel() {
    let (context, security) = server_context();
    let (port, device) = ServerIpTunnelService::build(
        plan(&security),
        context.reliable_streams.clone(),
        4,
        16 * 1_500,
        context.session_retention_timeout,
    );
    let retired_session = SessionId(41);
    let retired_path = context.reliable_streams.register_test_carrier_path(
        retired_session,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let (retired_events, mut retired_commands) = tokio::sync::mpsc::unbounded_channel();
    let retired_attachment = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(41),
            path: &retired_path,
            carrier: Arc::new(TestCarrier {
                events: retired_events,
            }),
        })
        .expect("open retiring IP tunnel");

    context
        .reliable_streams
        .retire_session(retired_session, CloseReason::PolicyRejected);

    assert!(
        !retired_attachment
            .receive(IpPacketId(1), ipv4_packet([10, 88, 0, 2], [10, 88, 0, 1]),)
            .expect("the published session fence rejects peer packets before owner cleanup")
    );
    assert!(
        !device
            .try_send_to_peer(ipv4_packet([10, 88, 0, 1], [10, 88, 0, 2]))
            .expect("the published session fence rejects device packets before owner cleanup")
    );
    assert!(
        matches!(
            retired_commands.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ),
        "fenced packet operations must not reach the carrier",
    );

    port.retire_session(retired_session, CloseReason::PolicyRejected);

    assert!(matches!(
        retired_commands.recv().await,
        Some(TestCarrierEvent::Close {
            tunnel_id: IpTunnelId(41),
            reason: CloseReason::PolicyRejected,
        })
    ));
    assert!(
        !retired_attachment
            .receive(IpPacketId(2), ipv4_packet([10, 88, 0, 2], [10, 88, 0, 1]),)
            .expect("retired attachment is fenced")
    );
    assert!(
        !device
            .try_send_to_peer(ipv4_packet([10, 88, 0, 1], [10, 88, 0, 2]))
            .expect("retired tunnel has no outbound route")
    );
    assert!(matches!(
        port.open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(41),
            path: &retired_path,
            carrier: Arc::new(TestCarrier {
                events: tokio::sync::mpsc::unbounded_channel().0,
            }),
        }),
        Err(crate::runtime::RuntimeError::RemoteClosed(
            CloseReason::PolicyRejected
        ))
    ));

    let unrelated_path = context.reliable_streams.register_test_carrier_path(
        SessionId(42),
        UnderlayProtocol::Tcp,
        PathId(1),
        ServerLocalPathProperties::default(),
    );
    let (unrelated_events, _unrelated_commands) = tokio::sync::mpsc::unbounded_channel();
    port.open(ServerIpTunnelOpenRequest {
        tunnel_id: IpTunnelId(42),
        path: &unrelated_path,
        carrier: Arc::new(TestCarrier {
            events: unrelated_events,
        }),
    })
    .expect("unrelated session remains admissible");
}

#[test]
fn retired_inflight_opener_cannot_displace_same_principal_successor() {
    let (context, security) = server_context();
    let (port, device) = ServerIpTunnelService::build(
        plan(&security),
        context.reliable_streams.clone(),
        4,
        16 * 1_500,
        context.session_retention_timeout,
    );
    let successor_session = SessionId(52);
    let successor_path = context.reliable_streams.register_test_carrier_path(
        successor_session,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let (successor_events, mut successor_commands) = tokio::sync::mpsc::unbounded_channel();
    let _successor = port
        .open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(52),
            path: &successor_path,
            carrier: Arc::new(TestCarrier {
                events: successor_events,
            }),
        })
        .expect("open live successor tunnel");

    let retired_session = SessionId(51);
    let retired_path = context.reliable_streams.register_test_carrier_path(
        retired_session,
        UnderlayProtocol::Tcp,
        PathId(1),
        ServerLocalPathProperties::default(),
    );
    let entered = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    let hook_entered = entered.clone();
    let hook_release = release.clone();
    port.set_open_after_initial_retirement_check_hook(Some(Arc::new(move || {
        hook_entered.wait();
        hook_release.wait();
    })));
    let opener_port = port.clone();
    let (retired_events, _retired_commands) = tokio::sync::mpsc::unbounded_channel();
    let opener = std::thread::spawn(move || {
        opener_port.open(ServerIpTunnelOpenRequest {
            tunnel_id: IpTunnelId(51),
            path: &retired_path,
            carrier: Arc::new(TestCarrier {
                events: retired_events,
            }),
        })
    });

    entered.wait();
    context
        .reliable_streams
        .retire_session(retired_session, CloseReason::PolicyRejected);
    release.wait();
    let result = opener.join().expect("retired opener thread");
    port.set_open_after_initial_retirement_check_hook(None);

    assert!(matches!(
        result,
        Err(crate::runtime::RuntimeError::RemoteClosed(
            CloseReason::PolicyRejected
        ))
    ));
    assert!(
        matches!(
            successor_commands.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ),
        "a retired older opener must not close the current principal owner",
    );
    let reply = ipv4_packet([10, 88, 0, 1], [10, 88, 0, 2]);
    assert!(
        device
            .try_send_to_peer(reply.clone())
            .expect("successor remains routable")
    );
    assert!(matches!(
        successor_commands.try_recv(),
        Ok(TestCarrierEvent::Packet {
            tunnel_id: IpTunnelId(52),
            payload,
            ..
        }) if payload == reply
    ));
}

#[test]
fn server_packet_ranking_requires_delivered_capacity_evidence() {
    let path: crate::transport::PathSpec =
        "quic://127.0.0.1:9000?initial-srtt-ms=20&initial-rate-mbps=100"
            .parse()
            .expect("path");
    let local = ServerLocalPath::new(0, path);
    let startup = local.startup_metrics(PathId(0));
    let mut live = crate::protocol::PathMetrics {
        delivery_rate_bps: 700_000_000,
        pacing_rate_bps: 900_000_000,
        has_ack_derived_data_sample: false,
        ..startup
    };
    assert_eq!(
        server_packet_delivery_rate(Some(live), Some(startup)),
        startup.delivery_rate_bps as f64
    );

    live.has_ack_derived_data_sample = true;
    live.data_sample_count = 1;
    live.data_sample_bytes = 64 * 1024;
    live.rate_observed = true;
    live.rate_valid_for_us = 1_000_000;
    live.pacing_rate_observed = true;
    assert_eq!(
        server_packet_delivery_rate(Some(live), Some(startup)),
        700_000_000.0
    );
}
