use super::service::server_packet_delivery_rate;
use super::*;
use crate::config::{MppPerformanceConfig, ResourceLimits, ServerSecurityConfig, SharedSecret};
use crate::outbound::OutboundConfig;
use crate::product::{PrincipalId, TunL3AddressPlan, TunL3AllocationSpec, TunL3ServerSpec};
use crate::protocol::{IpPacketId, IpTunnelId, PathId, SessionId, UnderlayProtocol};
use crate::runtime::node::server::{ServerIdentityRuntime, new_identity_runtime};
use crate::runtime::path::{ServerLocalPath, ServerLocalPathProperties};
use bytes::Bytes;
use std::net::Ipv4Addr;
use std::sync::Arc;

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
    );
    let local_path: crate::transport::PathSpec = "tcp://127.0.0.1:9000?srtt-ms=20&rate-mbps=500"
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

#[test]
fn server_packet_ranking_requires_delivered_capacity_evidence() {
    let path: crate::transport::PathSpec = "udp://127.0.0.1:9000?srtt-ms=20&rate-mbps=100"
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
    assert_eq!(
        server_packet_delivery_rate(Some(live), Some(startup)),
        700_000_000.0
    );
}
