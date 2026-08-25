use super::*;
use std::future::poll_fn;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn target(port: u16) -> TargetAddr {
    TargetAddr::Ip(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
}

#[test]
fn flow_lifecycle_and_bounded_registry_remain_distinct() {
    let telemetry = RuntimeTelemetry::new(1);
    let reliable = telemetry.open_reliable_flow(Some(SessionId(7)), StreamId(11), target(443));
    let datagram = telemetry.open_datagram_flow(Some(SessionId(7)), DatagramFlowId(13), target(53));

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.reliable.flows.opened, 1);
    assert_eq!(snapshot.reliable.flows.active, 1);
    assert_eq!(snapshot.datagram.flows.opened, 1);
    assert_eq!(snapshot.datagram.flows.active, 1);
    assert_eq!(snapshot.active_flows.len(), 1);
    assert_eq!(snapshot.active_flow_record_overflow, 1);
    assert_eq!(snapshot.active_flow_record_overflow_total, 1);

    reliable.complete();
    drop(datagram);
    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.reliable.flows.completed, 1);
    assert_eq!(snapshot.reliable.flows.failed, 0);
    assert_eq!(snapshot.datagram.flows.completed, 0);
    assert_eq!(snapshot.datagram.flows.failed, 1);
    assert_eq!(snapshot.reliable.flows.active, 0);
    assert_eq!(snapshot.datagram.flows.active, 0);
    assert!(snapshot.active_flows.is_empty());
    assert_eq!(snapshot.active_flow_record_overflow, 0);
    assert_eq!(snapshot.active_flow_record_overflow_total, 1);
}

#[test]
fn production_flow_detail_capacity_is_independent_from_forwarding_capacity() {
    assert_eq!(active_flow_detail_capacity(8), 8);
    assert_eq!(
        active_flow_detail_capacity(65_536),
        MAX_ACTIVE_FLOW_DETAIL_RECORDS
    );
}

#[test]
fn datagrams_count_one_product_packet_independent_of_payload_size() {
    let telemetry = RuntimeTelemetry::new(1);
    let flow = telemetry.open_datagram_flow(None, DatagramFlowId(3), target(53));
    let counter = flow.counter();

    counter.record_datagram_to_peer(1200);
    counter.record_datagram_to_peer(0);
    counter.record_datagram_from_peer(900);

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.datagram.io.to_peer_bytes, 1200);
    assert_eq!(snapshot.datagram.io.to_peer_packets, 2);
    assert_eq!(snapshot.datagram.io.from_peer_bytes, 900);
    assert_eq!(snapshot.datagram.io.from_peer_packets, 1);
    assert_eq!(snapshot.io, snapshot.datagram.io);
    assert_eq!(snapshot.active_flows[0].io, snapshot.datagram.io);
}

#[test]
fn reusable_local_datagram_association_does_not_claim_one_target() {
    let telemetry = RuntimeTelemetry::new(1);
    let _flow = telemetry.open_local_datagram_flow(Some(SessionId(9)));

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.active_flows.len(), 1);
    assert_eq!(snapshot.active_flows[0].session_id, Some(SessionId(9)));
    assert_eq!(snapshot.active_flows[0].target, None);
}

#[test]
fn active_flow_snapshots_preserve_registration_order() {
    let telemetry = RuntimeTelemetry::new(3);
    let _first = telemetry.open_reliable_flow(None, StreamId(30), target(443));
    let _second = telemetry.open_reliable_flow(None, StreamId(10), target(443));
    let _third = telemetry.open_reliable_flow(None, StreamId(20), target(443));

    let flow_ids = telemetry
        .snapshot()
        .active_flows
        .into_iter()
        .map(|flow| flow.flow_id)
        .collect::<Vec<_>>();
    assert_eq!(
        flow_ids,
        vec![
            ProductFlowId::Reliable(StreamId(30)),
            ProductFlowId::Reliable(StreamId(10)),
            ProductFlowId::Reliable(StreamId(20)),
        ]
    );
}

#[test]
fn generation_owner_records_only_scoped_product_flows_with_immutable_attribution() {
    let telemetry = RuntimeTelemetry::generation_owner(4);
    let internal = telemetry.open_reliable_flow(None, StreamId(1), target(853));
    internal.counter().record_to_peer_bytes(99);
    assert_eq!(
        telemetry.snapshot().reliable.flows.opened,
        0,
        "unscoped DNS/probe work must remain outside Product totals"
    );

    let flow = FlowContext::new(
        Network::Tcp,
        crate::product::ProtocolTarget::parse_authority("service.example:443").expect("target"),
        crate::product::SourceEndpoint::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 32100),
        crate::product::PrincipalId::parse("daily-user").expect("principal"),
        crate::product::InboundId::parse("local-socks").expect("inbound"),
    );
    let scope = ProductFlowScope::from_flow(
        ProductFlowOriginKind::LocalInbound,
        &flow,
        crate::product::OutboundId::parse("edge-b").expect("outbound"),
        Some(crate::product::BalancerId::parse("daily-egress").expect("balancer")),
        crate::runtime::telemetry::ProductFlowSource::local_peer(
            "127.0.0.1:32100".parse().expect("local peer"),
        ),
    );
    let native = telemetry.open_native_reliable_flow(scope);
    native.counter().record_to_peer_bytes(7);
    native.counter().record_from_peer_bytes(8);

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.reliable.flows.opened, 1);
    assert_eq!(snapshot.io.to_peer_bytes, 7);
    assert_eq!(snapshot.io.from_peer_bytes, 8);
    let active = &snapshot.active_flows[0];
    assert_eq!(active.display_id, 1);
    assert_eq!(active.network, Network::Tcp);
    assert_eq!(
        active.target.as_ref().map(TargetAddr::authority),
        Some("service.example:443".to_string())
    );
    assert_eq!(
        active.origin.as_ref().map(|origin| origin.inbound.as_str()),
        Some("local-socks")
    );
    assert_eq!(
        active
            .origin
            .as_ref()
            .map(|origin| origin.source)
            .map(|source| (source.kind, source.endpoint)),
        Some((
            crate::runtime::telemetry::ProductFlowSourceKind::LocalPeer,
            "127.0.0.1:32100".parse().expect("local peer"),
        ))
    );
    let selection = active.selection.as_ref().expect("selection");
    assert_eq!(selection.outbound.as_str(), "edge-b");
    assert_eq!(
        selection.balancer.as_ref().map(|id| id.as_str()),
        Some("daily-egress")
    );
    assert_eq!(
        selection.member.as_ref().map(|id| id.as_str()),
        Some("edge-b")
    );
}

#[tokio::test]
async fn observed_io_counts_only_successfully_transferred_bytes() {
    let telemetry = RuntimeTelemetry::new(1);
    let flow = telemetry.open_reliable_flow(None, StreamId(5), target(443));
    let (product, mut peer) = tokio::io::duplex(64);
    let mut observed = ObservedProductIo::new(product, flow.counter());

    peer.write_all(b"request")
        .await
        .expect("seed product input");
    let mut request = [0_u8; 7];
    observed
        .read_exact(&mut request)
        .await
        .expect("read product input");
    observed
        .write_all(b"response")
        .await
        .expect("write product output");
    let mut response = [0_u8; 8];
    peer.read_exact(&mut response)
        .await
        .expect("read product output");

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.reliable.io.to_peer_bytes, 7);
    assert_eq!(snapshot.reliable.io.from_peer_bytes, 8);
    assert_eq!(snapshot.reliable.io.to_peer_packets, 0);
    assert_eq!(snapshot.reliable.io.from_peer_packets, 0);
    assert_eq!(snapshot.active_flows[0].io, snapshot.reliable.io);
}

#[tokio::test]
async fn pending_io_does_not_change_counters() {
    let telemetry = RuntimeTelemetry::new(1);
    let flow = telemetry.open_reliable_flow(None, StreamId(17), target(443));
    let (product, mut peer) = tokio::io::duplex(1);
    let mut observed = ObservedProductIo::new(product, flow.counter());
    let mut byte = [0_u8; 1];

    let pending =
        poll_fn(
            |cx| match Pin::new(&mut observed).poll_read(cx, &mut ReadBuf::new(&mut byte)) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(result) => panic!("unexpected ready read: {result:?}"),
            },
        );
    pending.await;
    assert_eq!(telemetry.snapshot().reliable.io.to_peer_bytes, 0);

    peer.write_all(b"x").await.expect("seed product input");
    observed
        .read_exact(&mut byte)
        .await
        .expect("read product input");
    assert_eq!(telemetry.snapshot().reliable.io.to_peer_bytes, 1);

    observed.write_all(b"y").await.expect("fill product output");
    let pending = poll_fn(|cx| match Pin::new(&mut observed).poll_write(cx, b"z") {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(result) => panic!("unexpected ready write: {result:?}"),
    });
    pending.await;
    assert_eq!(telemetry.snapshot().reliable.io.from_peer_bytes, 1);
}
