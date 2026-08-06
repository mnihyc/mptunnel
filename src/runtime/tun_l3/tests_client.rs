use super::*;

fn event(frame: Frame) -> ClientIpTunnelEvent {
    ClientIpTunnelEvent {
        path: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(1),
        frame,
    }
}

#[test]
fn accepted_events_preserve_wire_order_with_independent_lifecycle_headroom() {
    let hub = ClientIpTunnelHub::default();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let _registration = hub
        .register(ClientIpTunnelSink::new(events_tx, 24, 2))
        .expect("register packet ingress");

    hub.route(event(Frame::IpPacket {
        tunnel_id: IpTunnelId(1),
        packet_id: IpPacketId(1),
        payload: Bytes::from_static(&[0_u8; 24]),
    }))
    .expect("one full-MTU-equivalent packet fits the payload budget");
    assert!(matches!(
        hub.route(event(Frame::IpPacket {
            tunnel_id: IpTunnelId(1),
            packet_id: IpPacketId(2),
            payload: Bytes::from_static(&[0_u8; 1]),
        })),
        Err(RuntimeError::SenderServiceBlocked)
    ));

    hub.route(event(Frame::IpTunnelReady {
        tunnel_id: IpTunnelId(1),
        mtu: 1_500,
        addresses: vec!["10.88.0.2".parse().expect("address")],
    }))
    .expect("lifecycle event bypasses packet budget");

    assert!(matches!(
        events_rx.try_recv(),
        Ok(ClientIpTunnelInput::Packet {
            event: ClientIpTunnelEvent {
                frame: Frame::IpPacket { .. },
                ..
            },
            ..
        })
    ));
    assert!(matches!(
        events_rx.try_recv(),
        Ok(ClientIpTunnelInput::Lifecycle {
            event: ClientIpTunnelEvent {
                frame: Frame::IpTunnelReady { .. },
                ..
            },
            ..
        })
    ));
}

#[tokio::test]
async fn carrier_retirement_follows_preceding_wire_events() {
    let hub = ClientIpTunnelHub::default();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink = ClientIpTunnelSink::new(events_tx, 64, 4);
    let _registration = hub.register(sink.clone()).expect("register packet ingress");
    let key = ClientIpCarrierKey {
        path: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(1),
    };

    hub.route(event(Frame::IpPacket {
        tunnel_id: IpTunnelId(1),
        packet_id: IpPacketId(1),
        payload: Bytes::from_static(&[0_u8; 24]),
    }))
    .expect("queue packet");
    hub.route(event(Frame::IpTunnelClose {
        tunnel_id: IpTunnelId(1),
        reason: CloseReason::Normal,
    }))
    .expect("queue close");
    let update = tokio::spawn({
        let sink = sink.clone();
        async move {
            sink.send_update(ClientIpCarrierUpdate::Retired { key })
                .await
        }
    });

    assert!(matches!(
        events_rx.recv().await,
        Some(ClientIpTunnelInput::Packet { .. })
    ));
    assert!(matches!(
        events_rx.recv().await,
        Some(ClientIpTunnelInput::Lifecycle {
            event: ClientIpTunnelEvent {
                frame: Frame::IpTunnelClose { .. },
                ..
            },
            ..
        })
    ));
    let Some(ClientIpTunnelInput::CarrierUpdate {
        update: ClientIpCarrierUpdate::Retired { .. },
        processed,
        ..
    }) = events_rx.recv().await
    else {
        panic!("retirement must follow preceding wire events");
    };
    let _ = processed.send(());
    update
        .await
        .expect("update task")
        .expect("acknowledge retirement");
}
