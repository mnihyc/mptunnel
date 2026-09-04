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

fn packet_flow(source_port: u16) -> IpPacketFlowKey {
    let mut packet = vec![
        0x45, 0, 0, 24, 0, 1, 0, 0, 64, 17, 0, 0, 10, 88, 0, 2, 10, 88, 0, 1, 0, 0, 0, 53,
    ];
    packet[20..22].copy_from_slice(&source_port.to_be_bytes());
    parse_ip_packet(&packet)
        .expect("valid IPv4 test packet")
        .flow_key
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

#[test]
fn stale_same_rate_native_decision_cannot_queue_or_bind_packet_flow() {
    let path_instance_id = CarrierPathInstanceId::from_raw(91);
    let scope =
        CarrierRateAuthorityScope::new(path_instance_id, PathMetricDirection::ClientToServer);
    let authority =
        crate::runtime::path::authority::NativeCarrierRateAuthorityHandle::from_observation_for_test(
            scope,
            80_000_000,
            1,
            7,
            Some(80_000_000),
        )
        .expect("initial native authority");
    let initial_shape = authority
        .refresh_scheduling_shape_for_test(
            scope,
            1,
            7,
            Some(80_000_000),
            std::time::Duration::from_millis(80),
            std::time::Duration::from_millis(10),
            512 * 1024,
            0,
            1_400,
            Some(80_000_000),
            false,
        )
        .expect("initial coherent shape");
    let key = ClientIpCarrierKey {
        path: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        path_instance_id,
    };
    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut state = ClientIpTunnelState::new(IpTunnelId(1), 8, 64 * 1024);
    state.carriers.insert(
        key,
        ClientIpCarrierState {
            carrier: ClientIpCarrier::NativeTest {
                authority: authority.clone(),
                accepted: accepted.clone(),
            },
            ready: true,
        },
    );
    let flow = packet_flow(41_000);
    let candidate = PacketPathCandidate {
        attachment: PacketPathAttachment {
            key: key.path,
            path_instance_id,
        },
        snapshot: crate::scheduler::PathSnapshot::new(
            crate::protocol::PathId(0),
            UnderlayProtocol::Udp,
            80.0,
            80_000_000.0,
        )
        .with_scheduling_service_rate(initial_shape.service_rate()),
        eta_ms: 80.0,
        native_authority_stamp: Some(initial_shape.stamp()),
    };

    let changed = authority
        .publish_observation_for_test(2, 8, Some(80_000_000))
        .expect("same-rate replacement advances the complete authority stamp");
    assert_ne!(changed.stamp(), initial_shape.stamp());

    let outcome = state
        .try_send_planned(
            candidate,
            &flow,
            IpPacketId(1),
            Bytes::from_static(b"packet"),
        )
        .expect("stale decision is an operation-local rejection");
    assert_eq!(outcome, ClientIpPacketSendOutcome::StaleNativeDecision);
    assert_eq!(
        accepted.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "stale authority must be rejected before queue acceptance",
    );
    assert_eq!(
        state.flows.current(&flow, Instant::now(), |_| true),
        None,
        "stale authority must be rejected before flow binding",
    );
    assert!(
        state.carriers.contains_key(&key),
        "authority churn is not carrier-failure evidence",
    );

    let current_shape = authority
        .refresh_scheduling_shape_for_test(
            scope,
            2,
            8,
            Some(80_000_000),
            std::time::Duration::from_millis(80),
            std::time::Duration::from_millis(10),
            512 * 1024,
            0,
            1_400,
            Some(80_000_000),
            false,
        )
        .expect("replacement coherent shape");
    let replanned = PacketPathCandidate {
        snapshot: candidate
            .snapshot
            .with_scheduling_service_rate(current_shape.service_rate()),
        native_authority_stamp: Some(current_shape.stamp()),
        ..candidate
    };
    let final_shape = authority
        .refresh_scheduling_shape_for_test(
            scope,
            2,
            8,
            Some(80_000_000),
            std::time::Duration::from_millis(300),
            std::time::Duration::from_millis(50),
            512 * 1024,
            0,
            1_400,
            Some(80_000_000),
            false,
        )
        .expect("same-controller shape refresh");
    assert_eq!(
        final_shape.stamp(),
        current_shape.stamp(),
        "RTT/window refreshes do not revise the central rate stamp",
    );
    let accepted_at = Instant::now();
    assert_eq!(
        state
            .try_send_planned(
                replanned,
                &flow,
                IpPacketId(1),
                Bytes::from_static(b"packet"),
            )
            .expect("fresh replan commits"),
        ClientIpPacketSendOutcome::Accepted,
    );
    assert_eq!(accepted.load(std::sync::atomic::Ordering::Relaxed), 1,);
    assert_eq!(
        state.flows.planned_current(
            &flow,
            accepted_at + std::time::Duration::from_millis(200),
            |_| true,
        ),
        Some(key),
        "accepted affinity uses the final same-stamp Native RTT, not the stale planning PTO",
    );
}
