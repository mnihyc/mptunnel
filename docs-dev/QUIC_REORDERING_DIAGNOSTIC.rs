// Diagnostic of existing behavior, NOT a desired-behavior acceptance test.
// To execute at 7189e69: paste the function into quinn-proto/src/tests/mod.rs,
// run the named native test with --nocapture, then remove it from that module.
// It uses the existing Pair and deterministic packet-number utilities.
// Temporary diagnosis; archived outside acceptance tests after execution.
#[test]
fn diagnostic_repeated_reordering_without_network_loss() {
    let mut config = client_config_with_deterministic_pns();
    let mut bbr = crate::congestion::Bbr3Config::default();
    bbr.loss_compensation_floor(0.10);
    Arc::get_mut(&mut config.transport)
        .unwrap()
        .congestion_controller_factory(Arc::new(bbr))
        .mtu_discovery_config(None);
    let mut pair = Pair::default_with_deterministic_pns();
    pair.latency = Duration::from_millis(50);
    let (client, server) = pair.connect_with(config);
    pair.drive();

    for round in 0..3 {
        let before = pair.client_conn_mut(client).stats();
        let rx_before = pair.server_conn_mut(server).stats().frame_rx.ping;
        let sent = pair.time;
        pair.client_conn_mut(client).ping();
        pair.drive_client();
        assert_eq!(pair.server.inbound.len(), 1);
        let mut held = pair.server.inbound.pop_front().unwrap();
        for _ in 0..4 {
            pair.client_conn_mut(client).ping();
            pair.drive_client();
        }
        assert_eq!(pair.server.inbound.len(), 4);
        pair.time += pair.latency;
        pair.drive_server();
        pair.time += pair.latency;
        pair.drive_client();
        let declared = pair.client_conn_mut(client).stats();
        assert_eq!(declared.path.lost_packets - before.path.lost_packets, 1);
        assert!(pair.time - sent < declared.path.rtt.mul_f32(9.0 / 8.0));

        // Deliver the exact original datagram, not a replacement. Every sent
        // PING arrives; the ACK of the delayed one is unambiguous late proof.
        held.0 = pair.time + Duration::from_millis(1);
        pair.server.inbound.push_front(held);
        pair.time += Duration::from_millis(1);
        pair.drive();
        let after = pair.client_conn_mut(client).stats();
        assert_eq!(
            pair.server_conn_mut(server).stats().frame_rx.ping - rx_before,
            5
        );
        if round > 0 {
            assert_eq!(
                after.path.spurious_congestion_events - before.path.spurious_congestion_events,
                1
            );
        }
        eprintln!(
            "round={round} all_originals_received=5 loss_declared={} undo={} rtt_us={}",
            after.path.lost_packets - before.path.lost_packets,
            after.path.spurious_congestion_events - before.path.spurious_congestion_events,
            declared.path.rtt.as_micros()
        );
    }
}
