// Temporary native Pair diagnostic, archived outside desired-behavior tests.
// Place in quinn-proto's tests module to reproduce the current receiver limit.
// This asserts observed rejection, not behavior an accepted fix must preserve.
#[test]
fn diagnostic_timely_original_beyond_receive_history() {
    for overtakes in [64, 130] {
        let mut config = client_config_with_deterministic_pns();
        let transport = Arc::get_mut(&mut config.transport).unwrap();
        transport.mtu_discovery_config(None);
        let mut cubic = crate::congestion::CubicConfig::default();
        cubic.initial_window(1024 * 1024);
        transport.congestion_controller_factory(Arc::new(cubic));
        let mut pair = Pair::default_with_deterministic_pns();
        pair.latency = Duration::from_millis(50);
        let (client, server) = pair.connect_with(config);
        pair.drive();
        // Keep >128 packets unacknowledged at the sender, so the held original
        // uses a two-byte packet number. This excludes one-byte PN expansion
        // ambiguity as an explanation for the later receiver discard.
        for _ in 0..150 {
            pair.time += Duration::from_micros(20);
            pair.client_conn_mut(client).ping();
            pair.drive_client();
        }
        assert_eq!(pair.server.inbound.len(), 150);
        pair.time += pair.latency;
        pair.drive_server();
        let before = pair.server_conn_mut(server).stats().frame_rx.ping;
        pair.client_conn_mut(client).ping();
        pair.drive_client();
        assert_eq!(pair.server.inbound.len(), 1);
        let mut held = pair.server.inbound.pop_front().unwrap();
        let held_sent = pair.time;
        for _ in 0..overtakes {
            pair.time += Duration::from_micros(20);
            pair.client_conn_mut(client).ping();
            pair.drive_client();
        }
        assert_eq!(pair.server.inbound.len(), overtakes);
        pair.time += pair.latency;
        pair.drive_server();
        pair.time += Duration::from_millis(10);
        held.0 = pair.time;
        pair.server.inbound.push_front(held);
        pair.drive_server();
        let received = pair.server_conn_mut(server).stats().frame_rx.ping - before;
        eprintln!("receive-history: overtakes={overtakes}, originals={}, received={received}, held_elapsed={:?}", overtakes + 1, pair.time - held_sent);
        assert_eq!(received, (overtakes + usize::from(overtakes < 129)) as u64);
    }
}
