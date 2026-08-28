use super::*;
use crate::config::{ClientPathConfig, ResourceLimits, SharedSecret};
use crate::ingress::ProxyAuthConfig;
use crate::model::path::next_carrier_path_instance_id;

fn tcp_path_instance(index: usize, id: u64) -> RelayPathInstance {
    RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(id.max(1)),
        attachment_id: id,
    }
}

fn tcp_path_test_context(path_count: usize) -> ClientPathContext {
    let paths = (0..path_count)
        .map(|index| {
            format!("tcp://127.0.0.1:{}", 12_700 + index)
                .parse::<PathSpec>()
                .expect("request TCP capacity test path")
        })
        .collect();
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("request TCP capacity test secret"),
    );
    ClientPathContext::new(paths, security, ResourceLimits::default())
        .expect("request TCP capacity test context")
}

fn udp_path_load_test_context() -> ClientPathContext {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("UDP load test secret"),
    );
    ClientPathContext::new(
        vec![
            "quic://127.0.0.1:12799"
                .parse::<PathSpec>()
                .expect("UDP load test path"),
        ],
        security,
        ResourceLimits::default(),
    )
    .expect("UDP load test context")
}

#[test]
fn tcp_carrier_groups_publish_every_bounded_pool_member() {
    let primary_security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("primary security"),
    );
    let secondary_security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"abcdef0123456789abcdef0123456789".to_vec())
            .expect("secondary security"),
    )
    .with_auth_freshness_window(Duration::from_secs(42));
    let context = ClientPathContext::new_with_path_configs_and_outbound(
        vec![
            ClientPathConfig {
                name: "primary".to_string(),
                spec: "tcp://127.0.0.1:12700?max-tcp-carriers=3"
                    .parse()
                    .expect("primary path"),
                security: primary_security.clone(),
                tls: crate::transport::encrypted::test_client_tls_config(),
            },
            ClientPathConfig {
                name: "secondary".to_string(),
                spec: "tcp://127.0.0.1:12701?max-tcp-carriers=2"
                    .parse()
                    .expect("secondary path"),
                security: secondary_security.clone(),
                tls: crate::transport::encrypted::test_client_tls_config(),
            },
        ],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        None,
    )
    .expect("TCP endpoint topology");

    assert_eq!(context.tcp_config_indices.as_slice(), [0, 1, 0, 0, 1]);
    assert_eq!(context.tcp_member_ordinals.as_slice(), [0, 0, 1, 2, 1]);
    assert_eq!(
        context
            .tcp_paths
            .iter()
            .map(|path| path.metadata.policy.backup)
            .collect::<Vec<_>>(),
        [false, false, true, true, true],
        "distinct endpoint primaries are regular and their siblings are ready backups"
    );
    assert_eq!(context.tcp_endpoint(0).expect("primary").members, [0, 2, 3]);
    assert_eq!(context.tcp_endpoint(1).expect("secondary").members, [1, 4]);
    assert_eq!(
        context.tcp_path_names.as_slice(),
        ["primary", "secondary", "primary", "primary", "secondary"]
    );
    assert_eq!(
        context.tcp_path_security(0).expect("primary security"),
        &primary_security
    );
    assert_eq!(
        context
            .tcp_path_security(4)
            .expect("secondary sibling security"),
        &secondary_security
    );
    assert!(std::ptr::eq(
        context.tcp_path_security(1).expect("secondary security"),
        context
            .tcp_path_security(4)
            .expect("secondary sibling security"),
    ));
    assert!(std::ptr::eq(
        context.tcp_path_tls(1).expect("secondary TLS"),
        context.tcp_path_tls(4).expect("secondary sibling TLS"),
    ));

    let health = context.health().lock().expect("path health");
    assert_eq!(health.tcp.len(), 5);
    drop(health);

    assert_eq!(
        context.automatic_bulk_path_count(UnderlayProtocol::Tcp, None),
        2,
        "automatic acquisition counts regular endpoint primaries before ready backups"
    );
    assert_eq!(
        context.ordered_tcp_path_indices(TrafficClass::Throughput, 64 * 1024),
        [0, 1, 2, 4, 3],
        "every bounded member is an establishment candidate"
    );
    let establishment = context
        .reserve_relay_path_load(
            RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 2,
            },
            TrafficClass::Throughput,
        )
        .expect("unestablished member can reserve its establishment transaction");
    drop(establishment);
    for path_index in 0..context.tcp_paths.len() {
        context.state.publish_tcp_peer_path_usage_committed(
            ClientTcpCarrierPublication {
                path_index,
                path_id: PathId(path_index as u16),
                path_instance_id: next_carrier_path_instance_id(),
                peer_usage_sequence: 0,
                peer_usage: PathUsage::Available,
                readiness_rtt: None,
            },
            || {},
        );
    }
    assert!(
        context
            .reserve_relay_path_load(
                RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: 2,
                },
                TrafficClass::Throughput,
            )
            .is_some()
    );
    assert_eq!(
        context.ordered_tcp_path_indices(TrafficClass::Throughput, 64 * 1024),
        [0, 1, 2, 4, 3]
    );

    let bounded_resources = ResourceLimits {
        max_paths: 2,
        ..ResourceLimits::default()
    };
    assert!(matches!(
        ClientPathContext::new(
            vec![
                "tcp://127.0.0.1:12702?max-tcp-carriers=3"
                    .parse()
                    .expect("bounded path")
            ],
            primary_security,
            bounded_resources,
        ),
        Err(RuntimeError::PathIdOverflow)
    ));
}

#[test]
fn single_tcp_endpoint_exposes_its_bounded_pool_as_regular_capacity() {
    let context = tcp_path_test_context(1);

    assert_eq!(context.tcp_member_ordinals.as_slice(), [0, 1, 2]);
    assert!(
        context
            .tcp_paths
            .iter()
            .all(|path| !path.metadata.policy.backup),
        "a lone endpoint has no distinct TCP primary to prefer over its bounded members"
    );
}

#[test]
fn multiple_default_tcp_endpoints_publish_their_bounded_pool_target() {
    let context = tcp_path_test_context(3);

    let health = context.health().lock().expect("path health");
    assert_eq!(health.tcp.len(), 9, "three bounded carriers per endpoint");
    drop(health);

    for config_index in 0..3 {
        let endpoint = context
            .tcp_endpoint(config_index)
            .expect("configured TCP endpoint group");
        assert_eq!(endpoint.range.max(), 3);
        assert_eq!(endpoint.members.len(), 3);
    }
    assert_eq!(
        (0..3)
            .map(|index| {
                usize::from(
                    context
                        .tcp_endpoint(index)
                        .expect("configured TCP endpoint group")
                        .range
                        .max(),
                )
            })
            .sum::<usize>(),
        9,
        "three default endpoints reserve three bounded carrier slots each"
    );
    assert_eq!(
        context.ordered_tcp_path_indices(TrafficClass::Throughput, 64 * 1024),
        [0, 1, 2, 3, 5, 7, 4, 6, 8],
        "equal-evidence startup visits every endpoint before a sibling tier"
    );

    let first = context
        .reserve_reliable_stream_path(TrafficClass::Throughput, 64 * 1024, &[])
        .expect("first flow path");
    let second = context
        .reserve_reliable_stream_path(TrafficClass::Throughput, 64 * 1024, &[])
        .expect("second flow path");
    let third = context
        .reserve_reliable_stream_path(TrafficClass::Throughput, 64 * 1024, &[])
        .expect("third flow path");
    assert_eq!(
        [first.key().index, second.key().index, third.key().index],
        [0, 1, 2]
    );
}

#[test]
fn stale_shared_load_snapshot_has_only_one_claim_winner() {
    let context = tcp_path_test_context(1);
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };

    let first = context
        .try_reserve_relay_path_load_if_unchanged(key, TrafficClass::Throughput, 0, 0)
        .expect("first exact snapshot claim");
    assert!(
        context
            .try_reserve_relay_path_load_if_unchanged(key, TrafficClass::Throughput, 0, 0)
            .is_none(),
        "a stale contender must rescore instead of sharing one idle candidate"
    );
    assert_eq!(
        context.health().lock().expect("path health lock").tcp[0].active_flows,
        1
    );

    drop(first);
    assert_eq!(
        context.health().lock().expect("path health lock").tcp[0].active_flows,
        0
    );
}

#[test]
fn relay_path_load_lease_rolls_back_scheduler_demand_on_drop() {
    let context = tcp_path_test_context(1);
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let lease = context
        .reserve_relay_path_load(key, TrafficClass::Throughput)
        .expect("path load lease");
    assert_eq!(lease.key(), key);
    assert_eq!(
        context.health().lock().expect("path health").tcp[0].active_flows,
        1
    );

    drop(lease);
    assert_eq!(
        context.health().lock().expect("path health").tcp[0].active_flows,
        0
    );
}

#[test]
fn udp_logical_load_lease_survives_physical_replacement_and_balances_on_drop() {
    let context = udp_path_load_test_context();
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let predecessor = next_carrier_path_instance_id();
    context.install_relay_path_instance_for_test(RelayPathInstance {
        key,
        path_instance_id: predecessor,
        attachment_id: 0,
    });
    let lease = context
        .reserve_relay_path_load(key, TrafficClass::RealtimeDatagram)
        .expect("UDP association load lease");
    assert_eq!(
        context.health().lock().expect("UDP predecessor health").udp[0].active_flows,
        1,
    );

    let successor = next_carrier_path_instance_id();
    context.install_relay_path_instance_for_test(RelayPathInstance {
        key,
        path_instance_id: successor,
        attachment_id: 0,
    });
    {
        let health = context.health().lock().expect("UDP successor health");
        assert_eq!(health.udp[0].path_instance_id(), Some(successor));
        assert_eq!(
            health.udp[0].active_flows, 1,
            "physical N to N+1 publication cannot duplicate or release logical association load",
        );
    }
    drop(lease);
    assert_eq!(
        context.health().lock().expect("UDP released health").udp[0].active_flows,
        0,
        "success, failure, and cancellation all balance through the same RAII drop",
    );
}

#[test]
fn relay_path_load_lease_releases_the_reclassified_lane() {
    let context = tcp_path_test_context(1);
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let mut lease = context
        .reserve_relay_path_load(key, TrafficClass::Throughput)
        .expect("path load lease");
    context.change_relay_path_lane_load(
        UnderlayProtocol::Tcp,
        0,
        TrafficClass::Throughput,
        TrafficClass::Latency,
    );
    lease.set_recorded_lane(TrafficClass::Latency);

    drop(lease);
    let health = context.health().lock().expect("path health");
    assert_eq!(health.tcp[0].active_flows, 0);
    assert_eq!(health.tcp[0].active_latency_sensitive_flows, 0);
}

#[test]
fn tcp_replacement_publication_is_exact_instance_atomic_during_product_work() {
    let paths = vec![
        "tcp://127.0.0.1:12720"
            .parse::<PathSpec>()
            .expect("TCP path"),
        "quic://127.0.0.1:12721"
            .parse::<PathSpec>()
            .expect("QUIC path"),
    ];
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("session Product ownership secret"),
    );
    let context = ClientPathContext::new(paths, security, ResourceLimits::default())
        .expect("mixed path context");
    let tcp_instance = next_carrier_path_instance_id();
    context.state.install_peer_path_usage(
        UnderlayProtocol::Tcp,
        0,
        tcp_instance,
        0,
        PathUsage::Available,
    );
    {
        let now = Instant::now();
        let mut health = context.health().lock().expect("path health");
        let predecessor = &mut health.tcp[0];
        predecessor.measured_rate_bps = Some(900_000_000.0);
        predecessor.delivery_samples = 7;
        predecessor.product_delivery_rate_bps = Some(850_000_000.0);
        predecessor.product_delivery_sample_bytes = 512 * 1024;
        predecessor.last_delivery_at = Some(now);
        predecessor.delivery_rate_expires_at = Some(now + Duration::from_secs(1));
        predecessor.carrier_srtt_ms = Some(18.0);
        predecessor.carrier_rttvar_ms = Some(3.0);
        predecessor.carrier_delivery_rate_bps = Some(920_000_000.0);
        predecessor.carrier_pacing_rate_bps = Some(940_000_000.0);
        predecessor.carrier_bytes_in_flight = 128 * 1024;
        predecessor.carrier_bytes_in_flight_observed = true;
        predecessor.carrier_queue_bytes = 64 * 1024;
        predecessor.carrier_queue_bytes_observed = true;
        predecessor.carrier_inflight_limit_bytes = 256 * 1024;
        predecessor.carrier_delivery_samples = 9;
        predecessor.carrier_delivery_sample_bytes = 768 * 1024;
        predecessor.carrier_last_delivery_at = Some(now);
        predecessor.carrier_bulk_proof_expires_at = Some(now + Duration::from_secs(1));
        predecessor.carrier_ack_derived_data_seen = true;
        predecessor.path_proof_success = true;
    }
    let product_flow = context.reserve_session_product_flow();
    let udp_load = context
        .reserve_relay_path_load(
            RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 0,
            },
            TrafficClass::RealtimeDatagram,
        )
        .expect("cross-underlay Product load");
    let replacement_instance = next_carrier_path_instance_id();
    let mut published = false;
    assert!(
        context.state.publish_tcp_replacement_if_current(
            tcp_instance,
            ClientTcpCarrierPublication {
                path_index: 0,
                path_id: PathId(8),
                path_instance_id: replacement_instance,
                peer_usage_sequence: 0,
                peer_usage: PathUsage::Available,
                readiness_rtt: None,
            },
            || published = true,
        ),
        "active logical and cross-underlay work precedes the atomic instance swap; it does not invalidate the exact predecessor"
    );
    assert!(published);
    {
        let health = context.health().lock().expect("path health");
        let successor = &health.tcp[0];
        assert_eq!(successor.path_instance_id(), Some(replacement_instance));
        assert_eq!(successor.measured_rate_bps, None);
        assert_eq!(successor.delivery_samples, 0);
        assert_eq!(successor.product_delivery_rate_bps, None);
        assert_eq!(successor.product_delivery_sample_bytes, 0);
        assert_eq!(successor.last_delivery_at, None);
        assert_eq!(successor.delivery_rate_expires_at, None);
        assert_eq!(successor.carrier_srtt_ms, None);
        assert_eq!(successor.carrier_rttvar_ms, None);
        assert_eq!(successor.carrier_delivery_rate_bps, None);
        assert_eq!(successor.carrier_pacing_rate_bps, None);
        assert_eq!(successor.carrier_bytes_in_flight, 0);
        assert!(!successor.carrier_bytes_in_flight_observed);
        assert_eq!(successor.carrier_queue_bytes, 0);
        assert!(!successor.carrier_queue_bytes_observed);
        assert_eq!(successor.carrier_inflight_limit_bytes, 0);
        assert_eq!(successor.carrier_delivery_samples, 0);
        assert_eq!(successor.carrier_delivery_sample_bytes, 0);
        assert_eq!(successor.carrier_last_delivery_at, None);
        assert_eq!(successor.carrier_bulk_proof_expires_at, None);
        assert!(!successor.carrier_ack_derived_data_seen);
        assert!(!successor.path_proof_success);
    }
    assert!(
        !context.state.publish_tcp_replacement_if_current(
            tcp_instance,
            ClientTcpCarrierPublication {
                path_index: 0,
                path_id: PathId(9),
                path_instance_id: next_carrier_path_instance_id(),
                peer_usage_sequence: 0,
                peer_usage: PathUsage::Available,
                readiness_rtt: None,
            },
            || panic!("stale predecessor must not publish another successor"),
        ),
        "a stale predecessor cannot replace the current exact instance"
    );

    drop(product_flow);
    drop(udp_load);
}

#[test]
fn peer_path_usage_is_directional_and_sequence_ordered_per_underlay() {
    let paths = vec![
        "tcp://127.0.0.1:12710"
            .parse::<PathSpec>()
            .expect("TCP path"),
        "quic://127.0.0.1:12711"
            .parse::<PathSpec>()
            .expect("QUIC path"),
    ];
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("path usage test secret"),
    );
    let context = ClientPathContext::new(paths, security, ResourceLimits::default())
        .expect("path usage test context");
    let tcp_g1 = next_carrier_path_instance_id();
    let udp_g1 = next_carrier_path_instance_id();

    context
        .state
        .install_peer_path_usage(UnderlayProtocol::Tcp, 0, tcp_g1, 0, PathUsage::Backup);
    context.state.install_peer_path_usage(
        UnderlayProtocol::Udp,
        0,
        udp_g1,
        0,
        PathUsage::Available,
    );
    assert_eq!(
        context.state.peer_path_usage(UnderlayProtocol::Tcp, 0),
        Some(PathUsage::Backup)
    );
    assert_eq!(
        context.state.peer_path_usage(UnderlayProtocol::Udp, 0),
        Some(PathUsage::Available),
        "one peer preference must not leak across TCP and QUIC path spaces"
    );

    assert!(context.state.update_peer_path_usage(
        UnderlayProtocol::Tcp,
        0,
        tcp_g1,
        2,
        PathUsage::Available,
    ));
    assert!(!context.state.update_peer_path_usage(
        UnderlayProtocol::Tcp,
        0,
        tcp_g1,
        1,
        PathUsage::Backup,
    ));
    assert!(!context.state.update_peer_path_usage(
        UnderlayProtocol::Tcp,
        0,
        tcp_g1,
        2,
        PathUsage::Backup,
    ));
    assert_eq!(
        context.state.peer_path_usage(UnderlayProtocol::Tcp, 0),
        Some(PathUsage::Available),
        "stale or duplicate PATH_STATUS must not overwrite the newest preference"
    );

    let tcp_g2 = next_carrier_path_instance_id();
    context
        .state
        .install_peer_path_usage(UnderlayProtocol::Tcp, 0, tcp_g2, 0, PathUsage::Backup);
    assert!(!context.state.update_peer_path_usage(
        UnderlayProtocol::Tcp,
        0,
        tcp_g1,
        3,
        PathUsage::Available,
    ));
    assert_eq!(
        context.state.peer_path_usage(UnderlayProtocol::Tcp, 0),
        Some(PathUsage::Backup),
        "a late prior-instance PATH_STATUS cannot overwrite the replacement carrier"
    );
    assert!(context.state.update_peer_path_usage(
        UnderlayProtocol::Tcp,
        0,
        tcp_g2,
        1,
        PathUsage::Available,
    ));
}

#[test]
fn request_tcp_capacity_budget_tracks_session_path_and_campaign_spend() {
    let context = tcp_path_test_context(1);
    let session_limit = reliable_capacity_measurement_session_limit_bytes(context.mux_limits);
    let train_bytes = 1024 * 1024;
    let path_share = 8 * 1024 * 1024;
    let tcp_campaign = Arc::new(RequestCapacityProbeCampaignBudget::default());

    let now = Instant::now();
    let tcp = context
        .try_reserve_request_tcp_capacity_probe(
            StreamId(70),
            0,
            tcp_path_instance(0, 100),
            51,
            train_bytes,
            path_share,
            tcp_campaign.clone(),
            PATH_OPEN_SCORE_BYTES as u64,
            now,
            now + Duration::from_secs(30),
            CapacityProbeCommandTicket::new(),
        )
        .expect("reserve TCP carrier spend");
    assert!(tcp.commit());
    drop(tcp);

    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        session_limit - train_bytes
    );
    assert_eq!(
        context.request_tcp_capacity_probe_path_remaining_bytes(0, path_share),
        path_share - train_bytes
    );
    assert_eq!(
        tcp_campaign.remaining_bytes(path_share),
        path_share - train_bytes
    );
}
