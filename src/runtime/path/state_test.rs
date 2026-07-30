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

#[test]
fn tcp_endpoint_topology_preserves_configured_primaries_and_dormant_capacity() {
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
                spec: "tcp://127.0.0.1:12700?tcp-carriers=1-3"
                    .parse()
                    .expect("primary path"),
                security: primary_security.clone(),
                tls: crate::transport::encrypted::test_client_tls_config(),
            },
            ClientPathConfig {
                name: "secondary".to_string(),
                spec: "tcp://127.0.0.1:12701?tcp-carriers=2-2"
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
            .tcp_path_security(3)
            .expect("primary sibling security"),
        &primary_security
    );
    assert_eq!(
        context
            .tcp_path_security(4)
            .expect("secondary sibling security"),
        &secondary_security
    );
    assert!(std::ptr::eq(
        context.tcp_path_security(0).expect("primary security"),
        context
            .tcp_path_security(3)
            .expect("primary sibling security"),
    ));
    assert!(std::ptr::eq(
        context.tcp_path_tls(0).expect("primary TLS"),
        context.tcp_path_tls(3).expect("primary sibling TLS"),
    ));

    let health = context.health().lock().expect("path health");
    let locally_eligible = health
        .tcp
        .iter()
        .map(ClientPathHealthRecord::is_locally_eligible)
        .collect::<Vec<_>>();
    assert_eq!(locally_eligible, [true, true, false, false, true]);
    drop(health);

    assert_eq!(
        context.automatic_bulk_path_count(UnderlayProtocol::Tcp, None),
        3
    );
    assert!(!context.should_probe_tcp_path(2));
    assert!(!context.should_probe_tcp_path(3));
    assert!(
        context
            .reserve_relay_path_load(
                RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: 2,
                },
                TrafficClass::Throughput,
            )
            .is_none()
    );
    assert_eq!(
        context.ordered_tcp_path_indices(TrafficClass::Throughput, 64 * 1024),
        [0, 1, 4]
    );

    let dormant_instance = next_carrier_path_instance_id();
    let dormant_registration = context.state.register_authenticated_path(
        UnderlayProtocol::Tcp,
        2,
        PathId(12),
        AuthNonce([12; 16]),
        dormant_instance,
        0,
        PathUsage::Available,
    );
    let dormant_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 2,
    };
    assert_eq!(
        context.current_request_tcp_service_carrier(dormant_key),
        None,
        "dormant capacity must remain absent from accepted Product scheduling"
    );
    let candidate = context
        .current_request_tcp_service_candidate(dormant_key)
        .expect("authenticated dormant capacity is validation-candidate authority");
    assert!(context.state.update_peer_path_usage(
        UnderlayProtocol::Tcp,
        2,
        dormant_instance,
        1,
        PathUsage::Available,
    ));
    assert_eq!(
        context
            .current_request_tcp_service_candidate(dormant_key)
            .expect("unchanged candidate authority")
            .eligibility_generation,
        candidate.eligibility_generation,
        "an unchanged peer preference must not advance the candidate fence"
    );
    assert!(context.state.update_peer_path_usage(
        UnderlayProtocol::Tcp,
        2,
        dormant_instance,
        2,
        PathUsage::Backup,
    ));
    assert_eq!(
        context.current_request_tcp_service_candidate(dormant_key),
        None,
        "peer withdrawal must invalidate dormant validation authority"
    );
    assert!(context.state.update_peer_path_usage(
        UnderlayProtocol::Tcp,
        2,
        dormant_instance,
        3,
        PathUsage::Available,
    ));
    assert!(
        context
            .current_request_tcp_service_candidate(dormant_key)
            .expect("restored candidate authority")
            .eligibility_generation
            > candidate.eligibility_generation
    );
    assert!(
        context
            .reserve_relay_path_load(dormant_key, TrafficClass::Throughput)
            .is_none(),
        "candidate authority must not make a dormant slot schedulable"
    );
    drop(dormant_registration);
    assert_eq!(
        context.current_request_tcp_service_candidate(dormant_key),
        None,
        "carrier-owner retirement must remove dormant candidate authority"
    );

    let bounded_resources = ResourceLimits {
        max_paths: 2,
        ..ResourceLimits::default()
    };
    assert!(matches!(
        ClientPathContext::new(
            vec![
                "tcp://127.0.0.1:12702?tcp-carriers=1-3"
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
fn peer_path_usage_is_directional_and_sequence_ordered_per_underlay() {
    let paths = vec![
        "tcp://127.0.0.1:12710"
            .parse::<PathSpec>()
            .expect("TCP path"),
        "udp://127.0.0.1:12711"
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
    let tcp_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let available_authority = context
        .current_request_tcp_service_carrier(tcp_key)
        .expect("AVAILABLE authenticated TCP carrier authority");
    assert!(context.state.update_peer_path_usage(
        UnderlayProtocol::Tcp,
        0,
        tcp_g1,
        3,
        PathUsage::Available,
    ));
    assert_eq!(
        context
            .current_request_tcp_service_carrier(tcp_key)
            .expect("unchanged AVAILABLE authority")
            .eligibility_generation,
        available_authority.eligibility_generation,
        "a newer status sequence with unchanged usage is not an eligibility change"
    );
    assert!(context.state.update_peer_path_usage(
        UnderlayProtocol::Tcp,
        0,
        tcp_g1,
        4,
        PathUsage::Backup,
    ));
    assert_eq!(
        context.current_request_tcp_service_carrier(tcp_key),
        None,
        "BACKUP preference cannot be bypassed for service validation"
    );
    assert!(context.state.update_peer_path_usage(
        UnderlayProtocol::Tcp,
        0,
        tcp_g1,
        5,
        PathUsage::Available,
    ));
    let restored_authority = context
        .current_request_tcp_service_carrier(tcp_key)
        .expect("restored AVAILABLE authority");
    assert!(restored_authority.eligibility_generation > available_authority.eligibility_generation);

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
