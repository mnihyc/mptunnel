use super::*;
use crate::config::SharedSecret;
use crate::runtime::path::tcp::metrics::{TcpNativeObservation, TcpSenderMetricTracker};
use crate::transport::tcp_telemetry::{
    TcpNativeFlight, TcpNativeLossCounters, TcpNativeRtt, TcpNativeSnapshot,
};

fn tcp_path_instance(index: usize, id: u64) -> RelayPathInstance {
    RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index,
        },
        id,
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
    let security = SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("request TCP capacity test secret"),
    );
    ClientPathContext::new(paths, security, ResourceLimits::default())
        .expect("request TCP capacity test context")
}

#[test]
fn stale_shared_load_snapshot_has_only_one_claim_winner() {
    let context = tcp_path_test_context(1);
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };

    let first = context
        .try_reserve_relay_path_load_if_unchanged(key, FlowLane::Throughput, 0, 0)
        .expect("first exact snapshot claim");
    assert!(
        context
            .try_reserve_relay_path_load_if_unchanged(key, FlowLane::Throughput, 0, 0)
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
        .reserve_relay_path_load(key, FlowLane::Throughput)
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
        .reserve_relay_path_load(key, FlowLane::Throughput)
        .expect("path load lease");
    context.change_relay_path_lane_load(
        UnderlayProtocol::Tcp,
        0,
        FlowLane::Throughput,
        FlowLane::Latency,
    );
    lease.set_recorded_lane(FlowLane::Latency);

    drop(lease);
    let health = context.health().lock().expect("path health");
    assert_eq!(health.tcp[0].active_flows, 0);
    assert_eq!(health.tcp[0].active_latency_sensitive_flows, 0);
}

#[test]
fn request_bulk_flow_registration_counts_only_tcp_service_flows_once() {
    let paths = vec![
        "tcp://127.0.0.1:10079".parse().expect("TCP path"),
        "udp://127.0.0.1:10080".parse().expect("QUIC path"),
    ];
    let security = SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("registration test secret"),
    );
    let context = ClientPathContext::new(paths, security, ResourceLimits::default())
        .expect("registration test context");
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 0);

    let first = context.reliable_tcp_request_bulk_flow_registration();
    first.update(true, Some(UnderlayProtocol::Udp));
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 0);
    first.update(true, Some(UnderlayProtocol::Tcp));
    first.update(true, Some(UnderlayProtocol::Tcp));
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 1);

    {
        let second = context.reliable_tcp_request_bulk_flow_registration();
        second.update(true, Some(UnderlayProtocol::Udp));
        assert_eq!(context.active_tcp_service_request_bulk_flows(), 1);
        second.update(true, Some(UnderlayProtocol::Tcp));
        assert_eq!(context.active_tcp_service_request_bulk_flows(), 2);
        second.update(true, Some(UnderlayProtocol::Udp));
        assert_eq!(context.active_tcp_service_request_bulk_flows(), 1);
    }
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 1);

    let shared = first.clone();
    drop(first);
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 1);
    drop(shared);
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 0);
}

#[test]
fn request_capacity_budgets_share_policy_but_not_protocol_spend() {
    let paths = vec![
        "tcp://127.0.0.1:12810"
            .parse::<PathSpec>()
            .expect("TCP path"),
        "udp://127.0.0.1:12811"
            .parse::<PathSpec>()
            .expect("QUIC path"),
    ];
    let security = SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("mixed capacity test secret"),
    );
    let context = ClientPathContext::new(paths, security, ResourceLimits::default())
        .expect("mixed capacity test context");
    let session_limit = reliable_capacity_calibration_session_limit_bytes(context.mux_limits);
    let train_bytes = 1024 * 1024;
    let path_share = 8 * 1024 * 1024;
    let tcp_campaign = Arc::new(RequestCapacityProbeCampaignBudget::default());
    let quic_campaign = RequestCapacityProbeCampaignBudget::default();

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
        context.request_quic_capacity_probe_remaining_bytes(),
        session_limit,
        "TCP spend must not debit QUIC's native proof controller"
    );
    assert_eq!(
        context.request_quic_capacity_probe_path_remaining_bytes(0, path_share),
        path_share
    );
    assert_eq!(
        tcp_campaign.remaining_bytes(path_share),
        path_share - train_bytes
    );
    assert_eq!(
        quic_campaign.remaining_bytes(path_share),
        path_share,
        "TCP flow spend must not debit a QUIC flow campaign"
    );
}

fn request_tcp_native_observation(path_index: usize) -> TcpNativeObservation {
    let snapshot = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            min_rtt_us: Some(170_000),
            srtt_us: 180_000,
            rttvar_us: 10_000,
        }),
        flight: Some(TcpNativeFlight {
            snd_mss_bytes: 1_024,
            unacked_packets: 64,
            snd_ssthresh_packets: 512,
            snd_cwnd_packets: 512,
        }),
        notsent_bytes: Some(0),
        bytes_acked: Some(100),
        loss: Some(TcpNativeLossCounters {
            retransmits: 0,
            data_segments_out: 10,
        }),
        pacing_rate_bytes_per_second: Some(250_000_000),
        delivery_rate_bytes_per_second: Some(125_000_000),
        app_limited: Some(false),
    };
    TcpSenderMetricTracker::new(snapshot).observe(
        PathId(path_index as u16),
        PathMetricDirection::ClientToServer,
        snapshot,
    )
}

#[test]
fn tcp_transport_state_updates_native_rtt_without_rate_authority() {
    let mut record = ClientPathHealthRecord::default();
    let observation = request_tcp_native_observation(2);

    record.mark_tcp_transport_state(observation);

    assert_eq!(record.carrier_srtt_ms, Some(180.0));
    assert_eq!(record.carrier_rttvar_ms, Some(10.0));
    assert_eq!(record.carrier_bytes_in_flight, 64 * 1024);
    assert_eq!(record.carrier_inflight_limit_bytes, 512 * 1024);
    assert_eq!(record.carrier_delivery_rate_bps, None);
    assert_eq!(record.carrier_delivery_samples, 0);
    assert!(!record.carrier_ack_derived_data_seen);
}

#[test]
fn partial_tcp_transport_state_does_not_clear_unknown_fields() {
    let mut record = ClientPathHealthRecord::default();
    record.carrier_bytes_in_flight = 64 * 1024;
    record.carrier_inflight_limit_bytes = 512 * 1024;
    record.carrier_queue_bytes = 8 * 1024;
    let snapshot = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            min_rtt_us: None,
            srtt_us: 30_000,
            rttvar_us: 3_000,
        }),
        ..TcpNativeSnapshot::default()
    };
    let observation = TcpSenderMetricTracker::new(snapshot).observe(
        PathId(0),
        PathMetricDirection::ClientToServer,
        snapshot,
    );

    record.mark_tcp_transport_state(observation);

    assert_eq!(record.carrier_srtt_ms, Some(30.0));
    assert_eq!(record.carrier_bytes_in_flight, 64 * 1024);
    assert_eq!(record.carrier_inflight_limit_bytes, 512 * 1024);
    assert_eq!(record.carrier_queue_bytes, 8 * 1024);
}

#[test]
fn observation_projects_deadlines_without_applying_lifecycle_transitions() {
    let deadline = Instant::now();
    let mut record = ClientPathHealthRecord {
        state: SchedulerPathState::Failed,
        failed_until: Some(deadline),
        ..ClientPathHealthRecord::default()
    };

    let observation = record.observation_at(deadline);

    assert_eq!(observation.state, SchedulerPathState::Suspect);
    assert_eq!(record.state, SchedulerPathState::Failed);
    assert_eq!(record.failed_until, Some(deadline));

    record.maintain(deadline);
    assert_eq!(record.state, SchedulerPathState::Suspect);
    assert_eq!(record.failed_until, None);
}
