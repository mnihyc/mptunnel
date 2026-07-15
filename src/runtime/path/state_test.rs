use super::*;
use crate::config::SharedSecret;
use crate::model::capacity::QuicCapacityProofCandidate;
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

fn request_quic_capacity_test_context(path_count: usize) -> ClientPathContext {
    let paths = (0..path_count)
        .map(|index| {
            format!("udp://127.0.0.1:{}", 12_800 + index)
                .parse::<PathSpec>()
                .expect("request QUIC capacity test path")
        })
        .collect();
    let security = SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("request QUIC capacity test secret"),
    );
    ClientPathContext::new(paths, security, ResourceLimits::default())
        .expect("request QUIC capacity test context")
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
fn request_quic_capacity_refund_and_replacement_preserve_frozen_share() {
    let context = request_quic_capacity_test_context(1);
    let session_limit = reliable_capacity_calibration_session_limit_bytes(context.mux_limits);
    let path_share = 8 * 1024 * 1024;
    let provisional_bytes = 1024 * 1024;
    let now = Instant::now();
    let campaign = Arc::new(RequestCapacityProbeCampaignBudget::default());
    let provisional = context
        .try_reserve_request_quic_capacity_probe(
            StreamId(80),
            0,
            udp_path_instance(0, 300),
            41,
            provisional_bytes,
            path_share,
            campaign.clone(),
            now,
            now + Duration::from_secs(1),
            Duration::from_secs(1),
            CapacityProbeCommandTicket::new(),
        )
        .expect("reserve provisional QUIC path spend");
    assert_eq!(
        campaign.remaining_bytes(path_share),
        path_share - provisional_bytes
    );
    assert_eq!(
        context.request_quic_capacity_probe_path_remaining_bytes(0, session_limit),
        path_share - provisional_bytes
    );
    drop(provisional);
    assert_eq!(
        context.request_quic_capacity_probe_remaining_bytes(),
        session_limit
    );
    assert_eq!(
        context.request_quic_capacity_probe_path_remaining_bytes(0, session_limit),
        path_share,
        "uncommitted QUIC cleanup refunds both counters but not the frozen share"
    );
    assert_eq!(campaign.remaining_bytes(session_limit), path_share);

    let mut replacement = context
        .try_reserve_request_quic_capacity_probe(
            StreamId(81),
            0,
            udp_path_instance(0, 301),
            42,
            path_share,
            session_limit,
            campaign.clone(),
            now,
            now + Duration::from_secs(1),
            Duration::from_secs(1),
            CapacityProbeCommandTicket::new(),
        )
        .expect("a replacement may consume only the original path share");
    replacement.commit();
    drop(replacement);
    assert_eq!(
        context.request_quic_capacity_probe_path_remaining_bytes(0, session_limit),
        0
    );
    assert_eq!(
        campaign.remaining_bytes(session_limit),
        0,
        "committed QUIC carrier spend remains charged to its flow"
    );
    assert!(
        context
            .try_reserve_request_quic_capacity_probe(
                StreamId(82),
                0,
                udp_path_instance(0, 302),
                43,
                PATH_OPEN_SCORE_BYTES as u64,
                session_limit,
                campaign,
                now,
                now + Duration::from_secs(1),
                Duration::from_secs(1),
                CapacityProbeCommandTicket::new(),
            )
            .is_none(),
        "flapping replacement cannot reopen the candidate share"
    );
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

fn proof_candidate(
    token: u64,
    accepted_at: Instant,
    expires_at: Instant,
    required_proof_bytes: u64,
) -> QuicCapacityProofCandidate {
    QuicCapacityProofCandidate {
        token,
        train_bytes: 16 * 1024 * 1024,
        sample_floor_bytes: required_proof_bytes + PATH_OPEN_SCORE_BYTES as u64,
        accounting_slack_bytes: PATH_OPEN_SCORE_BYTES as u64,
        warmup_bytes: 15 * 1024 * 1024,
        required_proof_bytes,
        written_bytes: 16 * 1024 * 1024,
        written_data_frame_count: 16,
        receipt_confirmed: true,
        received_bytes: 16 * 1024 * 1024,
        proof_elapsed: Duration::from_millis(900),
        rate_bps: 117_000_000,
        accepted_at,
        expires_at,
        proof_validity: expires_at.saturating_duration_since(accepted_at),
    }
}

fn install_handoff(
    record: &mut ClientPathHealthRecord,
    stream_id: StreamId,
    path_instance: RelayPathInstance,
    token: u64,
    accepted_at: Instant,
    expires_at: Instant,
    required_product_sample_bytes: u64,
) {
    let candidate = proof_candidate(
        token,
        accepted_at,
        expires_at,
        required_product_sample_bytes,
    );
    record.quic_capacity_proof = Some(RequestQuicCapacityProof {
        candidate,
        rate_bps: 117_000_000,
        rate_sample_bytes: required_product_sample_bytes,
    });
    record.request_quic_capacity_product_handoff = Some(RequestQuicCapacityProductHandoff {
        stream_id,
        path_instance,
        token,
        acked_product_bytes: 0,
        required_product_sample_bytes,
        rate_bps: 117_000_000,
        rate_sample_bytes: required_product_sample_bytes,
        accepted_at,
        expires_at,
        complete: false,
        completed_at: None,
        rate_prior_expires_at: None,
    });
}

fn udp_path_instance(index: usize, id: u64) -> RelayPathInstance {
    RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index,
        },
        id,
    }
}

#[test]
fn exact_post_proof_product_floor_survives_carrier_proof_expiry() {
    let accepted_at = Instant::now();
    let expires_at = accepted_at + Duration::from_secs(2);
    let required = 247_544;
    let final_fragment = MIN_RATE_SAMPLE_BYTES;
    let stream_id = StreamId(71);
    let path_instance = udp_path_instance(2, 17);
    let mut record = ClientPathHealthRecord::default();
    install_handoff(
        &mut record,
        stream_id,
        path_instance,
        41,
        accepted_at,
        expires_at,
        required,
    );

    let before_expiry = expires_at - Duration::from_nanos(1);
    record.record_request_quic_capacity_product_ack(
        StreamId(72),
        path_instance,
        required as usize,
        accepted_at,
        before_expiry,
    );
    record.record_request_quic_capacity_product_ack(
        stream_id,
        udp_path_instance(2, 18),
        required as usize,
        accepted_at,
        before_expiry,
    );
    record.record_request_quic_capacity_product_ack(
        stream_id,
        path_instance,
        required as usize,
        accepted_at - Duration::from_nanos(1),
        before_expiry,
    );
    record.record_request_quic_capacity_product_ack(
        stream_id,
        path_instance,
        (required - final_fragment) as usize,
        accepted_at,
        before_expiry,
    );
    assert_eq!(
        record.request_quic_capacity_product_handoff_state(41),
        RequestQuicCapacityProductHandoffState::Pending
    );

    record.record_request_quic_capacity_product_ack(
        stream_id,
        path_instance,
        final_fragment as usize,
        accepted_at,
        before_expiry,
    );
    assert_eq!(
        record.request_quic_capacity_product_handoff_state(41),
        RequestQuicCapacityProductHandoffState::Complete
    );

    record.maintain(expires_at);
    let observation = record.observation_at(expires_at);
    assert!(!observation.explicit_carrier_capacity_proof);
    assert!(observation.quic_capacity_product_handoff_complete);
    assert!(observation.product_delivery_rate_bps.is_none());
    assert_eq!(observation.carrier_delivery_rate_bps, Some(117_000_000.0));
    assert_eq!(
        record.request_quic_capacity_product_handoff_state(41),
        RequestQuicCapacityProductHandoffState::Complete
    );

    record.mark_data_plane_failure(Instant::now(), false);
    assert_eq!(
        record.request_quic_capacity_product_handoff_state(41),
        RequestQuicCapacityProductHandoffState::Absent
    );
}

#[test]
fn incomplete_product_handoff_expires_with_its_carrier_proof() {
    let accepted_at = Instant::now();
    let expires_at = accepted_at + Duration::from_secs(2);
    let required = 247_544;
    let stream_id = StreamId(72);
    let path_instance = udp_path_instance(3, 19);
    let mut record = ClientPathHealthRecord::default();
    install_handoff(
        &mut record,
        stream_id,
        path_instance,
        42,
        accepted_at,
        expires_at,
        required,
    );
    record.record_request_quic_capacity_product_ack(
        stream_id,
        path_instance,
        (required - 1) as usize,
        accepted_at,
        expires_at - Duration::from_nanos(1),
    );
    // Expiry is an exclusive fence even when the final ACK races observation.
    record.record_request_quic_capacity_product_ack(
        stream_id,
        path_instance,
        1,
        accepted_at,
        expires_at,
    );
    assert_eq!(
        record.request_quic_capacity_product_handoff_state(42),
        RequestQuicCapacityProductHandoffState::Pending
    );

    record.maintain(expires_at);
    let observation = record.observation_at(expires_at);
    assert!(!observation.explicit_carrier_capacity_proof);
    assert!(!observation.quic_capacity_product_handoff_complete);
    assert_eq!(
        record.request_quic_capacity_product_handoff_state(42),
        RequestQuicCapacityProductHandoffState::Absent
    );
}

#[test]
fn completed_handoff_yields_at_the_durable_native_window_floor() {
    let accepted_at = Instant::now();
    let expires_at = accepted_at + Duration::from_secs(2);
    let required = 247_544;
    let stream_id = StreamId(73);
    let path_instance = udp_path_instance(4, 20);
    let mut record = ClientPathHealthRecord::default();
    install_handoff(
        &mut record,
        stream_id,
        path_instance,
        43,
        accepted_at,
        expires_at,
        required,
    );
    record.record_request_quic_capacity_product_ack(
        stream_id,
        path_instance,
        required as usize,
        accepted_at,
        expires_at - Duration::from_nanos(1),
    );

    let native_window = 4 * 1024 * 1024;
    let native_rate_bps = 400_000_000.0;
    record.carrier_delivery_rate_bps = Some(native_rate_bps);
    record.carrier_delivery_samples = 1;
    record.carrier_ack_derived_data_seen = true;
    record.carrier_app_limited = false;
    record.carrier_inflight_limit_bytes = native_window;
    record.carrier_delivery_sample_bytes = native_window - 1;

    let below_floor = record.observation_at(expires_at);
    assert!(below_floor.quic_capacity_product_handoff_complete);
    assert_eq!(below_floor.carrier_delivery_rate_bps, Some(117_000_000.0));

    record.carrier_delivery_sample_bytes = native_window;
    let at_floor = record.observation_at(expires_at);
    assert!(at_floor.quic_capacity_product_handoff_complete);
    assert_eq!(at_floor.carrier_delivery_rate_bps, Some(native_rate_bps));
}

#[test]
fn completed_handoff_rate_prior_expires_without_erasing_product_progress() {
    let accepted_at = Instant::now();
    let expires_at = accepted_at + Duration::from_secs(2);
    let completed_at = accepted_at + Duration::from_secs(1);
    let required = 247_544;
    let stream_id = StreamId(74);
    let path_instance = udp_path_instance(5, 21);
    let mut record = ClientPathHealthRecord::default();
    install_handoff(
        &mut record,
        stream_id,
        path_instance,
        44,
        accepted_at,
        expires_at,
        required,
    );
    record.record_request_quic_capacity_product_ack(
        stream_id,
        path_instance,
        required as usize,
        accepted_at,
        completed_at,
    );
    let native_window = 4 * 1024 * 1024;
    let corrected_native_rate = 50_000_000.0;
    record.carrier_delivery_rate_bps = Some(corrected_native_rate);
    record.carrier_delivery_samples = 1;
    record.carrier_ack_derived_data_seen = true;
    record.carrier_app_limited = false;
    record.carrier_inflight_limit_bytes = native_window;
    record.carrier_delivery_sample_bytes = native_window - 1;

    let proof_expired = record.observation_at(expires_at);
    assert!(proof_expired.quic_capacity_product_handoff_complete);
    assert!(proof_expired.quic_capacity_rate_prior_fresh);
    assert_eq!(proof_expired.carrier_delivery_rate_bps, Some(117_000_000.0));

    let prior_expires_at = completed_at + Duration::from_secs(2);
    let prior_expired = record.observation_at(prior_expires_at);
    assert!(prior_expired.quic_capacity_product_handoff_complete);
    assert!(!prior_expired.quic_capacity_rate_prior_fresh);
    assert_eq!(
        prior_expired.carrier_delivery_rate_bps,
        Some(corrected_native_rate)
    );
}
