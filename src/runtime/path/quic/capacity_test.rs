use super::*;
use crate::config::{ResourceLimits, SecurityConfig, SharedSecret};
use crate::model::capacity::{
    MIN_RATE_SAMPLE_BYTES, PATH_OPEN_SCORE_BYTES, reliable_capacity_calibration_session_limit_bytes,
};
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::protocol::{StreamId, UnderlayProtocol};
use crate::runtime::path::{
    CapacityProbeCommandTicket, ClientPathContext, ClientPathHealthRecord,
    RequestCapacityProbeCampaignBudget,
};
use crate::transport::PathSpec;
use std::sync::Arc;
use std::time::{Duration, Instant};

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

fn poison_request_quic_capacity_health(context: &ClientPathContext) {
    let poisoned = context.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poisoned.health().lock().expect("path health lock");
            panic!("poison path health for no-lock product ACK assertion");
        })
        .join()
        .is_err()
    );
    assert!(context.health().is_poisoned());
}

#[test]
fn product_ack_without_quic_transaction_skips_health_lock() {
    let context = request_quic_capacity_test_context(1);
    poison_request_quic_capacity_health(&context);
    let now = Instant::now();

    context.record_relay_path_product_ack(
        StreamId(209),
        udp_path_instance(0, 1),
        PATH_OPEN_SCORE_BYTES,
        now,
        now + Duration::from_millis(1),
    );
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
    record.quic_capacity.proof = Some(RequestQuicCapacityProof {
        candidate,
        rate_bps: 117_000_000,
        rate_sample_bytes: required_product_sample_bytes,
    });
    record.quic_capacity.handoff = Some(RequestQuicCapacityProductHandoff {
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
    record.quic_capacity.record_product_ack(
        StreamId(72),
        path_instance,
        required as usize,
        accepted_at,
        before_expiry,
    );
    record.quic_capacity.record_product_ack(
        stream_id,
        udp_path_instance(2, 18),
        required as usize,
        accepted_at,
        before_expiry,
    );
    record.quic_capacity.record_product_ack(
        stream_id,
        path_instance,
        required as usize,
        accepted_at - Duration::from_nanos(1),
        before_expiry,
    );
    record.quic_capacity.record_product_ack(
        stream_id,
        path_instance,
        (required - final_fragment) as usize,
        accepted_at,
        before_expiry,
    );
    assert_eq!(
        record.quic_capacity.handoff_state(41),
        RequestQuicCapacityProductHandoffState::Pending
    );

    record.quic_capacity.record_product_ack(
        stream_id,
        path_instance,
        final_fragment as usize,
        accepted_at,
        before_expiry,
    );
    assert_eq!(
        record.quic_capacity.handoff_state(41),
        RequestQuicCapacityProductHandoffState::Complete
    );

    record.maintain(expires_at);
    let observation = record.observation_at(expires_at);
    assert!(!observation.explicit_carrier_capacity_proof);
    assert!(observation.quic_capacity_product_handoff_complete);
    assert!(observation.product_delivery_rate_bps.is_none());
    assert_eq!(observation.carrier_delivery_rate_bps, Some(117_000_000.0));
    assert_eq!(
        record.quic_capacity.handoff_state(41),
        RequestQuicCapacityProductHandoffState::Complete
    );

    record.mark_data_plane_failure(Instant::now(), false);
    assert_eq!(
        record.quic_capacity.handoff_state(41),
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
    record.quic_capacity.record_product_ack(
        stream_id,
        path_instance,
        (required - 1) as usize,
        accepted_at,
        expires_at - Duration::from_nanos(1),
    );
    // Expiry is an exclusive fence even when the final ACK races observation.
    record
        .quic_capacity
        .record_product_ack(stream_id, path_instance, 1, accepted_at, expires_at);
    assert_eq!(
        record.quic_capacity.handoff_state(42),
        RequestQuicCapacityProductHandoffState::Pending
    );

    record.maintain(expires_at);
    let observation = record.observation_at(expires_at);
    assert!(!observation.explicit_carrier_capacity_proof);
    assert!(!observation.quic_capacity_product_handoff_complete);
    assert_eq!(
        record.quic_capacity.handoff_state(42),
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
    record.quic_capacity.record_product_ack(
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
    record.quic_capacity.record_product_ack(
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
