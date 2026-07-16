use super::*;
use crate::config::{ResourceLimits, SecurityConfig, SharedSecret};
use crate::model::capacity::reliable_capacity_measurement_session_limit_bytes;
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::protocol::StreamId;
use crate::runtime::path::{
    CapacityProbeCommandTicket, ClientPathContext, ClientPathHealthRecord,
    RequestCapacityProbeCampaignBudget,
};
use crate::transport::PathSpec;
use std::sync::Arc;

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

fn request_tcp_capacity_test_context(path_count: usize) -> ClientPathContext {
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

fn reserve_request_tcp_capacity_for_test(
    context: &ClientPathContext,
    path_index: usize,
    token: u64,
    train_bytes: u64,
) -> Option<RequestTcpCapacityProbeLease> {
    reserve_request_tcp_capacity_with_limit_for_test(
        context,
        path_index,
        token,
        train_bytes,
        reliable_capacity_measurement_session_limit_bytes(context.mux_limits),
    )
}

fn reserve_request_tcp_capacity_with_limit_for_test(
    context: &ClientPathContext,
    path_index: usize,
    token: u64,
    train_bytes: u64,
    path_limit_bytes: u64,
) -> Option<RequestTcpCapacityProbeLease> {
    reserve_request_tcp_capacity_identity_with_limit_for_test(
        context,
        StreamId(70),
        path_index,
        100 + path_index as u64,
        token,
        train_bytes,
        path_limit_bytes,
    )
}

#[allow(clippy::too_many_arguments)]
fn reserve_request_tcp_capacity_identity_with_limit_for_test(
    context: &ClientPathContext,
    stream_id: StreamId,
    path_index: usize,
    instance_id: u64,
    token: u64,
    train_bytes: u64,
    path_limit_bytes: u64,
) -> Option<RequestTcpCapacityProbeLease> {
    reserve_request_tcp_capacity_identity_with_campaign_for_test(
        context,
        stream_id,
        path_index,
        instance_id,
        token,
        train_bytes,
        path_limit_bytes,
        Arc::new(RequestCapacityProbeCampaignBudget::default()),
    )
}

#[allow(clippy::too_many_arguments)]
fn reserve_request_tcp_capacity_identity_with_campaign_for_test(
    context: &ClientPathContext,
    stream_id: StreamId,
    path_index: usize,
    instance_id: u64,
    token: u64,
    train_bytes: u64,
    path_limit_bytes: u64,
    campaign: Arc<RequestCapacityProbeCampaignBudget>,
) -> Option<RequestTcpCapacityProbeLease> {
    let now = Instant::now();
    context.try_reserve_request_tcp_capacity_probe(
        stream_id,
        path_index,
        tcp_path_instance(path_index, instance_id),
        token,
        train_bytes,
        path_limit_bytes,
        campaign,
        PATH_OPEN_SCORE_BYTES as u64,
        now,
        now + Duration::from_secs(30),
        CapacityProbeCommandTicket::new(),
    )
}

#[test]
fn request_tcp_capacity_campaign_bounds_one_flow_without_spending_later_flow_credit() {
    const CANDIDATE_SHARE: u64 = 16 * 1024 * 1024;
    const ITER298_FIRST_TRAIN: u64 = 3_471_410;
    const ITER298_SECOND_TRAIN: u64 = 7_070_776;
    const ITER298_THIRD_TRAIN: u64 = 7_897_460;
    const HISTORICAL_CAMPAIGN: u64 = 16_193_904;

    let context = request_tcp_capacity_test_context(3);
    let first_flow = Arc::new(RequestCapacityProbeCampaignBudget::default());
    let first = reserve_request_tcp_capacity_identity_with_campaign_for_test(
        &context,
        StreamId(60),
        0,
        160,
        61,
        ITER298_FIRST_TRAIN,
        CANDIDATE_SHARE,
        first_flow.clone(),
    )
    .expect("the first measured Iter298 train fits the flow campaign");
    let second = reserve_request_tcp_capacity_identity_with_campaign_for_test(
        &context,
        StreamId(60),
        1,
        161,
        62,
        ITER298_SECOND_TRAIN,
        CANDIDATE_SHARE,
        first_flow.clone(),
    )
    .expect("the second measured Iter298 train fits the residual campaign");
    assert!(first.commit());
    assert!(second.commit());
    drop(first);
    drop(second);
    assert_eq!(
        first_flow.remaining_bytes(CANDIDATE_SHARE),
        CANDIDATE_SHARE - ITER298_FIRST_TRAIN - ITER298_SECOND_TRAIN
    );
    assert!(
        reserve_request_tcp_capacity_identity_with_campaign_for_test(
            &context,
            StreamId(60),
            2,
            162,
            63,
            ITER298_THIRD_TRAIN,
            CANDIDATE_SHARE,
            first_flow,
        )
        .is_none(),
        "the third Iter298 train must not overbook one flow's campaign"
    );

    let later_flow = Arc::new(RequestCapacityProbeCampaignBudget::default());
    let later = reserve_request_tcp_capacity_identity_with_campaign_for_test(
        &context,
        StreamId(61),
        2,
        262,
        64,
        ITER298_THIRD_TRAIN,
        CANDIDATE_SHARE,
        later_flow,
    )
    .expect("a later flow retains independent campaign credit");
    assert!(later.commit());
    drop(later);

    let historical = RequestCapacityProbeCampaignBudget::default();
    assert!(historical.try_reserve(HISTORICAL_CAMPAIGN, CANDIDATE_SHARE));
    assert_eq!(
        historical.remaining_bytes(CANDIDATE_SHARE),
        CANDIDATE_SHARE - HISTORICAL_CAMPAIGN,
        "the bounded policy must preserve the historical successful campaign"
    );
    let frozen_residual = CANDIDATE_SHARE - HISTORICAL_CAMPAIGN;
    assert!(
        !historical.try_reserve(frozen_residual + 1, 4 * CANDIDATE_SHARE),
        "a later topology proposal cannot expand the first frozen campaign share"
    );
}

#[test]
fn request_tcp_capacity_path_share_is_cumulative_and_cannot_expand() {
    let context = request_tcp_capacity_test_context(2);
    let session_limit = reliable_capacity_measurement_session_limit_bytes(context.mux_limits);
    let path_share = 8 * 1024 * 1024;
    let half_share = path_share / 2;

    let first =
        reserve_request_tcp_capacity_with_limit_for_test(&context, 0, 31, half_share, path_share)
            .expect("reserve first half of the path share");
    assert!(first.commit());
    drop(first);
    assert_eq!(
        context.request_tcp_capacity_probe_path_remaining_bytes(0, session_limit),
        half_share,
        "a larger later proposal must observe the frozen first share"
    );

    let later_flow_campaign = Arc::new(RequestCapacityProbeCampaignBudget::default());
    let second = reserve_request_tcp_capacity_identity_with_campaign_for_test(
        &context,
        StreamId(71),
        0,
        201,
        32,
        half_share,
        session_limit,
        later_flow_campaign.clone(),
    )
    .expect("a later stream may spend the residual fixed share");
    assert!(second.commit());
    drop(second);
    assert_eq!(
        later_flow_campaign.remaining_bytes(session_limit),
        half_share,
        "a later flow must freeze to the session's authoritative candidate share"
    );
    assert_eq!(
        context.request_tcp_capacity_probe_path_remaining_bytes(0, session_limit),
        0
    );
    let rejected_flow_campaign = Arc::new(RequestCapacityProbeCampaignBudget::default());
    assert!(
        reserve_request_tcp_capacity_identity_with_campaign_for_test(
            &context,
            StreamId(72),
            0,
            202,
            33,
            PATH_OPEN_SCORE_BYTES as u64,
            session_limit,
            rejected_flow_campaign.clone(),
        )
        .is_none(),
        "retries and path replacement cannot expand the frozen share"
    );
    assert_eq!(
        rejected_flow_campaign.remaining_bytes(session_limit),
        path_share,
        "a path-budget rejection must refund the flow reservation exactly"
    );

    let other = reserve_request_tcp_capacity_with_limit_for_test(
        &context,
        1,
        34,
        path_share,
        session_limit,
    )
    .expect("one exhausted path must not consume another path's share");
    assert!(other.commit());
    drop(other);
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        session_limit - 2 * path_share
    );
}

#[test]
fn request_tcp_capacity_reservations_are_path_parallel_and_session_bounded() {
    let context = request_tcp_capacity_test_context(3);
    let session_limit = reliable_capacity_measurement_session_limit_bytes(context.mux_limits);
    let first_bytes = session_limit / 2;
    let second_bytes = session_limit - first_bytes;

    let first = reserve_request_tcp_capacity_for_test(&context, 0, 1, first_bytes)
        .expect("reserve first exact TCP path");
    let remaining_after_first = context.request_tcp_capacity_probe_remaining_bytes();
    assert_eq!(remaining_after_first, second_bytes);
    assert!(
        reserve_request_tcp_capacity_for_test(&context, 0, 2, PATH_OPEN_SCORE_BYTES as u64)
            .is_none(),
        "one TCP path record must retain exact transaction ownership"
    );
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        remaining_after_first,
        "same-path rejection must not spend the shared envelope"
    );

    let second = reserve_request_tcp_capacity_for_test(&context, 1, 3, second_bytes)
        .expect("reserve a distinct TCP path concurrently");
    assert_eq!(context.request_tcp_capacity_probe_remaining_bytes(), 0);
    assert!(
        reserve_request_tcp_capacity_for_test(&context, 2, 4, PATH_OPEN_SCORE_BYTES as u64)
            .is_none(),
        "parallel reservations must not overbook the cumulative session envelope"
    );
    {
        let health = context.health().lock().expect("client path health lock");
        assert!(health.tcp[0].tcp_capacity.reservation.is_some());
        assert!(health.tcp[1].tcp_capacity.reservation.is_some());
        assert!(health.tcp[2].tcp_capacity.reservation.is_none());
    }

    assert!(first.commit());
    assert!(second.commit());
    drop(first);
    drop(second);
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        0,
        "committed carrier spend is cumulative and non-refilling"
    );
    let health = context.health().lock().expect("client path health lock");
    assert!(
        health
            .tcp
            .iter()
            .all(|record| record.tcp_capacity.reservation.is_none())
    );
}

#[test]
fn request_tcp_capacity_unwritten_refund_is_terminal_and_path_local() {
    let context = request_tcp_capacity_test_context(3);
    let session_limit = reliable_capacity_measurement_session_limit_bytes(context.mux_limits);
    let train_bytes = 1024 * 1024;
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        session_limit
    );
    let campaign = Arc::new(RequestCapacityProbeCampaignBudget::default());

    let reserve = |path_index, token| {
        reserve_request_tcp_capacity_identity_with_campaign_for_test(
            &context,
            StreamId(70),
            path_index,
            100 + path_index as u64,
            token,
            train_bytes,
            session_limit,
            campaign.clone(),
        )
    };
    let refund_before_commit = reserve(0, 11).expect("reserve refund-before-commit path");
    let refund_before_commit_clone = refund_before_commit.clone();
    let refund_after_commit = reserve(1, 12).expect("reserve refund-after-commit path");
    let committed = reserve(2, 13).expect("reserve committed path");
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        session_limit - 3 * train_bytes
    );
    assert_eq!(
        campaign.remaining_bytes(session_limit),
        session_limit - 3 * train_bytes
    );
    assert_eq!(
        context.request_tcp_capacity_probe_path_remaining_bytes(0, session_limit),
        session_limit - train_bytes
    );

    refund_before_commit.refund_if_unwritten();
    assert!(
        !refund_before_commit.commit(),
        "planner commit must not overwrite an earlier no-wire decision"
    );
    drop(refund_before_commit);
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        session_limit - 3 * train_bytes,
        "one live lease clone retains exact transaction ownership"
    );
    drop(refund_before_commit_clone);
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        session_limit - 2 * train_bytes
    );
    assert_eq!(
        context.request_tcp_capacity_probe_path_remaining_bytes(0, session_limit),
        session_limit,
        "an unwritten refund restores both aggregate and path-local budget"
    );
    assert_eq!(
        campaign.remaining_bytes(session_limit),
        session_limit - 2 * train_bytes,
        "the exact flow campaign refunds only after the last lease clone drops"
    );

    assert!(refund_after_commit.commit());
    refund_after_commit.refund_if_unwritten();
    assert!(
        !refund_after_commit.commit(),
        "a later planner call must not resurrect committed no-wire spend"
    );
    drop(refund_after_commit);
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        session_limit - train_bytes
    );
    assert_eq!(
        campaign.remaining_bytes(session_limit),
        session_limit - train_bytes
    );

    assert!(committed.commit());
    assert!(
        committed.commit(),
        "commit is idempotent before carrier write"
    );
    drop(committed);
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        session_limit - train_bytes,
        "written carrier spend remains charged after lease cleanup"
    );
    assert_eq!(
        context.request_tcp_capacity_probe_path_remaining_bytes(2, session_limit),
        session_limit - train_bytes
    );
    let health = context.health().lock().expect("client path health lock");
    assert!(
        health
            .tcp
            .iter()
            .all(|record| record.tcp_capacity.reservation.is_none())
    );
}

fn request_tcp_proof_metrics(path_index: usize) -> PathMetrics {
    PathMetrics {
        path_id: PathId(path_index as u16),
        underlay: UnderlayProtocol::Tcp,
        direction: PathMetricDirection::ClientToServer,
        metric_epoch: 1,
        metric_age_us: 0,
        srtt_us: 180_000,
        rttvar_us: 10_000,
        jitter_us: 10_000,
        delivery_rate_bps: 1_000_000_000,
        pacing_rate_bps: 2_000_000_000,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight: 64 * 1024,
        queue_bytes: 0,
        inflight_limit_bytes: 512 * 1024,
        inflight_hi_bytes: 512 * 1024,
        confidence_ppm: 1_000_000,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: 1,
        data_sample_bytes: 256 * 1024,
    }
}

#[test]
fn request_tcp_capacity_authority_expires_without_a_native_rate_prior() {
    let accepted_at = Instant::now();
    let expires_at = accepted_at + Duration::from_secs(2);
    let stream_id = StreamId(70);
    let path_index = 1;
    let path_instance = tcp_path_instance(path_index, 16);
    let train_bytes = 3 * 1024 * 1024;
    let rate_sample_bytes = 256 * 1024;
    let token = 40;
    let mut record = ClientPathHealthRecord::default();
    record.tcp_capacity.reserve(
        stream_id,
        path_instance,
        token,
        train_bytes,
        rate_sample_bytes,
        accepted_at,
        expires_at,
        CapacityProbeCommandTicket::new(),
    );
    let candidate = TcpCapacityProofCandidate {
        token,
        train_bytes,
        received_bytes: train_bytes,
        rate_sample_bytes,
        proof_elapsed: Duration::from_millis(25),
        receipt_rate_bps: 80_000_000,
        rate_bps: 80_000_000,
        accepted_at,
        expires_at,
    };

    assert!(!record.accept_request_tcp_capacity_proof(
        stream_id,
        path_instance,
        TcpCapacityProofCandidate {
            rate_bps: candidate.rate_bps + 1,
            ..candidate
        },
        request_tcp_proof_metrics(path_index),
        None,
        accepted_at,
    ));
    record.measured_srtt_ms = Some(25.0);
    record.carrier_inflight_limit_bytes = 512 * 1024;
    assert!(record.accept_request_tcp_capacity_proof(
        stream_id,
        path_instance,
        candidate,
        request_tcp_proof_metrics(path_index),
        None,
        accepted_at,
    ));
    assert_eq!(record.measured_srtt_ms, Some(25.0));
    assert_eq!(record.carrier_srtt_ms, None);
    assert_eq!(record.carrier_inflight_limit_bytes, 512 * 1024);
    let active = record.observation_at(expires_at - Duration::from_nanos(1));
    assert!(active.explicit_carrier_capacity_proof);
    assert_eq!(active.carrier_delivery_rate_bps, Some(80_000_000.0));
    assert_eq!(active.carrier_delivery_sample_bytes, rate_sample_bytes);

    let expired = record.observation_at(expires_at);
    assert!(!expired.explicit_carrier_capacity_proof);
    assert_eq!(expired.carrier_delivery_rate_bps, None);
    assert_eq!(expired.carrier_delivery_sample_bytes, 0);
    assert!(!expired.carrier_ack_derived_data_seen);
}

#[test]
fn portable_request_receipt_uses_exact_rate_and_configured_path_shape() {
    let mut baseline = portable_tcp_receipt_metrics(PathId(2), PathMetricDirection::ClientToServer);
    baseline.srtt_us = 40_000;
    baseline.inflight_limit_bytes = 32_000;
    baseline.inflight_hi_bytes = 24_000;
    baseline.app_limited = true;

    let metrics = request_tcp_capacity_receipt_metrics(
        PathId(2),
        2 * 1024 * 1024,
        120_000_000,
        Some(baseline),
        None,
    );

    assert_eq!(metrics.delivery_rate_bps, 120_000_000);
    assert_eq!(metrics.pacing_rate_bps, 120_000_000);
    assert_eq!(metrics.srtt_us, baseline.srtt_us);
    assert_eq!(metrics.inflight_limit_bytes, 0);
    assert_eq!(metrics.inflight_hi_bytes, 0);
    assert_eq!(metrics.app_limited, baseline.app_limited);
    assert!(metrics.has_ack_derived_data_sample);
    assert_eq!(metrics.data_sample_count, 1);
    assert_eq!(metrics.data_sample_bytes, 2 * 1024 * 1024);
    assert_eq!(metrics.confidence_ppm, 1_000_000);
}
