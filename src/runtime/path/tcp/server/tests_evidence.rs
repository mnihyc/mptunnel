use super::{ServerTcpEvidenceState, merge_local_tcp_metrics, project_tcp_delivery_rate_sample};
use crate::config::{
    DEFAULT_OUTBOUND_CONNECT_TIMEOUT, MppPerformanceConfig, ResourceLimits, ServerSecurityConfig,
    SharedSecret,
};
use crate::model::capacity::RELIABLE_INITIAL_WINDOW_PACKETS;
use crate::model::timing::transport_rate_sample_freshness_horizon;
use crate::mux::MuxLimits;
use crate::outbound::OutboundConfig;
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::node::server::{ServerIdentityRuntime, new_identity_runtime};
use crate::runtime::path::proof::allocated_path_proof_data_frame;
use crate::runtime::path::tcp::metrics::TcpSenderMetricTracker;
use crate::runtime::path::{CarrierDeliveryRateSample, ServerLocalPathProperties};
use crate::transport::tcp_telemetry::{TcpNativeFlight, TcpNativeRtt, TcpNativeSnapshot};
use std::time::{Duration, Instant};

fn baseline_metrics() -> PathMetrics {
    PathMetrics {
        path_id: PathId(2),
        underlay: UnderlayProtocol::Tcp,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: 1,
        metric_age_us: 9_000,
        rate_valid_for_us: 0,
        rate_observed: false,
        srtt_us: 180_000,
        rttvar_us: 90_000,
        jitter_us: 90_000,
        delivery_rate_bps: 25_000_000,
        pacing_rate_bps: 25_000_000,
        pacing_rate_observed: false,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight_observed: false,
        queue_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: 0,
        inflight_hi_bytes: 0,
        confidence_ppm: 0,
        app_limited: true,
        has_ack_derived_data_sample: false,
        data_sample_count: 0,
        data_sample_bytes: 0,
    }
}

#[test]
fn partial_native_shape_updates_credit_without_fabricating_delivery_evidence() {
    let snapshot = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 25_000,
            rttvar_us: None,
        }),
        flight: Some(TcpNativeFlight {
            bytes_in_flight: Some(64 * 1024),
            inflight_limit_bytes: 512 * 1024,
            inflight_hi_bytes: Some(512 * 1024),
        }),
        ..TcpNativeSnapshot::default()
    };
    let observation = TcpSenderMetricTracker::new(snapshot).observe(
        PathId(2),
        PathMetricDirection::ServerToClient,
        snapshot,
    );

    let mut previous = baseline_metrics();
    previous.loss_ppm = 12_345;
    previous.loss_observed = true;
    let metrics = merge_local_tcp_metrics(Some(previous), observation)
        .expect("partial native transport shape");
    assert_eq!(metrics.srtt_us, 25_000);
    assert_eq!(metrics.rttvar_us, 90_000, "unknown variance stays unknown");
    assert_eq!(metrics.bytes_in_flight, 64 * 1024);
    assert_eq!(metrics.inflight_limit_bytes, 512 * 1024);
    assert_eq!(metrics.delivery_rate_bps, 25_000_000);
    assert_eq!(metrics.loss_ppm, 12_345, "raw prior stays diagnostic");
    assert!(
        !metrics.loss_observed,
        "missing current loss loses authority"
    );
    assert!(!metrics.has_ack_derived_data_sample);
    assert_eq!(metrics.data_sample_count, 0);
    assert_eq!(metrics.metric_age_us, 0);
}

#[test]
fn partial_rate_without_transport_shape_does_not_refresh_the_registry() {
    let snapshot = TcpNativeSnapshot {
        pacing_rate_bytes_per_second: Some(1_000_000),
        ..TcpNativeSnapshot::default()
    };
    let observation = TcpSenderMetricTracker::new(snapshot).observe(
        PathId(2),
        PathMetricDirection::ServerToClient,
        snapshot,
    );

    assert_eq!(
        merge_local_tcp_metrics(Some(baseline_metrics()), observation),
        None
    );
}

#[test]
fn server_retains_qualified_native_delivery_across_later_app_limited_polls() {
    let baseline = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 20_000,
            rttvar_us: Some(2_000),
        }),
        flight: Some(TcpNativeFlight {
            bytes_in_flight: Some(0),
            inflight_limit_bytes: 64 * 1024,
            inflight_hi_bytes: Some(64 * 1024),
        }),
        notsent_bytes: Some(0),
        bytes_acked: Some(1_000),
        delivery_rate_bytes_per_second: Some(10_000_000),
        pacing_rate_bytes_per_second: Some(20_000_000),
        app_limited: Some(true),
        ..TcpNativeSnapshot::default()
    };
    let mut tracker = TcpSenderMetricTracker::new(baseline);
    let qualified = tracker.observe(
        PathId(2),
        PathMetricDirection::ServerToClient,
        TcpNativeSnapshot {
            bytes_acked: Some(101_000),
            app_limited: Some(false),
            ..baseline
        },
    );
    let mut state = ServerTcpEvidenceState::new(None, None, MuxLimits::default());
    let observed_at = Instant::now();
    state.observe_delivery_rate_sample_at(qualified, observed_at);
    let retained = state
        .delivery_rate_sample
        .expect("qualified native delivery sample");
    assert_eq!(retained.delivery_rate_bps, 80_000_000);
    assert_eq!(retained.pacing_rate_bps, Some(160_000_000));
    assert_eq!(retained.sample_bytes, 100_000);
    assert!(retained.delivery_window_covered);
    let frozen_horizon = transport_rate_sample_freshness_horizon(
        Duration::from_millis(20),
        Duration::from_millis(2),
    );
    assert_eq!(retained.observed_at, observed_at);
    assert_eq!(retained.expires_at, observed_at + frozen_horizon);

    let idle = tracker.observe(
        PathId(2),
        PathMetricDirection::ServerToClient,
        TcpNativeSnapshot {
            bytes_acked: Some(102_000),
            delivery_rate_bytes_per_second: Some(5_000_000),
            app_limited: Some(true),
            ..baseline
        },
    );
    state.observe_delivery_rate_sample_at(idle, observed_at + Duration::from_millis(1));

    assert_eq!(state.delivery_rate_sample, Some(retained));

    state.observe_delivery_rate_sample_at(qualified, retained.expires_at - Duration::from_nanos(1));
    let accumulated = state.delivery_rate_sample.expect("in-horizon accumulation");
    assert_eq!(accumulated.sample_count, 2);
    assert_eq!(accumulated.sample_bytes, 200_000);

    state.observe_delivery_rate_sample_at(qualified, accumulated.expires_at);
    let reset = state.delivery_rate_sample.expect("new post-expiry epoch");
    assert_eq!(reset.sample_count, 1);
    assert_eq!(reset.sample_bytes, 100_000);
    assert_eq!(reset.observed_at, accumulated.expires_at);
}

#[test]
fn retained_tcp_sidecar_projects_one_immutable_rate_epoch_through_idle_shape_refreshes() {
    let observed_at = Instant::now();
    let sample = CarrierDeliveryRateSample {
        delivery_rate_bps: 80_000_000,
        pacing_rate_bps: Some(100_000_000),
        sample_count: 1,
        sample_bytes: 512 * 1024,
        delivery_window_covered: true,
        observed_at,
        expires_at: observed_at + Duration::from_millis(300),
    };
    let mut metrics = baseline_metrics();
    metrics.delivery_rate_bps = 900_000_000;
    metrics.pacing_rate_bps = 1_000_000_000;
    metrics.confidence_ppm = 1_000_000;
    metrics.app_limited = true;

    let projected_at = observed_at + Duration::from_millis(123);
    project_tcp_delivery_rate_sample(&mut metrics, sample, projected_at);

    assert_eq!(metrics.delivery_rate_bps, 80_000_000);
    assert_eq!(metrics.pacing_rate_bps, 100_000_000);
    assert!(metrics.rate_observed);
    assert!(metrics.pacing_rate_observed);
    assert_eq!(metrics.metric_age_us, 123_000);
    assert_eq!(metrics.rate_valid_for_us, 177_000);
    assert_eq!(
        metrics.confidence_ppm,
        1_000_000 / RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        "one ACK in a new epoch cannot inherit cumulative socket confidence"
    );
    assert!(!metrics.app_limited);

    for later_rtt in [1_000, 5_000_000] {
        metrics.srtt_us = later_rtt;
        metrics.rttvar_us = later_rtt / 2;
        project_tcp_delivery_rate_sample(
            &mut metrics,
            sample,
            observed_at + Duration::from_millis(250),
        );
        assert_eq!(metrics.metric_age_us, 250_000);
        assert_eq!(metrics.rate_valid_for_us, 50_000);
        assert_eq!(metrics.delivery_rate_bps, 80_000_000);
        assert_eq!(sample.expires_at, observed_at + Duration::from_millis(300));
    }

    project_tcp_delivery_rate_sample(&mut metrics, sample, sample.expires_at);
    assert_eq!(metrics.rate_valid_for_us, 0);
    assert!(metrics.rate_observed);
    assert!(metrics.pacing_rate_observed);
    assert_eq!(metrics.pacing_rate_bps, 100_000_000);

    let overlong = CarrierDeliveryRateSample {
        expires_at: observed_at
            + Duration::from_micros(crate::protocol::PATH_METRICS_MAX_RATE_VALID_FOR_US + 1),
        ..sample
    };
    project_tcp_delivery_rate_sample(&mut metrics, overlong, observed_at);
    assert_eq!(
        metrics.rate_valid_for_us,
        crate::protocol::PATH_METRICS_MAX_RATE_VALID_FOR_US
    );
}

#[test]
fn path_proof_ack_validates_without_overwriting_native_metrics() {
    let security = ServerSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("test shared secret"),
    );
    let ServerIdentityRuntime {
        paths: context,
        reliable_relay: _,
    } = new_identity_runtime(
        Vec::new(),
        OutboundConfig::Direct,
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        security,
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
    );
    let session_id = crate::protocol::SessionId(41);
    let path_id = PathId(2);
    let registration = context.reliable_streams.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        path_id,
        ServerLocalPathProperties::default(),
    );
    let native = baseline_metrics();
    context
        .reliable_streams
        .record_local_path_metrics(&registration, native, true);
    let before = context
        .reliable_streams
        .management_snapshot()
        .paths
        .into_iter()
        .find(|path| path.path_instance_id == registration.path_instance_id())
        .and_then(|path| path.metrics)
        .expect("native metrics before validation");

    let mut evidence = ServerTcpEvidenceState::new(None, None, context.mux_limits);
    let (proof_id, proof) = allocated_path_proof_data_frame(path_id, context.mux_limits);
    let payload_bytes = match &proof {
        crate::protocol::Frame::PathProofData { payload, .. } => payload.len(),
        _ => unreachable!("path proof allocator returned another frame"),
    };
    evidence.record_sent_frame(&proof);
    evidence.handle_path_proof_ack(
        &context,
        &registration,
        path_id,
        proof_id,
        u32::try_from(payload_bytes).expect("test proof size"),
    );

    let after = context
        .reliable_streams
        .management_snapshot()
        .paths
        .into_iter()
        .find(|path| path.path_instance_id == registration.path_instance_id())
        .and_then(|path| path.metrics)
        .expect("native metrics after validation");
    assert_eq!(
        PathMetrics {
            metric_age_us: 0,
            ..after
        },
        PathMetrics {
            metric_age_us: 0,
            ..before
        },
        "path proof must not publish capacity, flight, queue, or congestion state",
    );
    assert!(
        registration
            .path_validation_challenge(context.mux_limits)
            .is_none(),
        "a successful ACK completes the path proof",
    );
}
