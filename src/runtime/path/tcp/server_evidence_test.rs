use super::merge_local_tcp_metrics;
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::path::tcp::metrics::TcpSenderMetricTracker;
use crate::transport::tcp_telemetry::{TcpNativeFlight, TcpNativeRtt, TcpNativeSnapshot};

fn baseline_metrics() -> PathMetrics {
    PathMetrics {
        path_id: PathId(2),
        underlay: UnderlayProtocol::Tcp,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: 1,
        metric_age_us: 9_000,
        srtt_us: 180_000,
        rttvar_us: 90_000,
        jitter_us: 90_000,
        delivery_rate_bps: 25_000_000,
        pacing_rate_bps: 25_000_000,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
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

    let metrics = merge_local_tcp_metrics(Some(baseline_metrics()), observation)
        .expect("partial native transport shape");
    assert_eq!(metrics.srtt_us, 25_000);
    assert_eq!(metrics.rttvar_us, 90_000, "unknown variance stays unknown");
    assert_eq!(metrics.bytes_in_flight, 64 * 1024);
    assert_eq!(metrics.inflight_limit_bytes, 512 * 1024);
    assert_eq!(metrics.delivery_rate_bps, 25_000_000);
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
