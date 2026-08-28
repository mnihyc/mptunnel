use super::*;
use crate::model::path::next_carrier_path_instance_id;
use crate::runtime::path::{ClientPathHealth, ClientPathHealthRecord};

fn remote_metrics() -> PathMetrics {
    PathMetrics {
        path_id: PathId(0),
        underlay: UnderlayProtocol::Udp,
        direction: crate::protocol::PathMetricDirection::ServerToClient,
        metric_epoch: 1,
        metric_age_us: 0,
        rate_valid_for_us: 1_000_000,
        rate_observed: true,
        srtt_us: 20_000,
        rttvar_us: 2_000,
        jitter_us: 2_000,
        delivery_rate_bps: 80_000_000,
        pacing_rate_bps: 100_000_000,
        pacing_rate_observed: true,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight_observed: true,
        queue_observed: true,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: 512 * 1024,
        inflight_hi_bytes: 512 * 1024,
        confidence_ppm: 1_000_000,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: 8,
        data_sample_bytes: 512 * 1024,
    }
}

#[test]
fn remote_path_metrics_rate_expires_at_advertised_remaining_budget_boundary() {
    let metrics = remote_metrics();

    let received_at = Instant::now();
    let near_boundary = remote_path_metrics_observation_at(
        PathMetrics {
            metric_age_us: u32::MAX,
            rate_valid_for_us: 1,
            ..metrics
        },
        received_at,
    );
    assert!(near_boundary.rate_sample.is_some());
    assert_eq!(
        near_boundary.rate_sample_expires_at,
        Some(received_at + Duration::from_micros(1))
    );
    let mut health = ClientPathHealthRecord::default();
    health.mark_udp_datagram_feedback_at(near_boundary, received_at);
    assert!(
        health
            .observation_at(received_at + Duration::from_nanos(999))
            .measured_rate_bps
            .is_some()
    );
    assert_eq!(
        health
            .observation_at(received_at + Duration::from_micros(1))
            .measured_rate_bps,
        None,
        "receipt must preserve only the source sample's remaining lifetime"
    );
    assert!(
        remote_path_metrics_observation_at(
            PathMetrics {
                metric_age_us: 0,
                rate_valid_for_us: 0,
                ..metrics
            },
            received_at,
        )
        .rate_sample
        .is_none(),
        "an incoming stale advisory rate cannot be reborn as a fresh local one-second sample"
    );
}

#[test]
fn remote_path_metrics_missing_loss_is_unknown_not_observed_zero() {
    let metrics = remote_metrics();
    assert_eq!(remote_path_metrics_observation(metrics).loss_rate, None);
    assert_eq!(
        remote_path_metrics_observation(PathMetrics {
            loss_observed: true,
            loss_ppm: 0,
            ..metrics
        })
        .loss_rate,
        Some(0.0),
        "canonical observed zero remains distinct from absent loss"
    );
}

#[test]
fn remote_ack_reachability_without_sample_volume_is_not_rate_capacity() {
    let metrics = remote_metrics();
    let observation = remote_path_metrics_observation(PathMetrics {
        data_sample_count: 0,
        data_sample_bytes: 0,
        ..metrics
    });
    assert_eq!(observation.rtt, Duration::from_millis(20));
    assert!(observation.rate_sample.is_none());
    assert_eq!(observation.rate_sample_expires_at, None);
}

#[test]
fn quic_datagram_status_uses_the_spawning_carrier_instance_fence() {
    let state = ClientPathState::new(ClientPathHealth::new(
        Vec::new(),
        vec![ClientPathHealthRecord::default()],
    ));
    let stale_instance_id = next_carrier_path_instance_id();
    let current_instance_id = next_carrier_path_instance_id();
    state.install_peer_path_usage(
        UnderlayProtocol::Udp,
        0,
        current_instance_id,
        0,
        PathUsage::Available,
    );

    assert!(
        !apply_client_udp_datagram_path_status(
            &state,
            0,
            stale_instance_id,
            PathId(0),
            PathId(0),
            99,
            PathUsage::Backup,
        )
        .expect("matching wire path ID")
    );
    assert!(
        apply_client_udp_datagram_path_status(
            &state,
            0,
            current_instance_id,
            PathId(0),
            PathId(0),
            1,
            PathUsage::Backup,
        )
        .expect("matching wire path ID")
    );
    assert!(
        !apply_client_udp_datagram_path_status(
            &state,
            0,
            current_instance_id,
            PathId(0),
            PathId(0),
            1,
            PathUsage::Available,
        )
        .expect("matching wire path ID")
    );
    assert_eq!(
        state.peer_path_usage(UnderlayProtocol::Udp, 0),
        Some(PathUsage::Backup)
    );
}

#[test]
fn quic_datagram_status_rejects_a_different_wire_path_id() {
    let state = ClientPathState::new(ClientPathHealth::new(
        Vec::new(),
        vec![ClientPathHealthRecord::default()],
    ));
    let path_instance_id = next_carrier_path_instance_id();
    state.install_peer_path_usage(
        UnderlayProtocol::Udp,
        0,
        path_instance_id,
        0,
        PathUsage::Available,
    );

    assert!(
        apply_client_udp_datagram_path_status(
            &state,
            0,
            path_instance_id,
            PathId(0),
            PathId(1),
            1,
            PathUsage::Backup,
        )
        .is_err()
    );
}
