use super::*;
use crate::protocol::{PathId, PathMetricDirection};
use crate::runtime::path::tcp::metrics::{TcpNativeObservation, TcpSenderMetricTracker};
use crate::scheduler::PathState as SchedulerPathState;
use crate::transport::tcp_telemetry::{
    TcpNativeFlight, TcpNativeLossCounters, TcpNativeRtt, TcpNativeSnapshot,
};

fn request_tcp_native_observation(path_index: usize) -> TcpNativeObservation {
    let snapshot = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 180_000,
            rttvar_us: Some(10_000),
        }),
        flight: Some(TcpNativeFlight {
            bytes_in_flight: Some(64 * 1_024),
            inflight_limit_bytes: 512 * 1_024,
            inflight_hi_bytes: Some(512 * 1_024),
        }),
        notsent_bytes: Some(0),
        bytes_acked: Some(100),
        retransmission_counter: Some(0),
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

fn request_quic_path_metrics(now: Instant, deadline: Instant) -> UdpPathMetrics {
    UdpPathMetrics {
        direction: PathMetricDirection::ClientToServer,
        srtt: Duration::from_millis(180),
        rttvar: Duration::from_millis(20),
        rtt_observed: true,
        delivery_rate_bps: 200_000_000.0,
        pacing_rate_bps: 250_000_000.0,
        inflight_hi: 4 * 1024 * 1024,
        bytes_in_flight: 0,
        pending_bytes: 0,
        loss_ppm: Some(0),
        ecn_ppm: Some(0),
        app_limited: true,
        ack_derived_data_seen: true,
        delivery_sample_count: 10,
        delivery_sample_bytes: 512 * 1024,
        last_delivery_sample_at: Some(now),
        bulk_proof_expires_at: Some(deadline),
        latest_delivery_sample_bytes: 0,
        latest_delivery_sample_count: 0,
        latest_carrier_ack_elapsed: None,
        latest_rate_sample_elapsed: None,
        #[cfg(feature = "lab-diagnostics")]
        ack_poll: crate::runtime::path::quic::metrics::QuicAckPollDiagnostics::default(),
    }
}

#[test]
fn quic_bulk_proof_deadline_survives_a_later_app_limited_poll() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(2);
    let mut record = ClientPathHealthRecord::default();
    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(path_instance_id, 0, PathUsage::Available);
    record.mark_quic_path_metrics(path_instance_id, request_quic_path_metrics(now, deadline));

    let fresh = record.observation_at(now + Duration::from_secs(1));
    assert!(fresh.carrier_app_limited);
    assert_eq!(fresh.carrier_bulk_proof_expires_at, Some(deadline));
    assert_eq!(
        record
            .observation_at(deadline)
            .carrier_bulk_proof_expires_at,
        None
    );
}

#[test]
fn stale_quic_metrics_cannot_overwrite_a_reconnected_carrier() {
    let now = Instant::now();
    let mut record = ClientPathHealthRecord::default();
    let stale_instance = crate::model::path::next_carrier_path_instance_id();
    let live_instance = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(live_instance, 0, PathUsage::Available);

    record.mark_quic_path_metrics(
        stale_instance,
        request_quic_path_metrics(now, now + Duration::from_secs(2)),
    );
    assert_eq!(record.carrier_delivery_rate_bps, None);
    assert_eq!(record.carrier_bulk_proof_expires_at, None);
}

#[test]
fn terminal_carrier_failure_rejects_delayed_native_metrics() {
    let now = Instant::now();
    let mut record = ClientPathHealthRecord::default();
    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(path_instance_id, 0, PathUsage::Available);
    assert!(record.mark_data_plane_failure(path_instance_id, now, true));

    record.mark_quic_path_metrics(
        path_instance_id,
        request_quic_path_metrics(now, now + Duration::from_secs(2)),
    );
    assert!(!record.mark_tcp_transport_state(path_instance_id, request_tcp_native_observation(0),));

    assert_eq!(record.state, SchedulerPathState::Failed);
    assert_eq!(record.carrier_srtt_ms, None);
    assert_eq!(record.carrier_delivery_rate_bps, None);
}

#[test]
fn tcp_transport_state_updates_native_rtt_without_rate_authority() {
    let mut record = ClientPathHealthRecord::default();
    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(path_instance_id, 0, PathUsage::Available);
    let observation = request_tcp_native_observation(2);

    assert!(record.mark_tcp_transport_state(path_instance_id, observation));

    assert_eq!(record.carrier_srtt_ms, Some(180.0));
    assert_eq!(record.carrier_rttvar_ms, Some(10.0));
    assert_eq!(record.carrier_bytes_in_flight, 64 * 1024);
    assert_eq!(record.carrier_inflight_limit_bytes, 512 * 1024);
    assert!(record.native_drain_observed);
    assert_eq!(record.carrier_delivery_rate_bps, None);
    assert_eq!(record.carrier_delivery_samples, 0);
    assert!(!record.carrier_ack_derived_data_seen);
}

#[test]
fn tcp_transport_state_retains_non_app_limited_ack_window_without_data_ack_authority() {
    let baseline = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 180_000,
            rttvar_us: Some(10_000),
        }),
        flight: Some(TcpNativeFlight {
            bytes_in_flight: Some(128 * 1_024),
            inflight_limit_bytes: 512 * 1_024,
            inflight_hi_bytes: Some(512 * 1_024),
        }),
        notsent_bytes: Some(4_096),
        bytes_acked: Some(100),
        retransmission_counter: Some(0),
        loss: Some(TcpNativeLossCounters {
            retransmits: 0,
            data_segments_out: 10,
        }),
        pacing_rate_bytes_per_second: Some(31_250_000),
        delivery_rate_bytes_per_second: Some(25_000_000),
        app_limited: Some(false),
    };
    let current = TcpNativeSnapshot {
        bytes_acked: Some(100 + 1024 * 1024),
        ..baseline
    };
    let observation = TcpSenderMetricTracker::new(baseline).observe(
        PathId(0),
        PathMetricDirection::ClientToServer,
        current,
    );
    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.install_peer_usage(path_instance_id, 0, PathUsage::Available);

    assert!(record.mark_tcp_transport_state(path_instance_id, observation));

    assert_eq!(record.carrier_delivery_rate_bps, Some(200_000_000.0));
    assert_eq!(record.carrier_pacing_rate_bps, Some(250_000_000.0));
    assert_eq!(record.carrier_delivery_samples, 1);
    assert_eq!(record.carrier_delivery_sample_bytes, 1024 * 1024);
    assert!(record.carrier_delivery_window_covered);
    assert!(record.carrier_last_delivery_at.is_some());
    assert!(!record.carrier_app_limited);
    assert!(!record.carrier_ack_derived_data_seen);

    let app_limited_current = TcpNativeSnapshot {
        bytes_acked: current.bytes_acked.map(|bytes| bytes + 512 * 1024),
        delivery_rate_bytes_per_second: Some(1_000_000),
        pacing_rate_bytes_per_second: Some(2_000_000),
        app_limited: Some(true),
        ..current
    };
    let app_limited = TcpSenderMetricTracker::new(current).observe(
        PathId(0),
        PathMetricDirection::ClientToServer,
        app_limited_current,
    );
    assert!(record.mark_tcp_transport_state(path_instance_id, app_limited));
    assert_eq!(record.carrier_delivery_rate_bps, Some(200_000_000.0));
    assert_eq!(record.carrier_pacing_rate_bps, Some(250_000_000.0));
    assert_eq!(record.carrier_delivery_samples, 1);
    assert_eq!(record.carrier_delivery_sample_bytes, 1024 * 1024);
    assert!(!record.carrier_app_limited);
}

#[test]
fn replacement_tcp_carrier_rejects_stale_native_observation_and_clears_credit() {
    let original = crate::model::path::next_carrier_path_instance_id();
    let replacement = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.install_peer_usage(original, 0, PathUsage::Available);
    record.measured_srtt_ms = Some(25.0);
    record.measured_jitter_ms = Some(3.0);
    record.measured_rate_bps = Some(180_000_000.0);
    record.measured_loss_rate = Some(0.01);
    record.delivery_samples = 4;
    record.product_delivery_rate_bps = Some(175_000_000.0);
    record.product_delivery_sample_bytes = 2 * 1024 * 1024;
    record.datagram_feedback_samples = 2;
    record.last_delivery_at = Some(Instant::now());
    record.relay_bytes_in_flight = 64 * 1024;
    record.relay_queue_bytes = 32 * 1024;
    record.carrier_delivery_rate_bps = Some(200_000_000.0);
    record.carrier_pacing_rate_bps = Some(250_000_000.0);
    record.carrier_bytes_in_flight = 128 * 1024;
    record.carrier_inflight_limit_bytes = 512 * 1024;
    record.carrier_delivery_samples = 3;
    record.carrier_delivery_sample_bytes = 1024 * 1024;
    record.carrier_delivery_window_covered = true;
    record.carrier_last_delivery_at = Some(Instant::now());
    record.carrier_app_limited = false;
    record.active_flows = 2;
    record.active_latency_sensitive_flows = 1;
    record.install_peer_usage(replacement, 0, PathUsage::Available);

    assert_eq!(record.measured_srtt_ms, None);
    assert_eq!(record.measured_jitter_ms, None);
    assert_eq!(record.measured_rate_bps, None);
    assert_eq!(record.measured_loss_rate, None);
    assert_eq!(record.delivery_samples, 0);
    assert_eq!(record.product_delivery_rate_bps, None);
    assert_eq!(record.product_delivery_sample_bytes, 0);
    assert_eq!(record.datagram_feedback_samples, 0);
    assert_eq!(record.last_delivery_at, None);
    assert_eq!(record.relay_bytes_in_flight, 0);
    assert_eq!(record.relay_queue_bytes, 0);
    assert_eq!(record.carrier_delivery_rate_bps, None);
    assert_eq!(record.carrier_pacing_rate_bps, None);
    assert_eq!(record.carrier_bytes_in_flight, 0);
    assert_eq!(record.carrier_inflight_limit_bytes, 0);
    assert_eq!(record.carrier_delivery_samples, 0);
    assert!(!record.carrier_delivery_window_covered);
    assert_eq!(record.carrier_last_delivery_at, None);
    assert_eq!(record.active_flows, 2);
    assert_eq!(record.active_latency_sensitive_flows, 1);
    assert!(!record.mark_tcp_transport_state(original, request_tcp_native_observation(0)));
    assert_eq!(record.carrier_srtt_ms, None);

    record.begin_planned_retirement();
    assert_eq!(record.state, SchedulerPathState::Draining);
    assert!(record.retire_planned_instance(replacement));
    assert_eq!(record.state, SchedulerPathState::Suspect);
    assert_eq!(record.consecutive_failures, 0);
    assert_eq!(record.failed_until, None);
    assert_eq!(record.active_flows, 2);
    assert!(!record.retire_planned_instance(replacement));
}

#[test]
fn partial_tcp_transport_state_does_not_clear_unknown_fields() {
    let mut record = ClientPathHealthRecord::default();
    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(path_instance_id, 0, PathUsage::Available);
    record.carrier_bytes_in_flight = 64 * 1024;
    record.carrier_inflight_limit_bytes = 512 * 1024;
    record.carrier_queue_bytes = 8 * 1024;
    record.native_drain_observed = true;
    let snapshot = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 30_000,
            rttvar_us: Some(3_000),
        }),
        ..TcpNativeSnapshot::default()
    };
    let observation = TcpSenderMetricTracker::new(snapshot).observe(
        PathId(0),
        PathMetricDirection::ClientToServer,
        snapshot,
    );

    assert!(record.mark_tcp_transport_state(path_instance_id, observation));

    assert_eq!(record.carrier_srtt_ms, Some(30.0));
    assert_eq!(record.carrier_bytes_in_flight, 64 * 1024);
    assert_eq!(record.carrier_inflight_limit_bytes, 512 * 1024);
    assert_eq!(record.carrier_queue_bytes, 8 * 1024);
    assert!(!record.native_drain_observed);
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

#[test]
fn data_plane_failure_is_published_once_until_liveness_recovers() {
    let mut record = ClientPathHealthRecord::default();
    let now = Instant::now();
    let original = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(original, 0, PathUsage::Available);

    assert!(record.mark_data_plane_failure(original, now, true));
    assert!(!record.mark_data_plane_failure(original, now, true));

    // A separate reachability probe may recover logical health, but it must not
    // make the retired carrier's delayed cleanup count a second time.
    record.mark_liveness_success();
    assert!(!record.mark_data_plane_failure(original, now, true));

    assert_eq!(record.state, SchedulerPathState::Active);
    assert_eq!(record.consecutive_failures, 0);

    let replacement = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(replacement, 0, PathUsage::Available);
    assert!(!record.mark_data_plane_failure(original, now, false));
    assert_eq!(record.state, SchedulerPathState::Active);
    assert!(record.mark_data_plane_failure(replacement, now, false));

    assert_eq!(record.state, SchedulerPathState::Suspect);
    assert_eq!(record.consecutive_failures, 1);
}
