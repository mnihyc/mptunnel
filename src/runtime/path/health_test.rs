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
    let mut record = ClientPathHealthRecord {
        carrier_bytes_in_flight: 64 * 1024,
        carrier_inflight_limit_bytes: 512 * 1024,
        carrier_queue_bytes: 8 * 1024,
        ..ClientPathHealthRecord::default()
    };
    let snapshot = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
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
