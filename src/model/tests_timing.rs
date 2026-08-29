use super::*;
use crate::protocol::{PathId, UnderlayProtocol};
use crate::scheduler::PathSnapshot;
use std::time::Instant;

#[test]
fn data_ack_loss_delay_uses_rack_and_quic_time_thresholds() {
    let tcp = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 1.0);
    let quic = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 100.0, 1.0);

    assert_eq!(
        reliable_data_ack_loss_delay(Some(UnderlayProtocol::Tcp), Some(tcp)),
        Some(Duration::from_millis(125)),
    );
    assert_eq!(
        reliable_data_ack_loss_delay(Some(UnderlayProtocol::Udp), Some(quic)),
        Some(Duration::from_micros(112_500)),
    );
    assert_eq!(reliable_data_ack_loss_delay(None, None), None);
}

#[test]
fn data_ack_gap_repair_uses_absolute_completion_and_owner_fallback() {
    let mut tcp = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 80.0, 1.0);
    tcp.jitter_ms = 10.0;
    let assignment_at = Instant::now();
    let loss_at = assignment_at + Duration::from_millis(100);
    let recovery_at = assignment_at + Duration::from_millis(200);

    assert_eq!(
        reliable_data_ack_gap_reinjection_deadline(
            Some(assignment_at),
            Some(UnderlayProtocol::Tcp),
            Some(tcp),
            Some(Duration::from_millis(50)),
            assignment_at,
        ),
        Some(loss_at),
    );
    assert_eq!(
        reliable_data_ack_gap_reinjection_deadline(
            Some(assignment_at),
            Some(UnderlayProtocol::Tcp),
            Some(tcp),
            Some(Duration::from_millis(50)),
            assignment_at + Duration::from_millis(175),
        ),
        Some(recovery_at),
        "a current completion estimate starts at observation time, not assignment time",
    );

    assert!(!reliable_data_ack_gap_reinjection_ready(
        Some(assignment_at),
        Some(UnderlayProtocol::Tcp),
        Some(tcp),
        Some(Duration::from_millis(50)),
        assignment_at + Duration::from_millis(99),
    ));
    assert!(reliable_data_ack_gap_reinjection_ready(
        Some(assignment_at),
        Some(UnderlayProtocol::Tcp),
        Some(tcp),
        Some(Duration::from_millis(50)),
        loss_at,
    ));

    // A 150 ms copy launched at the 100 ms loss threshold would finish at
    // 250 ms, after the owner's 200 ms recovery boundary. Arm the exact owner
    // fallback instead of comparing the two durations as if both started at
    // assignment time.
    assert_eq!(
        reliable_data_ack_gap_reinjection_deadline(
            Some(assignment_at),
            Some(UnderlayProtocol::Tcp),
            Some(tcp),
            Some(Duration::from_millis(150)),
            assignment_at,
        ),
        Some(recovery_at),
    );
    assert!(!reliable_data_ack_gap_reinjection_ready(
        Some(assignment_at),
        Some(UnderlayProtocol::Tcp),
        Some(tcp),
        Some(Duration::from_millis(150)),
        loss_at,
    ));
    assert!(reliable_data_ack_gap_reinjection_ready(
        Some(assignment_at),
        Some(UnderlayProtocol::Tcp),
        Some(tcp),
        Some(Duration::from_millis(500)),
        recovery_at,
    ));
}

#[test]
fn data_ack_silence_waits_for_the_owner_recovery_interval() {
    let mut tcp = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 80.0, 1.0);
    tcp.jitter_ms = 10.0;
    let assigned_at = Instant::now();

    assert_eq!(
        reliable_data_ack_recovery_deadline(
            Some(assigned_at),
            Some(UnderlayProtocol::Tcp),
            Some(tcp),
            Some(Duration::from_millis(150)),
        ),
        Some(assigned_at + Duration::from_millis(200)),
    );
    assert_eq!(
        reliable_data_ack_recovery_deadline(
            Some(assigned_at),
            Some(UnderlayProtocol::Tcp),
            Some(tcp),
            Some(Duration::from_millis(200)),
        ),
        Some(assigned_at + Duration::from_millis(200)),
    );
}

#[test]
fn tcp_data_retransmission_uses_rto_without_quic_ack_delay() {
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 80.0, 1.0);
    path.jitter_ms = 10.0;

    assert_eq!(
        reliable_data_retransmission_interval(Some(UnderlayProtocol::Tcp), Some(path)),
        Duration::from_millis(200),
    );

    path.srtt_ms = 500.0;
    path.jitter_ms = 60.0;
    assert_eq!(
        reliable_data_retransmission_interval(Some(UnderlayProtocol::Tcp), Some(path)),
        Duration::from_millis(750),
    );
}

#[test]
fn quic_data_retransmission_uses_pto() {
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 500.0, 1.0);
    path.jitter_ms = 60.0;

    assert_eq!(
        reliable_data_retransmission_interval(Some(UnderlayProtocol::Udp), Some(path)),
        Duration::from_millis(775),
    );
}

#[test]
fn stale_path_threshold_is_later_than_data_retransmission() {
    let mut tcp = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 80.0, 1.0);
    tcp.jitter_ms = 10.0;
    assert_eq!(
        reliable_path_stale_interval(Some(UnderlayProtocol::Tcp), Some(tcp)),
        Duration::from_millis(800),
    );

    let mut quic = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 80.0, 1.0);
    quic.jitter_ms = 10.0;
    let pto = transport_pto_from_snapshot(Some(quic));
    assert_eq!(
        reliable_path_stale_interval(Some(UnderlayProtocol::Udp), Some(quic)),
        pto.saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD),
    );
}

#[test]
fn cold_reliable_path_open_counts_transport_join_and_stream_acceptance() {
    let tcp = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 80.0, 1.0);
    let quic = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 80.0, 1.0);

    assert_eq!(path_open_serialized_exchanges(Some(tcp)), 3);
    assert_eq!(path_open_serialized_exchanges(Some(quic)), 3);
}
