use super::*;
use crate::transport::tcp_telemetry::{TcpNativeFlight, TcpNativeLossCounters, TcpNativeRtt};

fn snapshot() -> TcpNativeSnapshot {
    TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 20_000,
            rttvar_us: 2_000,
        }),
        flight: Some(TcpNativeFlight {
            snd_mss_bytes: 1_460,
            unacked_packets: 2,
            snd_ssthresh_packets: u32::MAX,
            snd_cwnd_packets: 10,
        }),
        notsent_bytes: Some(4_096),
        bytes_acked: Some(100),
        loss: Some(TcpNativeLossCounters {
            retransmits: 10,
            data_segments_out: 20,
        }),
        pacing_rate_bytes_per_second: Some(25_000_000),
        delivery_rate_bytes_per_second: Some(12_500_000),
        app_limited: Some(true),
    }
}

fn sender_queue_snapshot(unacked_packets: u32, notsent_bytes: u32) -> TcpSenderQueueSnapshot {
    TcpSenderQueueSnapshot {
        unacked_packets: Some(unacked_packets),
        notsent_bytes,
    }
}

#[test]
fn tcp_receipt_baseline_waits_only_for_unsent_bytes() {
    assert!(sender_queue_snapshot(0, 0).is_write_queue_drained());
    assert!(
        sender_queue_snapshot(1, 0).is_write_queue_drained(),
        "prior unacked control can only lengthen a full receiver receipt"
    );
    assert!(!sender_queue_snapshot(0, 1).is_write_queue_drained());
    assert!(!sender_queue_snapshot(1, 1).is_write_queue_drained());
}

#[test]
fn native_tcp_metrics_are_post_handshake_and_keep_kernel_units_explicit() {
    let baseline = snapshot();
    let current = TcpNativeSnapshot {
        app_limited: Some(false),
        flight: Some(TcpNativeFlight {
            unacked_packets: 3,
            snd_cwnd_packets: 20,
            ..baseline.flight.expect("flight baseline")
        }),
        bytes_acked: baseline.bytes_acked.map(|value| value + 300_000),
        notsent_bytes: Some(8_192),
        loss: Some(TcpNativeLossCounters {
            retransmits: 12,
            data_segments_out: baseline.loss.expect("loss baseline").data_segments_out + 200,
        }),
        ..baseline
    };
    let observation = TcpSenderMetricTracker::new(baseline).observe(
        PathId(3),
        PathMetricDirection::ServerToClient,
        current,
    );
    let metrics = observation
        .complete_path_metrics()
        .expect("complete native sample");

    assert_eq!(observation.delivery_rate_bps(), Some(100_000_000));
    assert_eq!(observation.pacing_rate_bps(), Some(200_000_000));
    assert_eq!(observation.newly_acked_bytes(), Some(300_000));
    assert_eq!(observation.acked_bytes_since_epoch(), Some(300_000));
    assert!(observation.delivery_window_covered());
    assert_eq!(metrics.delivery_rate_bps, 100_000_000);
    assert_eq!(metrics.pacing_rate_bps, 200_000_000);
    assert_eq!(metrics.bytes_in_flight, 3 * 1_460);
    assert_eq!(metrics.inflight_limit_bytes, 20 * 1_460);
    assert_eq!(metrics.inflight_hi_bytes, metrics.inflight_limit_bytes);
    assert_eq!(metrics.queue_bytes, 8_192);
    assert_eq!(metrics.data_sample_count, 0);
    assert_eq!(metrics.data_sample_bytes, 0);
    assert!(!metrics.has_ack_derived_data_sample);
    assert!(metrics.loss_observed);
    assert!(!metrics.app_limited);
}

#[test]
fn slow_start_cwnd_growth_does_not_move_the_delivery_epoch_floor() {
    let baseline = snapshot();
    let current = TcpNativeSnapshot {
        flight: Some(TcpNativeFlight {
            snd_cwnd_packets: 300,
            ..baseline.flight.expect("flight baseline")
        }),
        bytes_acked: baseline.bytes_acked.map(|value| value + 64 * 1024),
        app_limited: Some(false),
        ..baseline
    };
    let observation = TcpSenderMetricTracker::new(baseline).observe(
        PathId(0),
        PathMetricDirection::ClientToServer,
        current,
    );

    assert!(observation.delivery_window_covered());
    assert!(
        observation.acked_bytes_since_epoch().unwrap_or(0)
            < observation.flight().expect("current flight").1,
        "the proof covers the frozen epoch even while the live cwnd grows"
    );
}

#[test]
fn repeated_native_poll_without_ack_progress_has_no_fresh_bytes() {
    let baseline = snapshot();
    let current = TcpNativeSnapshot {
        bytes_acked: baseline.bytes_acked.map(|value| value + 300_000),
        app_limited: Some(false),
        ..baseline
    };
    let mut tracker = TcpSenderMetricTracker::new(baseline);
    let first = tracker.observe(PathId(0), PathMetricDirection::ClientToServer, current);
    let repeated = tracker.observe(PathId(0), PathMetricDirection::ClientToServer, current);

    assert_eq!(first.newly_acked_bytes(), Some(300_000));
    assert_eq!(repeated.newly_acked_bytes(), Some(0));
    assert_eq!(repeated.acked_bytes_since_epoch(), Some(300_000));
}

#[test]
fn pacing_only_native_evidence_never_claims_delivery() {
    let baseline = snapshot();
    let current = TcpNativeSnapshot {
        delivery_rate_bytes_per_second: Some(0),
        pacing_rate_bytes_per_second: Some(50_000_000),
        loss: Some(TcpNativeLossCounters {
            data_segments_out: baseline.loss.expect("loss baseline").data_segments_out + 1,
            ..baseline.loss.expect("loss baseline")
        }),
        ..baseline
    };
    let observation = TcpSenderMetricTracker::new(baseline).observe(
        PathId(2),
        PathMetricDirection::ServerToClient,
        current,
    );
    let metrics = observation
        .complete_path_metrics()
        .expect("complete native sample");

    assert_eq!(observation.delivery_rate_bps(), None);
    assert_eq!(observation.pacing_rate_bps(), Some(400_000_000));
    assert_eq!(metrics.delivery_rate_bps, 1);
    assert_eq!(metrics.pacing_rate_bps, 400_000_000);
}

#[test]
fn partial_pacing_snapshot_cannot_replace_complete_path_metrics() {
    let baseline = TcpNativeSnapshot {
        pacing_rate_bytes_per_second: Some(10_000_000),
        ..TcpNativeSnapshot::default()
    };
    let current = TcpNativeSnapshot {
        pacing_rate_bytes_per_second: Some(20_000_000),
        ..TcpNativeSnapshot::default()
    };
    let observation = TcpSenderMetricTracker::new(baseline).observe(
        PathId(2),
        PathMetricDirection::ServerToClient,
        current,
    );

    assert_eq!(observation.pacing_rate_bps(), Some(160_000_000));
    assert_eq!(observation.delivery_rate_bps(), None);
    assert_eq!(observation.complete_path_metrics(), None);
}

#[test]
fn partial_rtt_snapshot_applies_available_timing() {
    let snapshot = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 30_000,
            rttvar_us: 3_000,
        }),
        ..TcpNativeSnapshot::default()
    };
    let observation = TcpSenderMetricTracker::new(snapshot).observe(
        PathId(2),
        PathMetricDirection::ServerToClient,
        snapshot,
    );
    let mut metrics = crate::runtime::path::tcp::capacity::request_tcp_capacity_receipt_metrics(
        PathId(2),
        1_024,
        1_000_000,
        None,
        None,
    );
    observation.apply_transport_shape(&mut metrics);

    assert_eq!(metrics.srtt_us, 30_000);
    assert_eq!(observation.complete_path_metrics(), None);
}

#[test]
fn positive_sender_interval_records_measured_zero_loss() {
    let baseline = snapshot();
    let current = TcpNativeSnapshot {
        loss: Some(TcpNativeLossCounters {
            retransmits: baseline.loss.expect("loss baseline").retransmits,
            data_segments_out: baseline.loss.expect("loss baseline").data_segments_out + 20,
        }),
        bytes_acked: baseline.bytes_acked.map(|value| value + 30_000),
        ..baseline
    };
    let metrics = TcpSenderMetricTracker::new(baseline)
        .observe(PathId(2), PathMetricDirection::ServerToClient, current)
        .complete_path_metrics()
        .expect("complete zero-loss sample");

    assert_eq!(metrics.loss_ppm, 0);
    assert!(metrics.loss_observed);
}

#[test]
fn native_loss_counters_remain_valid_across_u32_wrap() {
    let baseline = TcpNativeSnapshot {
        loss: Some(TcpNativeLossCounters {
            retransmits: u32::MAX - 1,
            data_segments_out: u32::MAX - 10,
        }),
        ..snapshot()
    };
    let current = TcpNativeSnapshot {
        loss: Some(TcpNativeLossCounters {
            retransmits: 0,
            data_segments_out: 9,
        }),
        bytes_acked: baseline.bytes_acked.map(|value| value + 30_000),
        ..baseline
    };
    let metrics = TcpSenderMetricTracker::new(baseline)
        .observe(PathId(2), PathMetricDirection::ServerToClient, current)
        .complete_path_metrics()
        .expect("complete wrapped-counter sample");

    assert_eq!(metrics.loss_ppm, 100_000);
    assert!(metrics.loss_observed);
}

#[test]
fn control_only_snapshot_never_claims_ack_derived_data() {
    let baseline = snapshot();
    let observation = TcpSenderMetricTracker::new(baseline).observe(
        PathId(1),
        PathMetricDirection::ServerToClient,
        baseline,
    );
    assert_eq!(observation.complete_path_metrics(), None);
}
