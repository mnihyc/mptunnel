use super::*;

#[cfg(target_os = "linux")]
fn snapshot() -> TcpInfoSnapshot {
    TcpInfoSnapshot {
        app_limited: true,
        retransmits: 10,
        min_rtt_us: 18_000,
        srtt_us: 20_000,
        rttvar_us: 2_000,
        snd_mss_bytes: 1_460,
        unacked_packets: 2,
        snd_ssthresh_packets: u32::MAX,
        snd_cwnd_packets: 10,
        pacing_rate_bytes_per_second: 25_000_000,
        bytes_acked: 100,
        notsent_bytes: 4_096,
        data_segments_out: 20,
        delivery_rate_bytes_per_second: 12_500_000,
    }
}

#[cfg(target_os = "linux")]
fn sender_queue_snapshot(unacked_packets: u32, notsent_bytes: u32) -> TcpSenderQueueSnapshot {
    TcpSenderQueueSnapshot {
        unacked_packets,
        notsent_bytes,
    }
}

#[test]
#[cfg(target_os = "linux")]
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
#[cfg(target_os = "linux")]
fn native_tcp_metrics_are_post_handshake_and_keep_kernel_units_explicit() {
    let baseline = snapshot();
    let tracker = TcpSenderMetricTracker::new(baseline);
    let current = TcpInfoSnapshot {
        app_limited: false,
        retransmits: 12,
        unacked_packets: 3,
        snd_cwnd_packets: 20,
        bytes_acked: baseline.bytes_acked + 300_000,
        notsent_bytes: 8_192,
        data_segments_out: baseline.data_segments_out + 200,
        ..baseline
    };
    let metrics = tracker.observe(PathId(3), PathMetricDirection::ServerToClient, current);

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
#[cfg(target_os = "linux")]
fn control_only_snapshot_never_claims_ack_derived_data() {
    let baseline = snapshot();
    let metrics = TcpSenderMetricTracker::new(baseline).observe(
        PathId(1),
        PathMetricDirection::ServerToClient,
        baseline,
    );
    assert!(!metrics.has_ack_derived_data_sample);
    assert_eq!(metrics.data_sample_count, 0);
    assert_eq!(metrics.data_sample_bytes, 0);
    assert_eq!(metrics.confidence_ppm, 0);
}

#[test]
fn tcp_capacity_proof_requires_exact_fresh_receipt() {
    let accepted_at = Instant::now();
    let proof = TcpCapacityProofCandidate {
        token: 7,
        train_bytes: 2 * 1024 * 1024,
        received_bytes: 2 * 1024 * 1024,
        rate_sample_bytes: 2 * 1024 * 1024,
        proof_elapsed: Duration::from_millis(400),
        receipt_rate_bps: 40_000_000,
        rate_bps: 80_000_000,
        accepted_at,
        expires_at: accepted_at + Duration::from_secs(1),
    };
    assert!(valid_tcp_capacity_proof_candidate_at(proof, accepted_at));
    assert!(!valid_tcp_capacity_proof_candidate_at(
        TcpCapacityProofCandidate {
            received_bytes: proof.received_bytes - 1,
            ..proof
        },
        accepted_at,
    ));
    assert!(!valid_tcp_capacity_proof_candidate_at(
        proof,
        proof.expires_at
    ));
}

#[test]
fn tcp_capacity_authority_ignores_pacing_and_bounds_delivery_uplift() {
    assert_eq!(
        tcp_capacity_authoritative_rate_bps(10_000_000, 15_000_000, 1_000_000_000),
        15_000_000
    );
    assert_eq!(
        tcp_capacity_authoritative_rate_bps(10_000_000, 1_000_000_000, 2_000_000_000),
        20_000_000
    );
}
