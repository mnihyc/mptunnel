use super::*;

#[test]
fn udp_stream_congestion_self_clocks_and_cuts_back_on_repair_timeout() {
    let mux_limits = MuxLimits {
        max_tcp_path_inflight_bytes: 64 * 1024,
        max_tcp_relay_chunk_bytes: 64 * 1024,
        max_payload_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    let mss = udp_stream_frame_payload_bytes(mux_limits);
    let mut congestion = UdpStreamCongestion::new(mux_limits);
    let initial = congestion.inflight_limit();

    assert_eq!(initial, mss.saturating_mul(10).min(64 * 1024));
    assert_eq!(congestion.repair_budget(0), 0);
    assert_eq!(congestion.repair_budget(mss / 2), mss);

    congestion.on_send(mss * 4);
    congestion.on_ack(mss * 4);
    assert!(congestion.inflight_limit() > initial);

    for _ in 0..32 {
        congestion.on_ack(64 * 1024);
    }
    assert_eq!(congestion.inflight_limit(), 64 * 1024);

    congestion.on_repair_timeout();
    assert!(congestion.inflight_limit() < 64 * 1024);
    assert!(congestion.inflight_limit() >= udp_stream_min_cwnd_bytes(mss).min(64 * 1024));
}

#[test]
fn udp_stream_congestion_ceiling_uses_path_inflight_budget() {
    let mux_limits = MuxLimits {
        max_tcp_path_inflight_bytes: 4 * 1024 * 1024,
        max_tcp_relay_chunk_bytes: 256 * 1024,
        ..MuxLimits::default()
    };
    let mss = udp_stream_frame_payload_bytes(mux_limits);
    let mut congestion = UdpStreamCongestion::new(mux_limits);

    assert!(congestion.inflight_limit() < mux_limits.max_tcp_relay_chunk_bytes);
    for _ in 0..32 {
        congestion.on_ack(mux_limits.max_tcp_path_inflight_bytes);
    }
    assert_eq!(
        congestion.inflight_limit(),
        mux_limits.max_tcp_path_inflight_bytes
    );
    assert_eq!(
        congestion.repair_budget(usize::MAX),
        mux_limits.max_tcp_path_inflight_bytes / 4
    );
    assert!(congestion.repair_budget(usize::MAX) >= mux_limits.max_tcp_relay_chunk_bytes);
    assert!(congestion.repair_budget(mss / 2) >= mss);
}

#[test]
fn udp_stream_ack_gap_repair_budget_is_bounded_to_path_burst() {
    let mux_limits = MuxLimits {
        max_tcp_path_inflight_bytes: 4 * 1024 * 1024,
        max_tcp_relay_chunk_bytes: 256 * 1024,
        ..MuxLimits::default()
    };
    let mss = udp_stream_frame_payload_bytes(mux_limits);

    assert_eq!(udp_stream_ack_gap_repair_budget(0, mux_limits), 0);
    assert_eq!(udp_stream_ack_gap_repair_budget(mss / 2, mux_limits), mss);
    assert_eq!(
        udp_stream_ack_gap_repair_budget(usize::MAX, mux_limits),
        mux_limits.max_tcp_path_inflight_bytes / 4
    );
}

#[test]
fn udp_stream_congestion_paces_after_rtt_evidence() {
    let mux_limits = MuxLimits::default();
    let mss = udp_stream_frame_payload_bytes(mux_limits);
    let mut congestion = UdpStreamCongestion::new(mux_limits);

    assert_eq!(congestion.pacing_interval(mss), None);

    congestion.on_send(mss);
    let sample = congestion
        .pending_samples
        .front_mut()
        .expect("pending sample");
    sample.sent_at = sample
        .sent_at
        .checked_sub(Duration::from_millis(80))
        .expect("past sample");
    congestion.on_ack(mss);

    let interval = congestion
        .pacing_interval(mss)
        .expect("paced after ack RTT");
    assert!(interval > Duration::ZERO);
    assert!(interval < Duration::from_millis(80));
}

#[test]
fn udp_stream_repair_replay_uses_measured_ack_rtt() {
    let mux_limits = MuxLimits::default();
    let mss = udp_stream_frame_payload_bytes(mux_limits);
    let mut congestion = UdpStreamCongestion::new(mux_limits);
    let base_interval =
        udp_stream_repair_replay_interval(mux_limits.max_tcp_path_inflight_bytes, mux_limits);

    assert_eq!(
        congestion.repair_replay_interval(mux_limits.max_tcp_path_inflight_bytes, mux_limits),
        base_interval
    );

    congestion.on_send(mss);
    let sample = congestion
        .pending_samples
        .front_mut()
        .expect("pending sample");
    sample.sent_at = sample
        .sent_at
        .checked_sub(Duration::from_millis(360))
        .expect("past sample");
    congestion.on_ack(mss);

    let high_rtt_interval =
        congestion.repair_replay_interval(mux_limits.max_tcp_path_inflight_bytes, mux_limits);
    assert!(high_rtt_interval > base_interval);
    assert!(high_rtt_interval <= TCP_STREAM_STALL_MAX_TIMEOUT);
}
