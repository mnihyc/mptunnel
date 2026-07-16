use super::super::ack_clock::{
    reliable_ack_clock_measurement_rate_coverage_floor_bytes,
    reliable_request_ack_clock_measurement_target_bytes,
};
use super::super::capacity::PATH_OPEN_SCORE_BYTES;
use super::*;
use crate::mux::MuxLimits;
use crate::protocol::UnderlayProtocol;
use std::time::{Duration, Instant};

#[test]
fn request_rate_coverage_uses_transport_evidence_requirements() {
    let mux_limits = MuxLimits::default();
    let tcp_measurement_target = reliable_request_ack_clock_measurement_target_bytes(mux_limits);

    assert_eq!(
        request_path_rate_coverage_floor_bytes(UnderlayProtocol::Tcp, None, mux_limits),
        reliable_ack_clock_measurement_rate_coverage_floor_bytes(mux_limits)
    );
    assert_eq!(
        request_path_rate_coverage_floor_bytes(
            UnderlayProtocol::Tcp,
            Some(tcp_measurement_target),
            mux_limits,
        ),
        tcp_measurement_target
    );
    assert_eq!(
        request_path_rate_coverage_floor_bytes(
            UnderlayProtocol::Udp,
            Some(tcp_measurement_target),
            mux_limits,
        ),
        PATH_OPEN_SCORE_BYTES as u64
    );
}

#[test]
fn exact_path_provenance_counts_attributed_bytes_before_rate_coverage() {
    let started = Instant::now();
    let provenance_bytes = PATH_OPEN_SCORE_BYTES as u64;
    let rate_coverage_bytes = provenance_bytes * 2;
    let mut evidence = RequestPathRateEvidence::new(started);

    assert!(!evidence.has_exact_path_provenance());
    assert!(matches!(
        evidence.observe(
            provenance_bytes - 1,
            started,
            started,
            started + Duration::from_millis(10),
            rate_coverage_bytes,
            true,
        ),
        RequestPathRateEvidenceUpdate::Pending
    ));
    assert!(!evidence.has_exact_path_provenance());

    assert!(matches!(
        evidence.observe(
            1,
            started,
            started,
            started + Duration::from_millis(20),
            rate_coverage_bytes,
            true,
        ),
        RequestPathRateEvidenceUpdate::Pending
    ));
    assert!(evidence.has_exact_path_provenance());
}

#[test]
fn request_rate_samples_use_causal_ack_windows_and_the_slower_clock() {
    let started = Instant::now();
    let bytes = PATH_OPEN_SCORE_BYTES as u64;
    let first_acked_at = started + Duration::from_millis(100);
    let mut evidence = RequestPathRateEvidence::new(started);

    let first = match evidence.observe(bytes, started, started, first_acked_at, bytes, true) {
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(sample),
            first_window: true,
        } => sample,
        _ => panic!("the first covered ACK window must produce a path-rate sample"),
    };
    assert_eq!(first.elapsed(), Duration::from_millis(100));

    let second = match evidence.observe(
        bytes,
        first_acked_at,
        started + Duration::from_millis(140),
        started + Duration::from_millis(141),
        bytes,
        true,
    ) {
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(sample),
            first_window: false,
        } => sample,
        _ => panic!("a causal post-ACK window must produce a path-rate sample"),
    };
    assert_eq!(second.elapsed(), Duration::from_millis(41));
    assert_eq!(second.rate_bps(), bytes as f64 * 8.0 / 0.041);

    assert!(matches!(
        evidence.observe(
            bytes,
            started + Duration::from_millis(130),
            started + Duration::from_millis(150),
            started + Duration::from_millis(200),
            bytes,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven { sample: None, .. }
    ));
}

#[test]
fn request_window_growth_evidence_is_connection_level_ack_credit() {
    let observed_at = Instant::now();
    let growth_interval = Duration::from_millis(75);
    let evidence = RequestWindowGrowthEvidence::AckCredits {
        bytes: 32 * 1024,
        growth_interval,
        observed_at,
    };

    match evidence {
        RequestWindowGrowthEvidence::AckCredits {
            bytes,
            growth_interval: interval,
            observed_at: observed,
        } => {
            assert_eq!(bytes, 32 * 1024);
            assert_eq!(interval, growth_interval);
            assert_eq!(observed, observed_at);
        }
        RequestWindowGrowthEvidence::None => panic!("ACK credits must remain available"),
    }
    assert!(matches!(
        RequestWindowGrowthEvidence::None,
        RequestWindowGrowthEvidence::None
    ));
}
