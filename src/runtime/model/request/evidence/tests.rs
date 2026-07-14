use super::super::super::ack_clock::{
    reliable_ack_clock_calibration_rate_coverage_floor_bytes,
    reliable_request_ack_clock_calibration_target_bytes,
};
use super::super::super::capacity::{PATH_OPEN_SCORE_BYTES, PathRateSample};
use super::*;
use crate::mux::MuxLimits;
use crate::protocol::UnderlayProtocol;
use std::time::{Duration, Instant};

#[test]
fn request_tcp_rate_uses_representative_coverage_for_service_and_candidate() {
    let mux_limits = MuxLimits::default();
    assert_eq!(
        request_path_rate_coverage_floor_bytes(UnderlayProtocol::Tcp, true, None, mux_limits,),
        reliable_ack_clock_calibration_rate_coverage_floor_bytes(mux_limits)
    );
    assert_eq!(
        request_path_rate_coverage_floor_bytes(
            UnderlayProtocol::Tcp,
            false,
            Some(reliable_request_ack_clock_calibration_target_bytes(
                mux_limits
            )),
            mux_limits,
        ),
        reliable_request_ack_clock_calibration_target_bytes(mux_limits)
    );
}

#[test]
fn request_tcp_turnover_authority_requires_calibration_plus_ordinary_coverage() {
    let started = Instant::now();
    let target = reliable_request_ack_clock_calibration_target_bytes(MuxLimits::default());
    let first_ack = started + Duration::from_millis(100);
    let mut split = RequestPathRateEvidence::new(started);
    let mut coalesced = RequestPathRateEvidence::new(started);

    for evidence in [&mut split, &mut coalesced] {
        assert!(matches!(
            evidence.observe(target, started, started, first_ack, target, true),
            RequestPathRateEvidenceUpdate::Proven {
                sample: Some(_),
                ..
            }
        ));
        assert!(!request_tcp_candidate_turnover_authorized(
            evidence.exact_attributed_bytes(),
            target,
            target,
        ));
    }

    let second_sent = first_ack + Duration::from_millis(1);
    assert!(matches!(
        split.observe(
            target / 2,
            second_sent,
            second_sent,
            first_ack + Duration::from_millis(50),
            target,
            true,
        ),
        RequestPathRateEvidenceUpdate::Pending
    ));
    assert!(!request_tcp_candidate_turnover_authorized(
        split.exact_attributed_bytes(),
        target,
        target,
    ));
    assert!(matches!(
        split.observe(
            target - target / 2,
            second_sent,
            second_sent,
            first_ack + Duration::from_millis(100),
            target,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(_),
            ..
        }
    ));
    assert!(matches!(
        coalesced.observe(
            target,
            second_sent,
            second_sent,
            first_ack + Duration::from_millis(100),
            target,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(_),
            ..
        }
    ));
    assert_eq!(
        split.exact_attributed_bytes(),
        coalesced.exact_attributed_bytes()
    );
    assert!(request_tcp_candidate_turnover_authorized(
        split.exact_attributed_bytes(),
        target,
        target,
    ));
    assert!(request_tcp_candidate_turnover_authorized(
        coalesced.exact_attributed_bytes(),
        target,
        target,
    ));
}

#[test]
fn request_tcp_turnover_smooths_same_epoch_pipe_and_expires_at_three_ptos() {
    let started = Instant::now();
    let first_sample =
        PathRateSample::new(1_000_000, Duration::from_millis(100)).expect("first rate sample");
    let first_pto = Duration::from_millis(50);
    let first = RequestTcpAckTurnoverModel::observe(None, first_sample, first_pto, started)
        .expect("first turnover");
    let first_pipe = first_sample.rate_bps() / 8.0 * first_pto.as_secs_f64();
    assert_eq!(first.turnover_bytes, first_pipe);
    assert!(first.is_fresh_at(started + Duration::from_millis(149)));
    assert!(!first.is_fresh_at(started + Duration::from_millis(150)));

    let second_sample =
        PathRateSample::new(2_000_000, Duration::from_millis(100)).expect("second rate sample");
    let second_pto = Duration::from_millis(200);
    let second = RequestTcpAckTurnoverModel::observe(
        Some(first),
        second_sample,
        second_pto,
        started + Duration::from_millis(100),
    )
    .expect("smoothed turnover");
    let second_pipe = second_sample.rate_bps() / 8.0 * second_pto.as_secs_f64();
    assert_eq!(
        second.turnover_bytes,
        first_pipe.mul_add(0.75, second_pipe * 0.25),
        "smooth pipe estimates captured with each sample's PTO, not a retained rate times current PTO"
    );
}

#[test]
fn request_rate_evidence_uses_ack_clock_after_initial_provenance() {
    let started = Instant::now();
    let bytes = PATH_OPEN_SCORE_BYTES as u64;
    let mut evidence = RequestPathRateEvidence::new(started);

    let initial = match evidence.observe(
        bytes,
        started,
        started,
        started + Duration::from_millis(100),
        bytes,
        true,
    ) {
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(sample),
            ..
        } => sample.rate_bps(),
        _ => panic!("first complete window must publish conservative provenance"),
    };
    let ack_clocked = match evidence.observe(
        bytes,
        started + Duration::from_millis(100),
        started + Duration::from_millis(100),
        started + Duration::from_millis(101),
        bytes,
        true,
    ) {
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(sample),
            ..
        } => sample.rate_bps(),
        _ => panic!("pipelined bytes must use ACK-to-ACK delivery time"),
    };

    assert!(
        ack_clocked > initial * 50.0,
        "a post-boundary stage must use ACK-to-ACK time without charging the first-stage RTT again"
    );
}

#[test]
fn request_service_rate_keeps_continuous_ack_clock_for_pipelined_bytes() {
    let started = Instant::now();
    let bytes = PATH_OPEN_SCORE_BYTES as u64;
    let first_ack = started + Duration::from_millis(100);
    let mut evidence = RequestPathRateEvidence::new(started);
    assert!(matches!(
        evidence.observe(bytes, started, started, first_ack, bytes, false),
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(_),
            first_window: true,
        }
    ));

    let sample = match evidence.observe(
        bytes,
        started + Duration::from_millis(90),
        started + Duration::from_millis(95),
        started + Duration::from_millis(120),
        bytes,
        false,
    ) {
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(sample),
            first_window: false,
        } => sample,
        _ => panic!("ordered Service bytes must retain continuous ACK-clock evidence"),
    };
    assert_eq!(sample.elapsed(), Duration::from_millis(20));
}

#[test]
fn request_rate_evidence_charges_post_boundary_idle_gap() {
    let started = Instant::now();
    let bytes = PATH_OPEN_SCORE_BYTES as u64;
    let mut evidence = RequestPathRateEvidence::new(started);
    assert!(matches!(
        evidence.observe(
            bytes,
            started,
            started,
            started + Duration::from_millis(100),
            bytes,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(_),
            ..
        }
    ));

    let conservative = match evidence.observe(
        bytes,
        started + Duration::from_millis(200),
        started + Duration::from_millis(200),
        started + Duration::from_millis(300),
        bytes,
        true,
    ) {
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(sample),
            ..
        } => sample,
        _ => panic!("post-boundary bytes must retain the full idle gap in their rate"),
    };
    assert_eq!(conservative.elapsed(), Duration::from_millis(200));
    assert!(matches!(
        evidence.observe(
            bytes,
            started + Duration::from_millis(290),
            started + Duration::from_millis(290),
            started + Duration::from_millis(301),
            bytes,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven { sample: None, .. }
    ));
}

#[test]
fn request_rate_evidence_rejects_window_with_any_pre_ack_bytes() {
    let started = Instant::now();
    let bytes = PATH_OPEN_SCORE_BYTES as u64;
    let previous_ack = started + Duration::from_millis(100);
    let mut evidence = RequestPathRateEvidence::new(started);
    assert!(matches!(
        evidence.observe(bytes, started, started, previous_ack, bytes, true),
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(_),
            ..
        }
    ));

    let old_byte_sent_at = started + Duration::from_millis(90);
    let new_bytes_sent_at = started + Duration::from_millis(101);
    assert!(matches!(
        evidence.observe(
            1,
            old_byte_sent_at,
            old_byte_sent_at,
            started + Duration::from_millis(110),
            bytes,
            true,
        ),
        RequestPathRateEvidenceUpdate::Pending
    ));
    assert!(matches!(
        evidence.observe(
            bytes - 1,
            new_bytes_sent_at,
            new_bytes_sent_at,
            started + Duration::from_millis(200),
            bytes,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven { sample: None, .. }
    ));
}

#[test]
fn request_rate_evidence_waits_for_representative_coverage() {
    let started = Instant::now();
    let coverage_floor =
        reliable_ack_clock_calibration_rate_coverage_floor_bytes(MuxLimits::default());
    let mut evidence = RequestPathRateEvidence::new(started);

    assert!(matches!(
        evidence.observe(
            coverage_floor / 2,
            started,
            started,
            started + Duration::from_millis(10),
            coverage_floor,
            true,
        ),
        RequestPathRateEvidenceUpdate::Pending
    ));
    assert!(matches!(
        evidence.observe(
            coverage_floor - coverage_floor / 2,
            started,
            started,
            started + Duration::from_millis(20),
            coverage_floor,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(_),
            first_window: true,
        }
    ));
}

#[test]
fn request_rate_evidence_post_boundary_clock_cannot_outrun_send_rate() {
    let started = Instant::now();
    let bytes = reliable_ack_clock_calibration_rate_coverage_floor_bytes(MuxLimits::default());
    let mut evidence = RequestPathRateEvidence::new(started);
    assert!(matches!(
        evidence.observe(
            bytes,
            started,
            started,
            started + Duration::from_millis(100),
            bytes,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven {
            first_window: true,
            ..
        }
    ));

    let sample = match evidence.observe(
        bytes,
        started + Duration::from_millis(100),
        started + Duration::from_millis(140),
        started + Duration::from_millis(141),
        bytes,
        true,
    ) {
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(sample),
            first_window: false,
        } => sample,
        _ => panic!("a causal second window must produce a sample"),
    };
    assert_eq!(sample.elapsed(), Duration::from_millis(41));
    assert_eq!(sample.rate_bps(), bytes as f64 * 8.0 / 0.041);
}
