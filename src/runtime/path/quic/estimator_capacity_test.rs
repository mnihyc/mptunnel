use super::super::estimator_test_support::*;
use super::*;
use crate::runtime::stream::response::quic_capacity_receipt_rate_bps;

#[test]
fn quic_app_limited_capacity_probe_emits_candidate_without_generic_proof() {
    let now = Instant::now();
    let required_bytes = 240 * 1024_u64;
    let mut congestion = quic_congestion(256 * 1024, Some(100_000_000));
    congestion.app_limited = true;
    congestion = with_capacity_probe(
        congestion,
        capacity_probe_metrics(
            41,
            now,
            0,
            required_bytes,
            required_bytes,
            32,
            Some(Duration::from_millis(40)),
        ),
    );
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = 256 * 1024;
    stats.path.current_mtu = 1400;
    let mut tracker = QuicPathMetricTracker::default();

    let observed = tracker.observe_at(stats, congestion, 2, now);
    let candidate = observed
        .capacity_proof_candidate
        .expect("receiver-confirmed capacity token");

    assert_eq!(candidate.token, 41);
    assert!(candidate.receipt_confirmed);
    assert_eq!(candidate.received_bytes, candidate.train_bytes);
    assert_eq!(candidate.proof_elapsed, Duration::from_millis(80));
    assert!(candidate.written_data_frame_count > 0);
    assert!(observed.app_limited);
    assert_eq!(observed.delivery_sample_count, 0);
    assert_eq!(observed.delivery_sample_bytes, 0);
    assert!(observed.bulk_proof_expires_at.is_none());
}

#[test]
fn quic_capacity_receipt_publishes_after_terminalization_and_freezes_rate() {
    let now = Instant::now();
    let required_bytes = 240 * 1024_u64;
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = 256 * 1024;
    stats.path.current_mtu = 1400;
    let base = quic_congestion(256 * 1024, Some(100_000_000));
    let mut probe = capacity_probe_metrics(
        42,
        now,
        0,
        required_bytes,
        required_bytes,
        32,
        Some(Duration::from_millis(40)),
    );
    probe.phase = quic_transport::MeasurementPhase::Complete;
    probe.last_authoritative_in_flight = Some(0);
    probe.last_authoritative_sent_watermark = Some(10_000);
    probe.receipt_frozen_sent_watermark = Some(11_200);
    probe.current_sent_watermark = 11_200;
    let mut tracker = QuicPathMetricTracker::default();

    let measured = tracker.observe_at(stats, with_capacity_probe(base, probe), 2, now);
    let candidate = measured
        .capacity_proof_candidate
        .expect("terminal exact receipt publishes independently of native cleanup");
    assert_eq!(candidate.proof_elapsed, Duration::from_millis(80));
    assert_eq!(candidate.accepted_at, now);
    assert_eq!(candidate.expires_at, now + candidate.proof_validity);
    assert_eq!(
        candidate.rate_bps,
        quic_capacity_receipt_rate_bps(candidate.train_bytes, candidate.proof_elapsed)
            .expect("receipt rate")
    );

    probe.phase = quic_transport::MeasurementPhase::Complete;
    probe.timed_measurement_ack_elapsed = Some(Duration::from_secs(2));
    probe.current_sent_watermark = 12_400;
    let later = tracker.observe_at(
        stats,
        with_capacity_probe(base, probe),
        2,
        now + Duration::from_millis(10),
    );
    assert_eq!(later.capacity_proof_candidate, Some(candidate));

    let mut late_tracker = QuicPathMetricTracker::default();
    let independently_observed = late_tracker.observe_at(
        stats,
        with_capacity_probe(base, probe),
        2,
        now + Duration::from_millis(20),
    );
    assert_eq!(
        independently_observed.capacity_proof_candidate,
        Some(candidate)
    );
}

#[test]
fn quic_capacity_candidate_accepts_only_receipted_publishable_phases() {
    let now = Instant::now();
    let required_bytes = 240 * 1024_u64;
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = 256 * 1024;
    stats.path.current_mtu = 1400;
    let base = quic_congestion(256 * 1024, Some(100_000_000));

    let proven = QuicPathMetricTracker::default().observe_at(
        stats,
        with_capacity_probe(
            base,
            capacity_probe_metrics(43, now, 0, required_bytes, 0, 0, None),
        ),
        2,
        now,
    );
    assert!(proven.capacity_proof_candidate.is_some());
    for phase in [
        quic_transport::MeasurementPhase::Writing,
        quic_transport::MeasurementPhase::Measuring,
        quic_transport::MeasurementPhase::AwaitingReceipt,
        quic_transport::MeasurementPhase::Expired,
        quic_transport::MeasurementPhase::Aborted,
    ] {
        let mut probe = capacity_probe_metrics(44, now, 0, required_bytes, 0, 0, None);
        probe.phase = phase;
        let observed = QuicPathMetricTracker::default().observe_at(
            stats,
            with_capacity_probe(base, probe),
            2,
            now,
        );
        assert!(
            observed.capacity_proof_candidate.is_none(),
            "phase {phase:?} cannot publish receipt authority"
        );
    }
}

#[test]
fn quic_capacity_probe_requires_exact_full_train_receipt() {
    let now = Instant::now();
    let warmup_bytes = 384 * 1024_u64;
    let required_bytes = 240 * 1024_u64;
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = 512 * 1024;
    stats.path.current_mtu = 1400;
    let base = quic_congestion(512 * 1024, Some(100_000_000));
    let mut tracker = QuicPathMetricTracker::default();

    let mut incomplete_receipt =
        capacity_probe_metrics(51, now, warmup_bytes, required_bytes, 0, 0, None);
    incomplete_receipt.receipt_received_payload_bytes = incomplete_receipt.train_payload_bytes - 1;
    let below_floor =
        tracker.observe_at(stats, with_capacity_probe(base, incomplete_receipt), 2, now);
    assert!(below_floor.capacity_proof_candidate.is_none());

    let proven = tracker.observe_at(
        stats,
        with_capacity_probe(
            base,
            capacity_probe_metrics(51, now, warmup_bytes, required_bytes, 0, 0, None),
        ),
        2,
        now + Duration::from_millis(1),
    );
    let candidate = proven
        .capacity_proof_candidate
        .expect("exact receiver-confirmed train");
    assert_eq!(candidate.warmup_bytes, warmup_bytes);
    assert_eq!(candidate.received_bytes, candidate.train_bytes);
    assert_eq!(candidate.required_proof_bytes, required_bytes);
}

#[test]
fn quic_capacity_receipt_candidate_is_sticky_and_frozen() {
    let now = Instant::now();
    let required_bytes = 240 * 1024_u64;
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = 256 * 1024;
    stats.path.current_mtu = 1400;
    let base = quic_congestion(256 * 1024, Some(100_000_000));
    let mut tracker = QuicPathMetricTracker::default();
    let probe = |token, elapsed| {
        with_capacity_probe(
            base,
            capacity_probe_metrics(token, now, 0, required_bytes, required_bytes, 32, elapsed),
        )
    };

    let received = tracker.observe_at(stats, probe(61, None), 2, now);
    let accepted = received
        .capacity_proof_candidate
        .expect("receipt does not depend on a native ACK span");
    let mut retried = tracker.observe_at(
        stats,
        probe(61, Some(Duration::from_millis(40))),
        2,
        now + Duration::from_millis(2),
    );
    let retried_candidate = retried
        .capacity_proof_candidate
        .expect("transient rejection must retain sticky token");
    assert_eq!(retried_candidate.token, accepted.token);
    tracker.accept_capacity_proof(&mut retried, retried_candidate);
    let frozen_deadline = retried_candidate.expires_at;
    assert_eq!(
        frozen_deadline,
        retried_candidate.accepted_at + retried_candidate.proof_validity
    );
    let sticky = tracker.observe_at(
        stats,
        probe(61, Some(Duration::from_millis(40))),
        2,
        now + Duration::from_millis(3),
    );
    assert!(sticky.capacity_proof_candidate.is_none());
    assert!(sticky.bulk_proof_expires_at.is_none());
    let expired_sticky = tracker.observe_at(
        stats,
        probe(61, Some(Duration::from_millis(40))),
        2,
        frozen_deadline,
    );
    assert!(expired_sticky.app_limited);
    assert!(expired_sticky.capacity_proof_candidate.is_none());
    let rollover_at = frozen_deadline + Duration::from_millis(1);
    let rollover = tracker.observe_at(
        stats,
        with_capacity_probe(
            base,
            capacity_probe_metrics(
                62,
                rollover_at,
                0,
                required_bytes,
                required_bytes,
                32,
                Some(Duration::from_millis(40)),
            ),
        ),
        2,
        rollover_at,
    );
    assert_eq!(
        rollover.capacity_proof_candidate.map(|proof| proof.token),
        Some(62)
    );
}
