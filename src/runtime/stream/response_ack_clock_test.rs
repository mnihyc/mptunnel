use super::*;

fn ack_clock_rate_sample(bytes: u64, rate_bps: f64) -> PathRateSample {
    PathRateSample::new(
        bytes,
        Duration::from_secs_f64(bytes as f64 * 8.0 / rate_bps),
    )
    .expect("valid ACK-clock rate sample")
}

fn assert_rate_close(actual: Option<f64>, expected: f64) {
    let actual = actual.expect("calibrated rate");
    let relative_error = (actual - expected).abs() / expected.max(1.0);
    assert!(
        relative_error < 1e-6,
        "expected {expected} bps, got {actual} bps"
    );
}

fn apply_ack_clock_evidence_update(
    calibration: &mut ResponseAckClockCalibrationState,
    update: ResponseAckClockRateEvidenceUpdate,
    acked_at: Instant,
) -> bool {
    let ResponseAckClockRateEvidenceUpdate::Proven {
        sample,
        bytes,
        fresh_bytes,
        first_window,
        earliest_sent_at,
        ..
    } = update
    else {
        return false;
    };
    calibration.record_ack_clock_window(
        if first_window { None } else { sample },
        bytes,
        fresh_bytes,
        earliest_sent_at,
        acked_at,
    )
}

#[test]
fn response_ack_clock_requires_a_later_window_already_in_flight() {
    let started_at = Instant::now();
    let first_acked_at = started_at + Duration::from_millis(100);
    let mut evidence = ResponseAckClockRateEvidence::new(started_at);
    assert!(matches!(
        evidence.observe(
            PATH_OPEN_SCORE_BYTES as u64,
            started_at,
            started_at,
            first_acked_at,
        ),
        ResponseAckClockRateEvidenceUpdate::Proven {
            sample: Some(_),
            first_window: true,
            ..
        }
    ));

    let second_acked_at = first_acked_at + Duration::from_millis(20);
    let second = evidence.observe(
        PATH_OPEN_SCORE_BYTES as u64,
        first_acked_at - Duration::from_millis(10),
        first_acked_at - Duration::from_millis(10),
        second_acked_at,
    );
    let ResponseAckClockRateEvidenceUpdate::Proven {
        sample: Some(sample),
        first_window: false,
        ..
    } = second
    else {
        panic!("a later window already in flight at the previous ACK is ACK-clocked");
    };
    assert_eq!(
        sample.rate_bps(),
        PathRateSample::new(PATH_OPEN_SCORE_BYTES as u64, Duration::from_millis(20),)
            .expect("non-zero sample")
            .rate_bps()
    );

    let mut app_limited = ResponseAckClockRateEvidence::new(started_at);
    let _ = app_limited.observe(
        PATH_OPEN_SCORE_BYTES as u64,
        started_at,
        started_at,
        first_acked_at,
    );
    assert!(
        matches!(
            app_limited.observe(
                PATH_OPEN_SCORE_BYTES as u64,
                first_acked_at - Duration::from_millis(1),
                first_acked_at + Duration::from_millis(1),
                first_acked_at + Duration::from_millis(40),
            ),
            ResponseAckClockRateEvidenceUpdate::Proven {
                sample: None,
                first_window: false,
                ..
            }
        ),
        "one pre-ACK send cannot make a mostly post-ACK window causal"
    );
}

#[test]
fn response_ack_clock_goodput_is_invariant_to_callback_compression() {
    let bytes = 64 * 1024;
    let started_at = Instant::now();
    let mut even = ResponseAckClockRateEvidence::new(started_at);
    let mut compressed = ResponseAckClockRateEvidence::new(started_at);

    // The first exact ACK establishes the per-output time boundary.
    let _ = even.observe(bytes, started_at, started_at, started_at);
    let _ = compressed.observe(bytes, started_at, started_at, started_at);

    let even_step = Duration::from_micros(104_858);
    let _ = even.observe(bytes, started_at, started_at, started_at + even_step);
    let _ = even.observe(bytes, started_at, started_at, started_at + even_step * 2);

    // The same bytes arrive after one long control-queue delay followed by
    // a 1 ms callback tail. The long interval must remain in the ratio.
    let _ = compressed.observe(
        bytes,
        started_at,
        started_at,
        started_at + Duration::from_micros(208_716),
    );
    let _ = compressed.observe(
        bytes,
        started_at,
        started_at,
        started_at + Duration::from_micros(209_716),
    );

    let even_rate = even.goodput_sample().expect("even goodput").rate_bps();
    let compressed_rate = compressed
        .goodput_sample()
        .expect("compressed goodput")
        .rate_bps();
    let relative_error = (even_rate - compressed_rate).abs() / even_rate;
    assert!(relative_error < 0.001, "{even_rate} vs {compressed_rate}");
    assert!(compressed_rate < 5_100_000.0);
}

#[test]
fn response_ack_clock_goodput_keeps_elapsed_for_mixed_assignment_window() {
    let bytes = 64 * 1024;
    let started_at = Instant::now();
    let mut evidence = ResponseAckClockRateEvidence::new(started_at);
    let _ = evidence.observe(bytes, started_at, started_at, started_at);

    let mixed_acked_at = started_at + Duration::from_millis(200);
    let mixed = evidence.observe(
        bytes,
        started_at + Duration::from_millis(1),
        started_at + Duration::from_millis(1),
        mixed_acked_at,
    );
    assert!(matches!(
        mixed,
        ResponseAckClockRateEvidenceUpdate::Proven { sample: None, .. }
    ));
    let mixed_rate = evidence
        .goodput_sample()
        .expect("mixed window still has causal goodput")
        .rate_bps();

    let _ = evidence.observe(
        bytes,
        started_at,
        started_at,
        mixed_acked_at + Duration::from_millis(1),
    );
    let tail_rate = evidence
        .goodput_sample()
        .expect("compressed tail goodput")
        .rate_bps();
    assert!(tail_rate < 5_300_000.0, "compressed tail was {tail_rate}");
    assert!(tail_rate > mixed_rate);
}

#[test]
fn response_ack_clock_credit_requires_fresh_evidence_for_each_stage() {
    let initial = 2 * 1024 * 1024;
    let mut calibration = ResponseAckClockCalibrationState::new(initial, 4 * initial);
    let first_stage_at = calibration.stage_authorized_at;
    let first_growth_at = first_stage_at + Duration::from_millis(100);

    calibration.spent_bytes = initial;
    let sample = ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, 10_000_000.0);
    assert!(calibration.record_ack_clock_sample(
        sample,
        first_stage_at + Duration::from_millis(1),
        first_growth_at,
    ));
    assert_eq!(calibration.credit_limit_bytes, 2 * initial);
    assert_eq!(calibration.calibrated_rate_bps, None);

    calibration.spent_bytes = 2 * initial;
    let stale_sample =
        ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, 20_000_000.0);
    assert!(
        !calibration.record_ack_clock_sample(
            stale_sample,
            first_stage_at + Duration::from_millis(2),
            first_growth_at + Duration::from_millis(10),
        ),
        "residual ACKs from the prior stage cannot pre-authorize another stage"
    );
    assert_eq!(calibration.credit_limit_bytes, 2 * initial);
    let second_growth_at = first_growth_at + Duration::from_millis(20);
    let second_sample =
        ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, 15_000_000.0);
    assert!(calibration.record_ack_clock_sample(
        second_sample,
        first_growth_at + Duration::from_millis(1),
        second_growth_at,
    ));
    assert_eq!(calibration.credit_limit_bytes, 4 * initial);
    assert_eq!(calibration.calibrated_rate_bps, None);

    calibration.spent_bytes = 4 * initial;
    assert!(!calibration.proven);
    let final_sample =
        ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, 18_000_000.0);
    assert!(!calibration.record_ack_clock_sample(
        final_sample,
        second_growth_at + Duration::from_millis(1),
        second_growth_at + Duration::from_millis(20),
    ));
    assert!(calibration.proven);
    assert_rate_close(calibration.calibrated_rate_bps, 15_000_000.0);
}

#[test]
fn response_ack_clock_small_seed_does_not_lower_publication_coverage() {
    let seed = 64 * 1024;
    let coverage_floor = 1024 * 1024;
    let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
        seed,
        8 * 1024 * 1024,
        coverage_floor,
    );
    assert_eq!(calibration.stage_rate_coverage_floor_bytes, coverage_floor);

    let authorized_at = calibration.stage_authorized_at;
    calibration.spent_bytes = seed;
    assert!(calibration.record_ack_clock_sample(
        ack_clock_rate_sample(seed, 10_000_000.0),
        authorized_at + Duration::from_millis(1),
        authorized_at + Duration::from_millis(100),
    ));
    assert_eq!(calibration.credit_limit_bytes, coverage_floor);
    assert_eq!(calibration.stage_rate_evidence_bytes, seed);
    assert_eq!(calibration.stage_rate_sample_count(), 0);

    calibration.spent_bytes = calibration.credit_limit_bytes;
    assert!(calibration.record_ack_clock_sample(
        ack_clock_rate_sample(coverage_floor - seed, 10_000_000.0),
        authorized_at + Duration::from_millis(2),
        authorized_at + Duration::from_millis(200),
    ));
    assert_eq!(calibration.stage_rate_sample_count(), 1);
    assert_rate_close(Some(calibration.stage_rate_samples_bps[0]), 10_000_000.0);
    assert_eq!(calibration.calibrated_rate_bps, None);
    assert!(!calibration.proven);
}

#[test]
fn response_ack_clock_stops_at_robust_rate_before_resource_max() {
    let initial = 2 * 1024 * 1024;
    let resource_max = 32 * initial;
    let mut calibration = ResponseAckClockCalibrationState::new(initial, resource_max);
    let mut authorized_at = calibration.stage_authorized_at;

    for (index, rate_bps) in [40_000_000.0, 60_000_000.0, 50_000_000.0]
        .into_iter()
        .enumerate()
    {
        calibration.spent_bytes = calibration.credit_limit_bytes;
        let acked_at = authorized_at + Duration::from_millis(20);
        let grew = calibration.record_ack_clock_sample(
            ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, rate_bps),
            authorized_at + Duration::from_millis(1),
            acked_at,
        );
        if index < 2 {
            assert!(grew);
            authorized_at = acked_at;
        } else {
            assert!(!grew, "a robust median ends exclusive calibration");
        }
    }

    assert!(calibration.proven);
    assert_eq!(calibration.spent_bytes, 4 * initial);
    assert_eq!(calibration.credit_limit_bytes, 4 * initial);
    assert_eq!(calibration.max_limit_bytes, resource_max);
    assert_rate_close(calibration.calibrated_rate_bps, 50_000_000.0);
}

#[test]
fn response_ack_clock_stage_rate_floor_respects_initial_resource_limit() {
    let zero = ResponseAckClockCalibrationState::new(0, 64 * 1024);
    assert_eq!(zero.credit_limit_bytes, 0);
    assert_eq!(zero.max_limit_bytes, 0);
    assert_eq!(zero.stage_rate_coverage_floor_bytes, 0);

    let exact_floor =
        ResponseAckClockCalibrationState::new(MIN_RATE_SAMPLE_BYTES, MIN_RATE_SAMPLE_BYTES);
    assert_eq!(
        exact_floor.stage_rate_coverage_floor_bytes,
        MIN_RATE_SAMPLE_BYTES
    );

    let odd_initial = 2 * MIN_RATE_SAMPLE_BYTES + 1;
    let odd = ResponseAckClockCalibrationState::new(odd_initial, odd_initial);
    assert_eq!(odd.stage_rate_coverage_floor_bytes, odd_initial.div_ceil(2));

    let default = ResponseAckClockCalibrationState::new(2 * 1024 * 1024, 64 * 1024 * 1024);
    assert_eq!(default.stage_rate_coverage_floor_bytes, 1024 * 1024);

    let clamped = ResponseAckClockCalibrationState::new(4 * 1024 * 1024, 1024 * 1024);
    assert_eq!(clamped.credit_limit_bytes, 1024 * 1024);
    assert_eq!(clamped.max_limit_bytes, 1024 * 1024);
    assert_eq!(clamped.stage_rate_coverage_floor_bytes, 512 * 1024);
}

#[test]
fn response_ack_clock_stage_rate_aggregates_prefull_windows_and_terminal_tail() {
    let initial = 2 * 1024 * 1024;
    let mut calibration = ResponseAckClockCalibrationState::new(initial, 2 * initial);
    let stage_authorized_at = calibration.stage_authorized_at;
    assert_eq!(calibration.stage_rate_coverage_floor_bytes, 1024 * 1024);

    calibration.spent_bytes = initial - 64 * 1024;
    let first = ack_clock_rate_sample(512 * 1024, 80_000_000.0);
    let second = ack_clock_rate_sample(512 * 1024, 40_000_000.0);
    assert!(!calibration.record_ack_clock_sample(
        first,
        stage_authorized_at + Duration::from_millis(1),
        stage_authorized_at + Duration::from_millis(20),
    ));
    assert!(!calibration.record_ack_clock_sample(
        second,
        stage_authorized_at + Duration::from_millis(2),
        stage_authorized_at + Duration::from_millis(40),
    ));

    calibration.spent_bytes = initial;
    let tail = ack_clock_rate_sample(64 * 1024, 4_000_000_000.0);
    assert!(calibration.record_ack_clock_sample(
        tail,
        stage_authorized_at + Duration::from_millis(3),
        stage_authorized_at + Duration::from_millis(50),
    ));

    let aggregate_bytes = first.bytes() + second.bytes() + tail.bytes();
    let aggregate_elapsed = first
        .elapsed()
        .saturating_add(second.elapsed())
        .saturating_add(tail.elapsed());
    let expected_rate = aggregate_bytes as f64 * 8.0 / aggregate_elapsed.as_secs_f64();
    assert_eq!(calibration.stage_rate_sample_count, 1);
    assert_rate_close(Some(calibration.stage_rate_samples_bps[0]), expected_rate);
    assert!(calibration.stage_rate_samples_bps[0] < 100_000_000.0);
    assert_eq!(calibration.stage_rate_evidence_bytes, 0);
    assert_eq!(calibration.stage_rate_evidence_elapsed, Duration::ZERO);
}

#[test]
fn response_ack_clock_stage_rate_is_invariant_to_submillisecond_ack_partitioning() {
    let initial = 2 * 1024 * 1024;
    let mut partitioned = ResponseAckClockCalibrationState::new(initial, 2 * initial);
    let partitioned_stage_at = partitioned.stage_authorized_at;
    partitioned.spent_bytes = initial - 1;
    for index in 0..3 {
        let sample = PathRateSample::new(256 * 1024, Duration::from_micros(250))
            .expect("partitioned rate sample");
        assert!(!partitioned.record_ack_clock_sample(
            sample,
            partitioned_stage_at + Duration::from_millis(index + 1),
            partitioned_stage_at + Duration::from_millis(index + 2),
        ));
    }
    partitioned.spent_bytes = initial;
    let final_partition = PathRateSample::new(256 * 1024, Duration::from_micros(250))
        .expect("final partitioned rate sample");
    assert!(partitioned.record_ack_clock_sample(
        final_partition,
        partitioned_stage_at + Duration::from_millis(4),
        partitioned_stage_at + Duration::from_millis(5),
    ));

    let mut combined = ResponseAckClockCalibrationState::new(initial, 2 * initial);
    let combined_stage_at = combined.stage_authorized_at;
    combined.spent_bytes = initial;
    let combined_sample =
        PathRateSample::new(1024 * 1024, Duration::from_millis(1)).expect("combined rate sample");
    assert!(combined.record_ack_clock_sample(
        combined_sample,
        combined_stage_at + Duration::from_millis(1),
        combined_stage_at + Duration::from_millis(2),
    ));

    assert_rate_close(
        Some(partitioned.stage_rate_samples_bps[0]),
        combined.stage_rate_samples_bps[0],
    );
    assert_rate_close(Some(partitioned.stage_rate_samples_bps[0]), 8_388_608_000.0);
}

#[test]
fn response_ack_clock_full_stage_waits_for_representative_coverage() {
    let initial = 2 * 1024 * 1024;
    let mut calibration = ResponseAckClockCalibrationState::new(initial, initial);
    let stage_authorized_at = calibration.stage_authorized_at;
    calibration.spent_bytes = initial;

    let tail = ack_clock_rate_sample(64 * 1024, 1_000_000.0);
    assert!(!calibration.record_ack_clock_sample(
        tail,
        stage_authorized_at + Duration::from_millis(1),
        stage_authorized_at + Duration::from_millis(100),
    ));

    assert!(!calibration.proven);
    assert_eq!(calibration.stage_rate_sample_count, 0);
    assert_eq!(calibration.calibrated_rate_bps, None);
    assert_eq!(calibration.stage_rate_evidence_bytes, tail.bytes());
    assert_eq!(calibration.stage_rate_evidence_elapsed, tail.elapsed());

    let rest = ack_clock_rate_sample(960 * 1024, 1_000_000.0);
    assert!(!calibration.record_ack_clock_sample(
        rest,
        stage_authorized_at + Duration::from_millis(2),
        stage_authorized_at + Duration::from_millis(200),
    ));
    assert!(calibration.proven);
    assert_eq!(calibration.stage_rate_sample_count, 1);
    assert_eq!(calibration.stage_rate_evidence_bytes, 0);
}

#[test]
fn response_ack_clock_reachability_topup_preserves_strict_stage_evidence() {
    let initial = 512 * 1024;
    let coverage_floor = 1024 * 1024;
    let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
        initial,
        4 * initial,
        coverage_floor,
    );
    let first_stage_at = calibration.stage_authorized_at;
    calibration.spent_bytes = initial;

    let uncovered = ack_clock_rate_sample(64 * 1024, 40_000_000.0);
    let second_stage_at = first_stage_at + Duration::from_millis(100);
    assert!(calibration.record_ack_clock_sample(
        uncovered,
        first_stage_at + Duration::from_millis(1),
        second_stage_at,
    ));
    assert_eq!(calibration.stage_rate_sample_count, 0);
    assert_eq!(calibration.stage_rate_evidence_bytes, 64 * 1024);
    assert_eq!(calibration.stage_authorized_spent_bytes, 0);
    assert_eq!(calibration.stage_credit_bytes(), coverage_floor);

    calibration.spent_bytes = calibration.credit_limit_bytes;
    let still_seed = ack_clock_rate_sample(coverage_floor - 64 * 1024, 40_000_000.0);
    assert!(calibration.record_ack_clock_sample(
        still_seed,
        second_stage_at + Duration::from_millis(1),
        second_stage_at + Duration::from_millis(40),
    ));
    assert_eq!(calibration.stage_rate_sample_count, 1);
    assert_rate_close(Some(calibration.stage_rate_samples_bps[0]), 40_000_000.0);
    assert!(!calibration.proven);
    assert_eq!(calibration.stage_rate_evidence_bytes, 0);
    assert_eq!(calibration.stage_rate_evidence_elapsed, Duration::ZERO);
}

#[test]
fn response_ack_clock_stage_reserves_capacity_for_clock_establishment() {
    let start = Instant::now();
    let seed = 512 * 1024;
    let coverage_floor = 1024 * 1024;
    let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
        seed,
        8 * 1024 * 1024,
        coverage_floor,
    );
    calibration.stage_authorized_at = start;
    calibration.spent_bytes = seed;
    let mut evidence = ResponseAckClockRateEvidence::new(start);

    let first_ack = start + Duration::from_millis(100);
    assert!(apply_ack_clock_evidence_update(
        &mut calibration,
        evidence.observe(
            PATH_OPEN_SCORE_BYTES as u64,
            start + Duration::from_millis(1),
            start + Duration::from_millis(1),
            first_ack,
        ),
        first_ack,
    ));
    assert_eq!(
        calibration.stage_rate_ineligible_bytes,
        PATH_OPEN_SCORE_BYTES as u64
    );
    assert_eq!(calibration.stage_strict_capacity_bytes(), coverage_floor);
    assert_eq!(calibration.stage_authorized_spent_bytes, 0);

    calibration.spent_bytes = calibration.credit_limit_bytes;
    let second_ack = start + Duration::from_millis(200);
    assert!(apply_ack_clock_evidence_update(
        &mut calibration,
        evidence.observe(
            PATH_OPEN_SCORE_BYTES as u64,
            start + Duration::from_millis(110),
            start + Duration::from_millis(110),
            second_ack,
        ),
        second_ack,
    ));
    assert_eq!(
        calibration.stage_rate_ineligible_bytes,
        2 * PATH_OPEN_SCORE_BYTES as u64
    );
    assert!(calibration.stage_strict_capacity_bytes() >= coverage_floor);

    calibration.spent_bytes = calibration.credit_limit_bytes;
    let third_ack = start + Duration::from_millis(300);
    assert!(apply_ack_clock_evidence_update(
        &mut calibration,
        evidence.observe(
            coverage_floor,
            start + Duration::from_millis(150),
            start + Duration::from_millis(150),
            third_ack,
        ),
        third_ack,
    ));
    assert_eq!(calibration.stage_rate_sample_count(), 1);
    assert_eq!(calibration.stage_rate_ineligible_bytes, 0);
    assert_eq!(
        calibration.stage_authorized_spent_bytes,
        calibration.spent_bytes
    );
}

#[test]
fn response_ack_clock_coalesced_warmup_can_exceed_one_floor() {
    let start = Instant::now();
    let coverage_floor = 1024 * 1024;
    let initial = 2 * coverage_floor;
    let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
        initial,
        8 * coverage_floor,
        coverage_floor,
    );
    calibration.stage_authorized_at = start;
    calibration.spent_bytes = initial;
    let mut evidence = ResponseAckClockRateEvidence::new(start);
    let warmup_bytes = coverage_floor + PATH_OPEN_SCORE_BYTES as u64;
    let acked_at = start + Duration::from_millis(100);

    assert!(apply_ack_clock_evidence_update(
        &mut calibration,
        evidence.observe(
            warmup_bytes,
            start + Duration::from_millis(1),
            start + Duration::from_millis(2),
            acked_at,
        ),
        acked_at,
    ));

    assert_eq!(calibration.stage_rate_ineligible_bytes, warmup_bytes);
    assert!(calibration.credit_limit_bytes > initial);
    assert!(calibration.stage_strict_capacity_bytes() >= coverage_floor);
    assert!(!calibration.proven);
}

#[test]
fn response_ack_clock_mixed_window_charges_only_fresh_stage_bytes() {
    let start = Instant::now();
    let authorized_at = start + Duration::from_millis(100);
    let coverage_floor = 1024 * 1024;
    let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
        2 * coverage_floor,
        4 * coverage_floor,
        coverage_floor,
    );
    calibration.stage_authorized_at = authorized_at;
    calibration.spent_bytes = coverage_floor;
    let total_bytes = 2 * PATH_OPEN_SCORE_BYTES as u64;
    let fresh_bytes = PATH_OPEN_SCORE_BYTES as u64;
    let sample = ack_clock_rate_sample(total_bytes, 20_000_000.0);

    assert!(!calibration.record_ack_clock_window(
        Some(sample),
        total_bytes,
        fresh_bytes,
        authorized_at - Duration::from_millis(1),
        authorized_at + Duration::from_millis(100),
    ));

    assert_eq!(calibration.stage_rate_evidence_bytes, 0);
    assert_eq!(calibration.stage_rate_ineligible_bytes, fresh_bytes);
    assert!(
        calibration.stage_rate_evidence_bytes + calibration.stage_rate_ineligible_bytes
            <= calibration.stage_credit_bytes()
    );
}

#[test]
fn response_ack_clock_drained_seed_restores_reachable_credit_or_terminates_at_cap() {
    let seed = 512 * 1024;
    let coverage_floor = 1024 * 1024;
    let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
        seed,
        8 * coverage_floor,
        coverage_floor,
    );
    calibration.spent_bytes = seed;

    assert!(calibration.advance_drained_stage(Instant::now()));
    assert_eq!(calibration.stage_rate_ineligible_bytes, seed);
    assert_eq!(calibration.stage_strict_capacity_bytes(), coverage_floor);
    assert!(!calibration.proven);

    let mut capped =
        ResponseAckClockCalibrationState::new_with_rate_coverage_floor(seed, seed, coverage_floor);
    capped.spent_bytes = seed;
    assert!(!capped.advance_drained_stage(Instant::now()));
    assert!(capped.proven);
    assert_eq!(capped.calibrated_rate_bps, None);
}

#[test]
fn response_ack_clock_drain_finalizes_prefull_representative_evidence() {
    let initial = 2 * 1024 * 1024;
    let coverage_floor = 1024 * 1024;
    let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
        initial,
        2 * initial,
        coverage_floor,
    );
    let authorized_at = calibration.stage_authorized_at;
    calibration.spent_bytes = initial - PATH_OPEN_SCORE_BYTES as u64;

    assert!(!calibration.record_ack_clock_sample(
        ack_clock_rate_sample(coverage_floor, 40_000_000.0),
        authorized_at + Duration::from_millis(1),
        authorized_at + Duration::from_millis(100),
    ));
    assert_eq!(calibration.stage_rate_evidence_bytes, coverage_floor);

    calibration.spent_bytes = initial;
    assert!(calibration.advance_drained_stage(authorized_at + Duration::from_millis(200)));
    assert_eq!(calibration.stage_rate_sample_count(), 1);
    assert_rate_close(Some(calibration.stage_rate_samples_bps[0]), 40_000_000.0);
    assert_eq!(calibration.stage_rate_evidence_bytes, 0);
    assert_eq!(calibration.stage_rate_ineligible_bytes, 0);
}

#[test]
fn response_ack_clock_stage_transition_waits_for_coverage_and_rejects_stale_windows() {
    let initial = 2 * 1024 * 1024;
    let mut calibration = ResponseAckClockCalibrationState::new(initial, 2 * initial);
    let first_stage_at = calibration.stage_authorized_at;
    calibration.spent_bytes = initial - 64 * 1024;
    let partial = ack_clock_rate_sample(512 * 1024, 20_000_000.0);
    assert!(!calibration.record_ack_clock_sample(
        partial,
        first_stage_at + Duration::from_millis(1),
        first_stage_at + Duration::from_millis(20),
    ));

    calibration.spent_bytes = initial;
    let first_growth_at = first_stage_at + Duration::from_millis(100);
    let tail = ack_clock_rate_sample(64 * 1024, 20_000_000.0);
    assert!(!calibration.record_ack_clock_sample(
        tail,
        first_stage_at + Duration::from_millis(2),
        first_growth_at,
    ));
    assert_eq!(calibration.stage_rate_sample_count, 0);
    assert_eq!(calibration.stage_rate_evidence_bytes, 576 * 1024);

    let representative_tail = ack_clock_rate_sample(512 * 1024, 20_000_000.0);
    assert!(calibration.record_ack_clock_sample(
        representative_tail,
        first_stage_at + Duration::from_millis(3),
        first_growth_at + Duration::from_millis(10),
    ));
    assert_eq!(calibration.stage_rate_sample_count, 1);
    assert_eq!(calibration.stage_rate_evidence_bytes, 0);

    calibration.spent_bytes = calibration.credit_limit_bytes;
    let stale = ack_clock_rate_sample(1024 * 1024, 20_000_000.0);
    assert!(!calibration.record_ack_clock_sample(
        stale,
        first_stage_at + Duration::from_millis(4),
        first_growth_at + Duration::from_millis(20),
    ));
    assert_eq!(calibration.credit_limit_bytes, 2 * initial);
    assert!(!calibration.proven);
    assert_eq!(calibration.stage_rate_evidence_bytes, 0);

    calibration.spent_bytes = initial;
    let fresh_partial = ack_clock_rate_sample(512 * 1024, 20_000_000.0);
    assert!(!calibration.record_ack_clock_sample(
        fresh_partial,
        first_growth_at + Duration::from_millis(11),
        first_growth_at + Duration::from_millis(40),
    ));
    assert_eq!(calibration.stage_rate_evidence_bytes, 512 * 1024);
    calibration.retire();
    assert_eq!(calibration.stage_rate_evidence_bytes, 0);
    assert_eq!(calibration.stage_rate_evidence_elapsed, Duration::ZERO);
}

#[test]
fn response_ack_clock_rate_uses_stage_median_instead_of_compressed_ack_peak() {
    let initial = 2 * 1024 * 1024;
    let mut calibration = ResponseAckClockCalibrationState::new(initial, 32 * initial);
    let stage_samples = [65_000_000.0, 1_740_000_000.0, 73_000_000.0];

    for (index, sample_bps) in stage_samples.into_iter().enumerate() {
        calibration.spent_bytes = calibration.credit_limit_bytes;
        let stage_authorized_at = calibration.stage_authorized_at;
        let sample = ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, sample_bps);
        let grew = calibration.record_ack_clock_sample(
            sample,
            stage_authorized_at + Duration::from_millis(1),
            stage_authorized_at + Duration::from_millis(10),
        );
        assert_eq!(grew, index < 2);
    }

    assert!(calibration.proven);
    assert_eq!(calibration.credit_limit_bytes, 4 * initial);
    assert_rate_close(calibration.calibrated_rate_bps, 73_000_000.0);
}

#[test]
fn response_ack_clock_stage_median_matches_v17_stable_path_samples() {
    let initial = 2 * 1024 * 1024;
    let mut calibration = ResponseAckClockCalibrationState::new(initial, 32 * initial);
    for sample_bps in [61_220_000.0, 16_150_000.0, 104_389_000.0] {
        calibration.spent_bytes = calibration.credit_limit_bytes;
        let stage_authorized_at = calibration.stage_authorized_at;
        let sample = ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, sample_bps);
        let _ = calibration.record_ack_clock_sample(
            sample,
            stage_authorized_at + Duration::from_millis(1),
            stage_authorized_at + Duration::from_millis(10),
        );
    }
    assert!(calibration.proven);
    assert_rate_close(calibration.calibrated_rate_bps, 61_220_000.0);
}

#[test]
fn response_ack_clock_stage_ring_is_not_a_monotonic_peak_filter() {
    let mut calibration = ResponseAckClockCalibrationState::new(1, 1);
    for sample_bps in [10.0, 20.0, 30.0, 40.0, 50.0] {
        calibration.record_stage_rate_sample(sample_bps);
    }
    assert_eq!(calibration.calibrated_rate_bps, Some(30.0));
    calibration.record_stage_rate_sample(100.0);
    assert_eq!(calibration.calibrated_rate_bps, Some(40.0));
    for sample_bps in [5.0, 1.0, 2.0] {
        calibration.record_stage_rate_sample(sample_bps);
    }
    assert_eq!(calibration.calibrated_rate_bps, Some(5.0));
}

#[test]
fn retired_response_ack_clock_state_cannot_publish_later_generic_acks() {
    let mut calibration = ResponseAckClockCalibrationState::new(1024, 4096);
    calibration.spent_bytes = 1024;
    calibration.retire();
    let stage_authorized_at = calibration.stage_authorized_at;
    let sample = ack_clock_rate_sample(MIN_RATE_SAMPLE_BYTES, 7_000_000_000.0);

    assert!(!calibration.record_ack_clock_sample(
        sample,
        stage_authorized_at + Duration::from_millis(1),
        stage_authorized_at + Duration::from_millis(10),
    ));
    assert_eq!(calibration.calibrated_rate_bps, None);
    assert!(!calibration.proven);
    assert_eq!(calibration.credit_limit_bytes, calibration.spent_bytes);
    assert_eq!(calibration.max_limit_bytes, calibration.spent_bytes);
}
