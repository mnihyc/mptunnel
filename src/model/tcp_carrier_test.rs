use super::*;
use crate::model::ack_clock::{
    reliable_ack_clock_measurement_ceiling_bytes, reliable_data_ack_rate_coverage_floor_bytes,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, reliable_path_startup_sample_limit_bytes,
    reliable_product_measurement_session_envelope_bytes,
};

fn geometry() -> TcpCarrierValidationGeometry {
    TcpCarrierValidationGeometry {
        startup_coverage_bytes: 500,
        rate_window_bytes: 1_000,
        cohort_coverage_bytes: 4_000,
        candidate_work_limit_bytes: 4_500,
    }
}

fn zero_work() -> TcpCarrierCandidateWorkState {
    TcpCarrierCandidateWorkState::default()
}

fn install_reference(state: &mut TcpCarrierValidationState) {
    assert_eq!(
        state.observe_cohort(
            4_000,
            8_000,
            0,
            Duration::from_millis(40),
            Duration::from_millis(40),
        ),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        state.advance_at_causal_boundary(zero_work()),
        TcpCarrierValidationUpdate::Advanced(TcpCarrierValidationPhase::CandidateStartup)
    );
}

fn complete_startup(state: &mut TcpCarrierValidationState) {
    assert_eq!(state.candidate_assignment_credit_bytes(), 500);
    assert_eq!(
        state.record_candidate_assignment(500),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(state.candidate_assignment_credit_bytes(), 0);
    assert_eq!(
        state.observe_candidate_resolution(500, 500),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        state.advance_at_causal_boundary(zero_work()),
        TcpCarrierValidationUpdate::Advanced(TcpCarrierValidationPhase::Assisted)
    );
}

fn install_assisted(
    state: &mut TcpCarrierValidationState,
    aggregate_bytes: u64,
    elapsed: Duration,
) {
    assert_eq!(state.candidate_assignment_credit_bytes(), 4_000);
    assert_eq!(
        state.record_candidate_assignment(4_000),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        state.observe_candidate_resolution(4_000, 4_000),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        state.observe_cohort(4_000, aggregate_bytes, 4_000, elapsed, elapsed),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        state.advance_at_causal_boundary(zero_work()),
        TcpCarrierValidationUpdate::Advanced(TcpCarrierValidationPhase::Confirmation)
    );
}

fn install_confirmation(
    state: &mut TcpCarrierValidationState,
    aggregate_bytes: u64,
    elapsed: Duration,
) -> TcpCarrierValidationResult {
    assert_eq!(
        state.observe_cohort(4_000, aggregate_bytes, 0, elapsed, elapsed),
        TcpCarrierValidationUpdate::Pending
    );
    let TcpCarrierValidationUpdate::Settled(result) = state.advance_at_causal_boundary(zero_work())
    else {
        panic!("complete confirmation must settle validation");
    };
    result
}

#[test]
fn geometry_reuses_existing_directional_coverage_and_resource_bounds() {
    let mux_limits = MuxLimits::default();
    let frozen = tcp_carrier_validation_geometry([45_000_000], mux_limits)
        .expect("measurable ordinary carrier set");
    let rate_window = reliable_data_ack_rate_coverage_floor_bytes(mux_limits);
    let envelope = reliable_product_measurement_session_envelope_bytes(mux_limits);
    assert_eq!(
        frozen.startup_coverage_bytes,
        reliable_path_startup_sample_limit_bytes(mux_limits)
    );
    assert_eq!(frozen.rate_window_bytes, rate_window);
    assert_eq!(frozen.cohort_coverage_bytes % rate_window, 0);
    assert!(frozen.cohort_coverage_bytes >= 45_000_000);
    assert!(frozen.cohort_coverage_bytes <= envelope);
    assert_eq!(
        frozen.candidate_work_limit_bytes,
        frozen.startup_coverage_bytes + frozen.cohort_coverage_bytes
    );
}

#[test]
fn invalid_overflown_or_unalignable_geometry_is_unavailable() {
    let mux_limits = MuxLimits::default();
    assert!(tcp_carrier_validation_geometry([], mux_limits).is_none());
    assert!(tcp_carrier_validation_geometry([0], mux_limits).is_none());
    assert!(tcp_carrier_validation_geometry([u64::MAX, 1], mux_limits).is_none());

    let below_rate_evidence = MuxLimits {
        max_path_flight_bytes: PATH_OPEN_SCORE_BYTES - 1,
        ..mux_limits
    };
    assert_eq!(
        reliable_ack_clock_measurement_ceiling_bytes(below_rate_evidence),
        0
    );
    assert!(tcp_carrier_validation_geometry([1], below_rate_evidence).is_none());

    let nondivisible_envelope = MuxLimits {
        max_stream_window_bytes: 30_001,
        max_repair_bytes: 30_001,
        max_reorder_bytes: 30_001,
        max_path_flight_bytes: 30_001,
        ..mux_limits
    };
    assert!(
        tcp_carrier_validation_geometry([30_001], nondivisible_envelope).is_none(),
        "alignment must not reduce coverage below the ordinary pipe"
    );
}

#[test]
fn exact_rate_order_handles_values_that_cross_multiplication_cannot() {
    let faster =
        ProductServiceRate::new(u64::MAX, Duration::from_nanos(u64::MAX - 1), Duration::ZERO)
            .expect("faster rate");
    let slower =
        ProductServiceRate::new(u64::MAX - 1, Duration::from_nanos(u64::MAX), Duration::ZERO)
            .expect("slower rate");
    assert_eq!(faster.cmp_exact(slower), Ordering::Greater);
    assert_eq!(slower.cmp_exact(faster), Ordering::Less);
}

#[test]
fn ordered_whole_cohort_validation_retains_only_strict_dual_gain() {
    let mut state = TcpCarrierValidationState::new(geometry());
    install_reference(&mut state);
    complete_startup(&mut state);
    install_assisted(&mut state, 8_000, Duration::from_millis(20));
    assert_eq!(
        install_confirmation(&mut state, 8_000, Duration::from_millis(40)),
        TcpCarrierValidationResult::Retain
    );
    assert_eq!(state.result(), Some(TcpCarrierValidationResult::Retain));
    assert_eq!(
        state.withdraw(),
        TcpCarrierValidationUpdate::Settled(TcpCarrierValidationResult::Retain),
        "a serialized result is immutable"
    );
}

#[test]
fn target_redistribution_without_aggregate_gain_is_no_gain() {
    let mut state = TcpCarrierValidationState::new(geometry());
    install_reference(&mut state);
    complete_startup(&mut state);
    install_assisted(&mut state, 4_000, Duration::from_millis(20));
    assert_eq!(
        install_confirmation(&mut state, 8_000, Duration::from_millis(40)),
        TcpCarrierValidationResult::NoGain
    );
}

#[test]
fn assisted_service_must_beat_both_adjacent_controls() {
    let mut state = TcpCarrierValidationState::new(geometry());
    install_reference(&mut state);
    complete_startup(&mut state);
    install_assisted(&mut state, 8_000, Duration::from_millis(20));
    assert_eq!(
        install_confirmation(&mut state, 16_000, Duration::from_millis(20)),
        TcpCarrierValidationResult::NoGain
    );
}

#[test]
fn startup_assignment_is_exact_and_qualified_before_assisted_service() {
    let mut over_assigned = TcpCarrierValidationState::new(geometry());
    install_reference(&mut over_assigned);
    assert_eq!(
        over_assigned.record_candidate_assignment(501),
        TcpCarrierValidationUpdate::Settled(TcpCarrierValidationResult::Withdrawn)
    );

    let mut ambiguous = TcpCarrierValidationState::new(geometry());
    install_reference(&mut ambiguous);
    assert_eq!(
        ambiguous.record_candidate_assignment(500),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        ambiguous.observe_candidate_resolution(500, 499),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        ambiguous.advance_at_causal_boundary(zero_work()),
        TcpCarrierValidationUpdate::Settled(TcpCarrierValidationResult::Withdrawn)
    );
}

#[test]
fn assisted_assignment_is_bounded_and_needs_one_qualified_rate_window() {
    let mut over_budget = TcpCarrierValidationState::new(geometry());
    install_reference(&mut over_budget);
    complete_startup(&mut over_budget);
    assert_eq!(
        over_budget.record_candidate_assignment(4_001),
        TcpCarrierValidationUpdate::Settled(TcpCarrierValidationResult::Withdrawn)
    );

    let mut insufficient = TcpCarrierValidationState::new(geometry());
    install_reference(&mut insufficient);
    complete_startup(&mut insufficient);
    assert_eq!(
        insufficient.record_candidate_assignment(999),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        insufficient.observe_candidate_resolution(999, 999),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        insufficient.observe_cohort(
            4_000,
            8_000,
            999,
            Duration::from_millis(20),
            Duration::from_millis(20),
        ),
        TcpCarrierValidationUpdate::Settled(TcpCarrierValidationResult::Withdrawn)
    );
}

#[test]
fn assisted_cohort_closure_seals_assignment_but_drains_existing_flight() {
    let mut state = TcpCarrierValidationState::new(geometry());
    install_reference(&mut state);
    complete_startup(&mut state);
    assert_eq!(
        state.record_candidate_assignment(1_000),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        state.observe_candidate_resolution(1_000, 1_000),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        state.observe_cohort(
            4_000,
            8_000,
            1_000,
            Duration::from_millis(20),
            Duration::from_millis(20),
        ),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(state.candidate_assignment_credit_bytes(), 0);
    assert_eq!(
        state.record_candidate_assignment(1),
        TcpCarrierValidationUpdate::Settled(TcpCarrierValidationResult::Withdrawn),
        "new candidate assignment is sealed at cohort closure"
    );

    let mut draining = TcpCarrierValidationState::new(geometry());
    install_reference(&mut draining);
    complete_startup(&mut draining);
    assert_eq!(
        draining.record_candidate_assignment(2_000),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        draining.observe_candidate_resolution(1_000, 1_000),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        draining.observe_cohort(
            4_000,
            8_000,
            1_000,
            Duration::from_millis(20),
            Duration::from_millis(20),
        ),
        TcpCarrierValidationUpdate::Pending,
        "the cohort closes over only releases processed by its closing ACK"
    );
    assert_eq!(draining.candidate_assignment_credit_bytes(), 0);
    assert_eq!(
        draining.observe_candidate_resolution(1_000, 1_000),
        TcpCarrierValidationUpdate::Pending,
        "already-assigned flight may resolve after cohort closure"
    );
    assert_eq!(
        draining.advance_at_causal_boundary(zero_work()),
        TcpCarrierValidationUpdate::Advanced(TcpCarrierValidationPhase::Confirmation)
    );
}

#[test]
fn malformed_cohort_or_phase_contamination_withdraws() {
    let mut incomplete = TcpCarrierValidationState::new(geometry());
    assert_eq!(
        incomplete.observe_cohort(
            3_999,
            8_000,
            0,
            Duration::from_millis(40),
            Duration::from_millis(40),
        ),
        TcpCarrierValidationUpdate::Settled(TcpCarrierValidationResult::Withdrawn)
    );

    let mut attributed_reference = TcpCarrierValidationState::new(geometry());
    assert_eq!(
        attributed_reference.observe_cohort(
            4_000,
            8_000,
            1,
            Duration::from_millis(40),
            Duration::from_millis(40),
        ),
        TcpCarrierValidationUpdate::Settled(TcpCarrierValidationResult::Withdrawn)
    );

    let mut zero_elapsed = TcpCarrierValidationState::new(geometry());
    assert_eq!(
        zero_elapsed.observe_cohort(4_000, 8_000, 0, Duration::ZERO, Duration::ZERO),
        TcpCarrierValidationUpdate::Settled(TcpCarrierValidationResult::Withdrawn)
    );
}

#[test]
fn confirmation_requires_zero_candidate_product_state() {
    let mut state = TcpCarrierValidationState::new(geometry());
    install_reference(&mut state);
    complete_startup(&mut state);
    assert_eq!(
        state.record_candidate_assignment(4_000),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        state.observe_candidate_resolution(4_000, 4_000),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        state.observe_cohort(
            4_000,
            8_000,
            4_000,
            Duration::from_millis(20),
            Duration::from_millis(20),
        ),
        TcpCarrierValidationUpdate::Pending
    );
    assert_eq!(
        state.advance_at_causal_boundary(TcpCarrierCandidateWorkState {
            recovery_bytes: 1,
            ..zero_work()
        }),
        TcpCarrierValidationUpdate::Settled(TcpCarrierValidationResult::Withdrawn)
    );
}

#[test]
fn invalid_order_cannot_be_ignored_or_retried_inside_the_state() {
    let mut state = TcpCarrierValidationState::new(geometry());
    assert_eq!(
        state.record_candidate_assignment(1),
        TcpCarrierValidationUpdate::Settled(TcpCarrierValidationResult::Withdrawn)
    );
    assert_eq!(
        state.advance_at_causal_boundary(zero_work()),
        TcpCarrierValidationUpdate::Settled(TcpCarrierValidationResult::Withdrawn)
    );
    assert_eq!(state.candidate_assignment_credit_bytes(), 0);
}
