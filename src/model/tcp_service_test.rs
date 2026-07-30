use super::*;
use crate::model::path::CarrierPathInstanceId;
use crate::protocol::{AuthNonce, PathId};

fn carrier(path_id: u16, nonce: u8, local_instance: u64) -> TcpServiceCarrierFence {
    TcpServiceCarrierFence {
        accepted: TcpCarrierAcceptedPath {
            path_id: PathId(path_id),
            path_join_nonce: AuthNonce([nonce; 16]),
        },
        local_instance_id: CarrierPathInstanceId::from_raw(local_instance),
        eligibility_generation: 1,
    }
}

fn carrier_group(raw: u64) -> TcpServiceCarrierGroupId {
    TcpServiceCarrierGroupId::from_raw(raw)
}

fn stream(stream_id: u64, horizon_bytes: u64) -> TcpServiceStreamFence {
    TcpServiceStreamFence {
        stream_id: StreamId(stream_id),
        demand_generation: 1,
        attachment_incarnation: 1,
        data_ack_horizon_bytes: horizon_bytes,
    }
}

fn limits() -> TcpServiceValidationLimits {
    TcpServiceValidationLimits {
        max_paths: 4,
        max_streams: 4,
        max_ack_release_records: 16,
        max_window_bytes: 1_000,
        validation_horizon_bytes: 100,
        unproven_flight_bytes: 50,
        data_ack_sample_floor_bytes: 25,
    }
}

fn activate_plan(
    controller: &mut TcpServiceSessionController,
    plan: TcpServiceValidationPlan,
    ack_sequence: u64,
    boundary_at: Instant,
) -> (TcpServiceSenderValidation, TcpServiceWriterLifecycle) {
    let preparation = controller.reserve(plan).expect("valid preparation");
    let fence = preparation.fence().clone();
    let writer_lifecycle = preparation.writer_lifecycle();
    let validation = controller
        .activate(
            preparation,
            TcpServiceBoundary {
                ack_sequence,
                acked_at: boundary_at,
                writer: writer_lifecycle.point(boundary_at),
            },
            boundary_at,
            &fence,
        )
        .expect("valid activation");
    (validation, writer_lifecycle)
}

struct ValidationHarness {
    controller: TcpServiceSessionController,
    validation: TcpServiceSenderValidation,
    writer_lifecycle: TcpServiceWriterLifecycle,
    fence: TcpServiceValidationFence,
    stream: TcpServiceStreamFence,
    accepted: TcpServiceCarrierFence,
    candidate: TcpServiceCarrierFence,
    clock: Instant,
    ack_sequence: u64,
    next_offset: u64,
}

impl ValidationHarness {
    fn new() -> Self {
        Self::with_limits(limits())
    }

    fn with_limits(limits: TcpServiceValidationLimits) -> Self {
        let registered_at = Instant::now();
        let initial_at = registered_at + Duration::from_millis(1);
        let accepted = carrier(1, 11, 101);
        let candidate = carrier(2, 22, 202);
        let stream = stream(7, 100);
        let fence = TcpServiceValidationFence {
            range_generation: 1,
            demand: TcpServiceDemandFence::Local,
            accepted: vec![accepted],
            candidate,
            streams: vec![stream],
        };
        let mut controller =
            TcpServiceSessionController::new(SessionId(9), 4).expect("session controller");
        let preparation = controller
            .reserve(TcpServiceValidationPlan {
                session_id: SessionId(9),
                trial_id: 3,
                direction: PathMetricDirection::ClientToServer,
                carrier_group_id: carrier_group(1),
                fence: fence.clone(),
                limits,
                registered_at,
                absolute_deadline: registered_at + Duration::from_secs(100),
            })
            .expect("valid service validation preparation");
        let writer_lifecycle = preparation.writer_lifecycle();
        let validation = controller
            .activate(
                preparation,
                TcpServiceBoundary {
                    ack_sequence: 1,
                    acked_at: initial_at,
                    writer: writer_lifecycle.point(initial_at),
                },
                initial_at,
                &fence,
            )
            .expect("valid service validation");
        Self {
            controller,
            validation,
            writer_lifecycle,
            fence,
            stream,
            accepted,
            candidate,
            clock: initial_at,
            ack_sequence: 1,
            next_offset: 0,
        }
    }

    fn allocate_range(&mut self, bytes: u64) -> OffsetRange {
        let start = self.next_offset;
        let end = start.checked_add(bytes).expect("test range");
        self.next_offset = end;
        OffsetRange { start, end }
    }

    fn restart_validation(&mut self, trial_id: u64, fence: TcpServiceValidationFence) {
        self.restart_validation_for_group(trial_id, carrier_group(1), fence);
    }

    fn restart_validation_for_group(
        &mut self,
        trial_id: u64,
        carrier_group_id: TcpServiceCarrierGroupId,
        fence: TcpServiceValidationFence,
    ) {
        if self.controller.has_active_lifecycle() {
            let cleanup = self
                .controller
                .finish(&mut self.validation, self.clock, &self.fence)
                .expect("settled validation must finish before replacement");
            assert!(self.controller.complete_cleanup(cleanup));
        }
        let registered_at = self.clock + Duration::from_nanos(1);
        let initial_at = registered_at + Duration::from_nanos(1);
        self.ack_sequence += 1;
        let preparation = self
            .controller
            .reserve(TcpServiceValidationPlan {
                session_id: SessionId(9),
                trial_id,
                direction: PathMetricDirection::ClientToServer,
                carrier_group_id,
                fence: fence.clone(),
                limits: limits(),
                registered_at,
                absolute_deadline: registered_at + Duration::from_secs(100),
            })
            .expect("replacement validation preparation");
        self.writer_lifecycle = preparation.writer_lifecycle();
        self.validation = self
            .controller
            .activate(
                preparation,
                TcpServiceBoundary {
                    ack_sequence: self.ack_sequence,
                    acked_at: initial_at,
                    writer: self.writer_lifecycle.point(initial_at),
                },
                initial_at,
                &fence,
            )
            .expect("replacement validation");
        self.clock = initial_at;
        self.stream = fence.streams[0];
        self.accepted = fence.accepted[0];
        self.candidate = fence.candidate;
        self.fence = fence;
    }

    fn saturate(&mut self) {
        self.clock += Duration::from_millis(1);
        self.controller.observe_saturation(
            &mut self.validation,
            TcpServiceSaturationObservation {
                observed_at: self.writer_lifecycle.point(self.clock),
                accepted_with_original_flight: self.fence.accepted.clone(),
                streams_with_fresh_demand: self.fence.streams.clone(),
                blocked_stream: self.stream,
                blocked_range: OffsetRange {
                    start: self.next_offset,
                    end: self.next_offset + 1,
                },
            },
            self.clock,
            &self.fence,
        );
    }

    fn send_ack(
        &mut self,
        releases: Vec<TcpServiceAckRelease>,
        after: Duration,
    ) -> TcpServiceValidationUpdate {
        self.ack_sequence += 1;
        let latest_commit = releases
            .iter()
            .filter_map(|release| release.committed_at.map(TcpServiceWriterPoint::at))
            .max()
            .unwrap_or(self.clock);
        self.clock = self.clock.max(latest_commit) + after;
        let next_writer_boundary_at = self.clock + Duration::from_nanos(1);
        let update = self.validation.observe_data_ack(
            TcpServiceDataAckEvent {
                sequence: self.ack_sequence,
                stream: self.stream,
                assigned_end: self.next_offset,
                acked_at: self.clock,
                next_writer_boundary: self.writer_lifecycle.point(next_writer_boundary_at),
                releases,
            },
            next_writer_boundary_at,
            &self.fence,
        );
        self.clock = next_writer_boundary_at;
        update
    }

    fn accepted_window(&mut self, ack_after: Duration) {
        self.saturate();
        self.clock += Duration::from_millis(1);
        let range = self.allocate_range(200);
        let release = TcpServiceAckRelease {
            carrier: self.accepted,
            range,
            committed_at: Some(self.writer_lifecycle.point(self.clock)),
            kind: TcpServiceReleaseKind::Original,
            unambiguous: true,
        };
        self.send_ack(vec![release], ack_after);
    }

    fn candidate_commit(&mut self, bytes: u64) -> TcpServiceAckRelease {
        self.clock += Duration::from_millis(1);
        let range = self.allocate_range(bytes);
        let placement = TcpServiceCandidatePlacement {
            stream: self.stream,
            range,
        };
        let TcpServiceCandidateReservationUpdate::Granted(permit) = self
            .validation
            .reserve_candidate_placement(placement, self.clock, &self.fence)
        else {
            panic!("candidate placement permit");
        };
        assert_eq!(
            self.validation.commit_candidate_placement(
                permit,
                self.writer_lifecycle.point(self.clock),
                self.clock,
                &self.fence,
            ),
            TcpServiceValidationUpdate::Pending
        );
        TcpServiceAckRelease {
            carrier: self.candidate,
            range,
            committed_at: Some(self.writer_lifecycle.point(self.clock)),
            kind: TcpServiceReleaseKind::Original,
            unambiguous: true,
        }
    }

    fn readiness(&mut self) {
        assert_eq!(
            self.validation.phase(),
            TcpServiceValidationPhase::Readiness
        );
        for _ in 0..2 {
            assert_eq!(self.validation.candidate_placement_credit_bytes(), 50);
            let candidate = self.candidate_commit(50);
            self.send_ack(vec![candidate], Duration::from_millis(2));
        }
        assert_eq!(
            self.validation.phase(),
            TcpServiceValidationPhase::Comparison {
                completed_windows: 0
            }
        );
    }

    fn comparison_window(&mut self, ack_after: Duration) {
        self.saturate();
        let candidate = self.candidate_commit(50);
        self.clock += Duration::from_millis(1);
        let accepted_range = self.allocate_range(100);
        let accepted = TcpServiceAckRelease {
            carrier: self.accepted,
            range: accepted_range,
            committed_at: Some(self.writer_lifecycle.point(self.clock)),
            kind: TcpServiceReleaseKind::Original,
            unambiguous: true,
        };
        self.send_ack(vec![candidate, accepted], ack_after);
        let candidate = self.candidate_commit(50);
        self.send_ack(vec![candidate], ack_after);
    }

    fn drive_to_post_reference(&mut self, comparison_ack_after: Duration) {
        self.accepted_window(Duration::from_millis(20));
        self.accepted_window(Duration::from_millis(20));
        self.readiness();
        self.comparison_window(comparison_ack_after);
        self.comparison_window(comparison_ack_after);
        assert_eq!(
            self.validation.phase(),
            TcpServiceValidationPhase::PostReference {
                completed_windows: 0
            }
        );
        assert_eq!(self.validation.candidate_placement_credit_bytes(), 0);
    }
}

#[test]
fn six_window_validation_retains_only_strictly_faster_aggregate_service() {
    let mut harness = ValidationHarness::new();
    harness.drive_to_post_reference(Duration::from_millis(1));
    harness.accepted_window(Duration::from_millis(20));
    harness.accepted_window(Duration::from_millis(20));

    let outcome = harness
        .validation
        .outcome()
        .expect("settled outcome")
        .clone();
    assert_eq!(outcome.result, TcpCarrierValidationResult::Retain);
    assert_eq!(outcome.withdrawal_reason, None);
    assert_eq!(outcome.no_gain_suppression, None);
    assert_eq!(
        harness.validation.phase(),
        TcpServiceValidationPhase::Settled
    );
    let cleanup = harness
        .controller
        .finish(&mut harness.validation, harness.clock, &harness.fence)
        .expect("settled validation enters cleanup");
    assert_eq!(cleanup.outcome(), &outcome);
    assert!(harness.controller.has_active_lifecycle());
    assert_eq!(
        harness
            .controller
            .reserve(TcpServiceValidationPlan {
                session_id: SessionId(9),
                trial_id: 4,
                direction: PathMetricDirection::ClientToServer,
                carrier_group_id: carrier_group(1),
                fence: harness.fence.clone(),
                limits: limits(),
                registered_at: harness.clock + Duration::from_nanos(1),
                absolute_deadline: harness.clock + Duration::from_secs(1),
            })
            .expect_err("observer cleanup must precede a later reservation"),
        TcpServiceValidationError::ValidationInProgress
    );
    assert_eq!(harness.controller.pending_cleanup(), Some(cleanup.clone()));
    assert!(harness.controller.complete_cleanup(cleanup));

    let mut stale_finish = ValidationHarness::new();
    stale_finish.drive_to_post_reference(Duration::from_millis(1));
    stale_finish.accepted_window(Duration::from_millis(20));
    stale_finish.accepted_window(Duration::from_millis(20));
    let mut changed = stale_finish.fence.clone();
    changed.accepted[0].eligibility_generation += 1;
    let cleanup = stale_finish
        .controller
        .finish(&mut stale_finish.validation, stale_finish.clock, &changed)
        .expect("finish produces current authority");
    assert_eq!(
        cleanup.outcome().result,
        TcpCarrierValidationResult::Withdrawn
    );
    assert_eq!(
        cleanup.outcome().withdrawal_reason,
        Some(TcpServiceWithdrawalReason::FenceChanged)
    );
    assert!(stale_finish.controller.complete_cleanup(cleanup));
}

#[test]
fn no_gain_records_exact_reference_suppression_without_a_percentage_margin() {
    let mut harness = ValidationHarness::new();
    harness.drive_to_post_reference(Duration::from_millis(20));
    harness.accepted_window(Duration::from_millis(20));
    harness.accepted_window(Duration::from_millis(20));

    let outcome = harness
        .validation
        .outcome()
        .expect("settled outcome")
        .clone();
    assert_eq!(outcome.result, TcpCarrierValidationResult::NoGain);
    let cleanup = harness
        .controller
        .finish(&mut harness.validation, harness.clock, &harness.fence)
        .expect("no-gain settlement enters cleanup");
    assert_eq!(cleanup.outcome(), &outcome);
    assert!(harness.controller.complete_cleanup(cleanup));
    let suppression = outcome
        .no_gain_suppression
        .as_ref()
        .expect("no-gain suppression");
    let same_identity = harness.fence.suppression_identity();
    assert!(!suppression.permits(&same_identity, suppression.rejected_reference_range));

    let faster_disjoint = TcpServiceReferenceRange::new(
        TcpServiceRate {
            bytes: 1_000,
            elapsed: Duration::from_millis(1),
        },
        TcpServiceRate {
            bytes: 2_000,
            elapsed: Duration::from_millis(1),
        },
    )
    .expect("ordered range");
    assert!(suppression.permits(&same_identity, faster_disjoint));

    let mut unchanged_fence = harness.fence.clone();
    unchanged_fence.range_generation += 1;
    unchanged_fence.streams[0].data_ack_horizon_bytes += 1;
    assert_eq!(unchanged_fence.suppression_identity(), same_identity);
    assert!(!suppression.permits(
        &unchanged_fence.suppression_identity(),
        suppression.rejected_reference_range
    ));

    unchanged_fence.streams[0].demand_generation += 1;
    assert!(suppression.permits(
        &unchanged_fence.suppression_identity(),
        suppression.rejected_reference_range
    ));

    let invalid_registered_at = harness.clock + Duration::from_nanos(1);
    assert_eq!(
        harness
            .controller
            .reserve(TcpServiceValidationPlan {
                session_id: SessionId(9),
                trial_id: 0,
                direction: PathMetricDirection::ClientToServer,
                carrier_group_id: carrier_group(1),
                fence: unchanged_fence.clone(),
                limits: limits(),
                registered_at: invalid_registered_at,
                absolute_deadline: invalid_registered_at + Duration::from_secs(1),
            })
            .expect_err("invalid replacement must not alter suppression"),
        TcpServiceValidationError::ZeroIdentifier
    );

    let same_fence = harness.fence.clone();
    let mut other_group_fence = same_fence.clone();
    other_group_fence.accepted = vec![carrier(3, 33, 303)];
    other_group_fence.candidate = carrier(4, 44, 404);
    assert_ne!(
        other_group_fence.suppression_identity(),
        same_fence.suppression_identity(),
        "the unrelated carrier group must exercise a distinct suppression slot"
    );
    harness.restart_validation_for_group(4, carrier_group(2), other_group_fence);
    assert_eq!(
        harness
            .validation
            .withdraw(TcpServiceWithdrawalReason::DemandEnded),
        TcpServiceValidationUpdate::Settled
    );
    let other_group_cleanup = harness
        .controller
        .finish(&mut harness.validation, harness.clock, &harness.fence)
        .expect("unrelated carrier-group withdrawal finishes");
    assert_eq!(
        other_group_cleanup.outcome().withdrawal_reason,
        Some(TcpServiceWithdrawalReason::DemandEnded)
    );
    assert!(harness.controller.complete_cleanup(other_group_cleanup));

    harness.restart_validation_for_group(5, carrier_group(1), same_fence);
    harness.accepted_window(Duration::from_millis(20));
    harness.accepted_window(Duration::from_millis(20));
    assert_eq!(
        harness
            .validation
            .outcome()
            .expect("overlapping baseline is suppressed")
            .withdrawal_reason,
        Some(TcpServiceWithdrawalReason::NoGainSuppressed)
    );
    assert_eq!(
        harness.validation.candidate_placement_credit_bytes(),
        0,
        "suppressed validation grants no candidate Product placement"
    );

    let mut changed_cohort_fence = harness.fence.clone();
    changed_cohort_fence.streams[0].demand_generation += 1;
    harness.restart_validation(6, changed_cohort_fence);
    harness.accepted_window(Duration::from_millis(20));
    harness.accepted_window(Duration::from_millis(20));
    assert_eq!(
        harness.validation.phase(),
        TcpServiceValidationPhase::Readiness,
        "a changed demand cohort reopens service validation"
    );
}

#[test]
fn candidate_credit_never_exceeds_the_existing_unproven_flight_bound() {
    let mut harness = ValidationHarness::new();
    harness.accepted_window(Duration::from_millis(20));
    harness.accepted_window(Duration::from_millis(20));
    assert_eq!(harness.validation.candidate_placement_credit_bytes(), 50);
    let reserved_range = harness.allocate_range(25);
    let TcpServiceCandidateReservationUpdate::Granted(permit) =
        harness.validation.reserve_candidate_placement(
            TcpServiceCandidatePlacement {
                stream: harness.stream,
                range: reserved_range,
            },
            harness.clock,
            &harness.fence,
        )
    else {
        panic!("candidate reservation");
    };
    assert_eq!(harness.validation.candidate_placement_credit_bytes(), 25);
    assert!(matches!(
        harness.validation.reserve_candidate_placement(
            TcpServiceCandidatePlacement {
                stream: harness.stream,
                range: OffsetRange {
                    start: reserved_range.end,
                    end: reserved_range.end + 1,
                },
            },
            harness.clock,
            &harness.fence,
        ),
        TcpServiceCandidateReservationUpdate::Unavailable
    ));
    assert!(harness.validation.cancel_candidate_placement(permit));
    assert_eq!(harness.validation.candidate_placement_credit_bytes(), 50);
    let candidate = harness.candidate_commit(50);
    assert_eq!(harness.validation.candidate_placement_credit_bytes(), 0);

    let update = harness.send_ack(
        vec![TcpServiceAckRelease {
            unambiguous: false,
            ..candidate
        }],
        Duration::from_millis(2),
    );
    assert_eq!(update, TcpServiceValidationUpdate::Settled);
    let outcome = harness.validation.outcome().expect("provisional outcome");
    assert_eq!(outcome.result, TcpCarrierValidationResult::Withdrawn);
    assert_eq!(
        outcome.withdrawal_reason,
        Some(TcpServiceWithdrawalReason::InvalidEvidence)
    );

    let mut bounded_records = ValidationHarness::with_limits(TcpServiceValidationLimits {
        max_ack_release_records: 1,
        validation_horizon_bytes: 2,
        unproven_flight_bytes: 1,
        data_ack_sample_floor_bytes: 1,
        ..limits()
    });
    bounded_records.accepted_window(Duration::from_millis(20));
    bounded_records.accepted_window(Duration::from_millis(20));
    for _ in 0..2 {
        let candidate = bounded_records.candidate_commit(1);
        bounded_records.send_ack(vec![candidate], Duration::from_millis(2));
    }
    assert_eq!(
        bounded_records.validation.phase(),
        TcpServiceValidationPhase::Comparison {
            completed_windows: 0
        }
    );
    let candidate = bounded_records.candidate_commit(1);
    bounded_records.send_ack(vec![candidate], Duration::from_millis(2));
    let next_range = bounded_records.allocate_range(1);
    assert_eq!(
        bounded_records.validation.reserve_candidate_placement(
            TcpServiceCandidatePlacement {
                stream: bounded_records.stream,
                range: next_range,
            },
            bounded_records.clock,
            &bounded_records.fence,
        ),
        TcpServiceCandidateReservationUpdate::Settled,
        "history exhaustion is rejected before carrier queue reservation"
    );
    assert_eq!(
        bounded_records
            .validation
            .outcome()
            .expect("bounded history withdrawal")
            .withdrawal_reason,
        Some(TcpServiceWithdrawalReason::ResourceLimit)
    );

    let mut fragmented = ValidationHarness::with_limits(TcpServiceValidationLimits {
        max_ack_release_records: 2,
        ..limits()
    });
    fragmented.accepted_window(Duration::from_millis(20));
    fragmented.accepted_window(Duration::from_millis(20));
    let candidate = fragmented.candidate_commit(3);
    let pending_range = fragmented.allocate_range(1);
    let TcpServiceCandidateReservationUpdate::Granted(_pending) =
        fragmented.validation.reserve_candidate_placement(
            TcpServiceCandidatePlacement {
                stream: fragmented.stream,
                range: pending_range,
            },
            fragmented.clock,
            &fragmented.fence,
        )
    else {
        panic!("second ledger slot is reserved before queue admission");
    };
    assert_eq!(
        fragmented.send_ack(
            vec![TcpServiceAckRelease {
                range: OffsetRange {
                    start: candidate.range.start + 1,
                    end: candidate.range.start + 2,
                },
                ..candidate
            }],
            Duration::from_millis(2),
        ),
        TcpServiceValidationUpdate::Settled,
        "ACK fragmentation cannot consume a pending permit's ledger slot"
    );
    assert_eq!(
        fragmented
            .validation
            .outcome()
            .expect("fragmentation resource withdrawal")
            .withdrawal_reason,
        Some(TcpServiceWithdrawalReason::ResourceLimit)
    );

    let mut wrong_range = ValidationHarness::new();
    wrong_range.accepted_window(Duration::from_millis(20));
    wrong_range.accepted_window(Duration::from_millis(20));
    let candidate = wrong_range.candidate_commit(50);
    let update = wrong_range.send_ack(
        vec![TcpServiceAckRelease {
            range: OffsetRange {
                start: candidate.range.start - 50,
                end: candidate.range.end - 50,
            },
            ..candidate
        }],
        Duration::from_millis(2),
    );
    assert_eq!(update, TcpServiceValidationUpdate::Settled);
    let outcome = wrong_range
        .validation
        .outcome()
        .expect("provisional outcome");
    assert_eq!(
        outcome.withdrawal_reason,
        Some(TcpServiceWithdrawalReason::InvalidEvidence)
    );

    let mut duplicate = ValidationHarness::new();
    duplicate.accepted_window(Duration::from_millis(20));
    duplicate.accepted_window(Duration::from_millis(20));
    let candidate = duplicate.candidate_commit(50);
    let update = duplicate.send_ack(
        vec![TcpServiceAckRelease {
            kind: TcpServiceReleaseKind::Duplicate,
            ..candidate
        }],
        Duration::from_millis(2),
    );
    assert_eq!(update, TcpServiceValidationUpdate::Settled);
    let outcome = duplicate.validation.outcome().expect("provisional outcome");
    assert_eq!(
        outcome.withdrawal_reason,
        Some(TcpServiceWithdrawalReason::InvalidEvidence)
    );

    let mut reused = ValidationHarness::new();
    reused.accepted_window(Duration::from_millis(20));
    reused.accepted_window(Duration::from_millis(20));
    let candidate = reused.candidate_commit(50);
    reused.send_ack(vec![candidate], Duration::from_millis(2));
    assert_eq!(
        reused.validation.phase(),
        TcpServiceValidationPhase::Readiness
    );
    assert_eq!(
        reused.validation.reserve_candidate_placement(
            TcpServiceCandidatePlacement {
                stream: reused.stream,
                range: candidate.range,
            },
            reused.clock,
            &reused.fence,
        ),
        TcpServiceCandidateReservationUpdate::Settled
    );
    let outcome = reused.validation.outcome().expect("provisional outcome");
    assert_eq!(
        outcome.withdrawal_reason,
        Some(TcpServiceWithdrawalReason::InvalidEvidence)
    );
    assert_eq!(reused.validation.pending_candidate_placements.capacity(), 0);
    assert_eq!(
        reused.validation.committed_candidate_placements.capacity(),
        0
    );
    assert_eq!(reused.validation.candidate_placement_history.capacity(), 0);
}

#[test]
fn candidate_permits_cannot_cross_validation_lifecycles() {
    let mut harness = ValidationHarness::new();
    harness.accepted_window(Duration::from_millis(20));
    harness.accepted_window(Duration::from_millis(20));
    let placement = TcpServiceCandidatePlacement {
        stream: harness.stream,
        range: harness.allocate_range(25),
    };
    let shared_reservation_time = harness.clock + Duration::from_secs(10);
    let TcpServiceCandidateReservationUpdate::Granted(stale_permit) = harness
        .validation
        .reserve_candidate_placement(placement, shared_reservation_time, &harness.fence)
    else {
        panic!("first lifecycle candidate reservation");
    };
    harness
        .validation
        .withdraw(TcpServiceWithdrawalReason::DemandEnded);
    let cleanup = harness
        .controller
        .finish(&mut harness.validation, harness.clock, &harness.fence)
        .expect("first lifecycle settlement");
    assert!(harness.controller.complete_cleanup(cleanup));

    let fence = harness.fence.clone();
    harness.restart_validation(4, fence);
    harness.accepted_window(Duration::from_millis(20));
    harness.accepted_window(Duration::from_millis(20));
    assert!(harness.clock < shared_reservation_time);
    let TcpServiceCandidateReservationUpdate::Granted(current_permit) = harness
        .validation
        .reserve_candidate_placement(placement, shared_reservation_time, &harness.fence)
    else {
        panic!("second lifecycle candidate reservation");
    };

    assert_ne!(stale_permit, current_permit);
    assert!(!harness.validation.cancel_candidate_placement(stale_permit));
    assert!(
        harness
            .validation
            .cancel_candidate_placement(current_permit)
    );
}

#[test]
fn deadline_fence_and_oversized_ack_fail_closed_without_no_gain() {
    let mut deadline = ValidationHarness::new();
    let update = deadline
        .validation
        .poll(Instant::now() + Duration::from_secs(101), &deadline.fence);
    assert_eq!(update, TcpServiceValidationUpdate::Settled);
    let outcome = deadline.validation.outcome().expect("provisional outcome");
    assert_eq!(outcome.result, TcpCarrierValidationResult::Withdrawn);
    assert_eq!(
        outcome.withdrawal_reason,
        Some(TcpServiceWithdrawalReason::Deadline)
    );
    assert_eq!(outcome.no_gain_suppression, None);

    let mut stale = ValidationHarness::new();
    let mut changed = stale.fence.clone();
    changed.accepted[0].eligibility_generation += 1;
    let update = stale.validation.poll(stale.clock, &changed);
    assert_eq!(update, TcpServiceValidationUpdate::Settled);
    let outcome = stale.validation.outcome().expect("provisional outcome");
    assert_eq!(
        outcome.withdrawal_reason,
        Some(TcpServiceWithdrawalReason::FenceChanged)
    );

    let mut bounded = ValidationHarness::new();
    bounded.saturate();
    bounded.clock += Duration::from_millis(1);
    let committed_at = bounded.clock;
    let mut releases = Vec::new();
    for _ in 0..17 {
        releases.push(TcpServiceAckRelease {
            carrier: bounded.accepted,
            range: bounded.allocate_range(1),
            committed_at: Some(bounded.writer_lifecycle.point(committed_at)),
            kind: TcpServiceReleaseKind::Original,
            unambiguous: true,
        });
    }
    let update = bounded.send_ack(releases, Duration::from_millis(1));
    assert_eq!(update, TcpServiceValidationUpdate::Settled);
    let outcome = bounded.validation.outcome().expect("provisional outcome");
    assert_eq!(
        outcome.withdrawal_reason,
        Some(TcpServiceWithdrawalReason::InvalidEvidence)
    );

    let mut ended = ValidationHarness::new();
    let update = ended
        .validation
        .withdraw(TcpServiceWithdrawalReason::DemandEnded);
    assert_eq!(update, TcpServiceValidationUpdate::Settled);
    let outcome = ended.validation.outcome().expect("provisional outcome");
    assert_eq!(
        outcome.withdrawal_reason,
        Some(TcpServiceWithdrawalReason::DemandEnded)
    );

    let mut delayed = ValidationHarness::new();
    let observed_at = delayed
        .writer_lifecycle
        .point(delayed.clock + Duration::from_millis(1));
    let update = delayed.controller.observe_saturation(
        &mut delayed.validation,
        TcpServiceSaturationObservation {
            observed_at,
            accepted_with_original_flight: delayed.fence.accepted.clone(),
            streams_with_fresh_demand: delayed.fence.streams.clone(),
            blocked_stream: delayed.stream,
            blocked_range: OffsetRange { start: 0, end: 1 },
        },
        delayed.clock + Duration::from_secs(101),
        &delayed.fence,
    );
    assert_eq!(update, TcpServiceValidationUpdate::Settled);
    let outcome = delayed.validation.outcome().expect("provisional outcome");
    assert_eq!(
        outcome.withdrawal_reason,
        Some(TcpServiceWithdrawalReason::Deadline)
    );
}

#[test]
fn absolute_deadline_recovers_cancelled_installing_and_running_lifecycles() {
    let template = ValidationHarness::new();
    let registered_at = Instant::now();
    let initial_at = registered_at + Duration::from_millis(1);
    let absolute_deadline = registered_at + Duration::from_secs(1);
    let plan = |trial_id| TcpServiceValidationPlan {
        session_id: SessionId(31),
        trial_id,
        direction: PathMetricDirection::ClientToServer,
        carrier_group_id: carrier_group(1),
        fence: template.fence.clone(),
        limits: limits(),
        registered_at,
        absolute_deadline,
    };
    let mut controller =
        TcpServiceSessionController::new(SessionId(31), 2).expect("session controller");

    let preparation = controller
        .reserve(plan(1))
        .expect("installation reservation");
    let installing_lifecycle = preparation.writer_lifecycle();
    assert_eq!(controller.active_deadline(), Some(absolute_deadline));
    drop(preparation);
    assert!(
        controller
            .begin_expiry(absolute_deadline - Duration::from_nanos(1))
            .is_none()
    );
    let cleanup = controller
        .begin_expiry(absolute_deadline)
        .expect("cancelled installation expires");
    assert_eq!(cleanup.writer_lifecycle(), installing_lifecycle);
    drop(cleanup);
    let cleanup = controller
        .begin_expiry(absolute_deadline)
        .expect("cancelled cleanup can reacquire exact authority");
    assert_eq!(cleanup.writer_lifecycle(), installing_lifecycle);
    assert_eq!(cleanup.stage(), TcpServiceCleanupStage::Installing);
    assert_eq!(
        cleanup.outcome().withdrawal_reason,
        Some(TcpServiceWithdrawalReason::Deadline)
    );
    assert_eq!(
        controller
            .reserve(plan(2))
            .expect_err("cleanup acknowledgment precedes replacement"),
        TcpServiceValidationError::ValidationInProgress
    );
    assert_eq!(controller.pending_cleanup(), Some(cleanup.clone()));
    let stale_cleanup = cleanup.clone();
    assert!(controller.complete_cleanup(cleanup));
    assert!(!controller.has_active_lifecycle());

    let preparation = controller
        .reserve(plan(2))
        .expect("running lifecycle reservation");
    assert!(
        !controller.complete_cleanup(stale_cleanup),
        "an acknowledged lifecycle cannot clear its replacement"
    );
    let running_lifecycle = preparation.writer_lifecycle();
    let validation = controller
        .activate(
            preparation,
            TcpServiceBoundary {
                ack_sequence: 1,
                acked_at: initial_at,
                writer: running_lifecycle.point(initial_at),
            },
            initial_at,
            &template.fence,
        )
        .expect("running lifecycle");
    drop(validation);
    let cleanup = controller
        .begin_expiry(absolute_deadline)
        .expect("cancelled running lifecycle expires");
    assert_eq!(cleanup.writer_lifecycle(), running_lifecycle);
    assert_eq!(cleanup.stage(), TcpServiceCleanupStage::Running);
    let withdrawal = cleanup.outcome();
    assert_eq!(withdrawal.session_id, SessionId(31));
    assert_eq!(withdrawal.trial_id, 2);
    assert_eq!(withdrawal.candidate, template.fence.candidate);
    assert_eq!(withdrawal.direction, PathMetricDirection::ClientToServer);
    assert_eq!(withdrawal.result, TcpCarrierValidationResult::Withdrawn);
    assert_eq!(
        withdrawal.withdrawal_reason,
        Some(TcpServiceWithdrawalReason::Deadline)
    );
    assert_eq!(withdrawal.no_gain_suppression, None);
    assert!(controller.complete_cleanup(cleanup));
    assert!(!controller.has_active_lifecycle());
}

#[test]
fn exact_rate_ordering_handles_values_that_cannot_be_cross_multiplied() {
    let maximum = TcpServiceRate {
        bytes: u64::MAX,
        elapsed: Duration::MAX,
    };
    let slightly_less = TcpServiceRate {
        bytes: u64::MAX - 1,
        elapsed: Duration::MAX,
    };
    assert_eq!(maximum.cmp_exact(slightly_less), Ordering::Greater);
    assert_eq!(
        TcpServiceRate {
            bytes: 1,
            elapsed: Duration::from_nanos(3),
        }
        .cmp_exact(TcpServiceRate {
            bytes: 2,
            elapsed: Duration::from_nanos(6),
        }),
        Ordering::Equal
    );
    for left_numerator in 0_u128..32 {
        for left_denominator in 1_u128..32 {
            for right_numerator in 0_u128..32 {
                for right_denominator in 1_u128..32 {
                    assert_eq!(
                        compare_nonnegative_fractions(
                            left_numerator,
                            left_denominator,
                            right_numerator,
                            right_denominator,
                        ),
                        (left_numerator * right_denominator)
                            .cmp(&(right_numerator * left_denominator))
                    );
                }
            }
        }
    }
}

#[test]
fn constructors_and_session_serialization_enforce_bounded_authority() {
    let harness = ValidationHarness::new();
    assert_eq!(
        harness.validation.request_id(),
        0,
        "client-to-server validation is locally demanded"
    );
    assert_eq!(
        harness.fence.accepted_wire_instances(),
        vec![harness.accepted.accepted]
    );

    let registered_at = Instant::now();
    let initial_at = registered_at + Duration::from_millis(1);
    let mut invalid_fence = harness.fence.clone();
    invalid_fence.accepted.push(invalid_fence.accepted[0]);
    invalid_fence.demand = TcpServiceDemandFence::PeerRequest {
        request_id: 7,
        anchor: invalid_fence.accepted[0],
    };
    let mut controller =
        TcpServiceSessionController::new(SessionId(1), 2).expect("session controller");
    let error = controller
        .reserve(TcpServiceValidationPlan {
            session_id: SessionId(1),
            trial_id: 1,
            direction: PathMetricDirection::ServerToClient,
            carrier_group_id: carrier_group(1),
            fence: invalid_fence,
            limits: limits(),
            registered_at,
            absolute_deadline: registered_at + Duration::from_secs(1),
        })
        .expect_err("duplicate accepted carrier");
    assert_eq!(error, TcpServiceValidationError::NonCanonicalCarriers);

    let mut duplicate_path_fence = harness.fence.clone();
    duplicate_path_fence
        .accepted
        .push(carrier(harness.accepted.accepted.path_id.0, 99, 909));
    duplicate_path_fence
        .accepted
        .sort_by_key(|carrier| carrier.accepted);
    let mut client_controller =
        TcpServiceSessionController::new(SessionId(9), 2).expect("session controller");
    let error = client_controller
        .reserve(TcpServiceValidationPlan {
            session_id: SessionId(9),
            trial_id: 2,
            direction: PathMetricDirection::ClientToServer,
            carrier_group_id: carrier_group(1),
            fence: duplicate_path_fence,
            limits: limits(),
            registered_at,
            absolute_deadline: registered_at + Duration::from_secs(1),
        })
        .expect_err("one session cannot reuse a live PathId");
    assert_eq!(error, TcpServiceValidationError::NonCanonicalCarriers);

    let mut session =
        TcpServiceSessionController::new(SessionId(5), usize::MAX).expect("bounded session owner");
    let first_fence = harness.fence.clone();
    let mut second_fence = harness.fence.clone();
    second_fence.candidate = carrier(3, 33, 303);
    second_fence.demand = TcpServiceDemandFence::PeerRequest {
        request_id: 2,
        anchor: second_fence.accepted[0],
    };
    let plan = |trial_id, direction, fence| TcpServiceValidationPlan {
        session_id: SessionId(5),
        trial_id,
        direction,
        carrier_group_id: carrier_group(1),
        fence,
        limits: limits(),
        registered_at,
        absolute_deadline: registered_at + Duration::from_secs(1),
    };
    let first_preparation = session
        .reserve(plan(
            1,
            PathMetricDirection::ClientToServer,
            first_fence.clone(),
        ))
        .expect("first session preparation");
    let first_writer_lifecycle = first_preparation.writer_lifecycle();
    assert!(session.has_active_lifecycle());
    assert_eq!(
        session
            .reserve(plan(
                2,
                PathMetricDirection::ServerToClient,
                second_fence.clone(),
            ))
            .expect_err("directions share one session reservation"),
        TcpServiceValidationError::ValidationInProgress
    );
    let mut first = session
        .activate(
            first_preparation,
            TcpServiceBoundary {
                ack_sequence: 1,
                acked_at: initial_at,
                writer: first_writer_lifecycle.point(initial_at),
            },
            initial_at,
            &first_fence,
        )
        .expect("first session validation");
    first.withdraw(TcpServiceWithdrawalReason::DemandEnded);
    let first_cleanup = session
        .finish(&mut first, initial_at, &first_fence)
        .expect("first direction enters cleanup");
    assert_eq!(
        session
            .reserve(plan(
                2,
                PathMetricDirection::ServerToClient,
                second_fence.clone(),
            ))
            .expect_err("observer cleanup precedes the other direction"),
        TcpServiceValidationError::ValidationInProgress
    );
    assert!(session.complete_cleanup(first_cleanup));
    assert_eq!(
        session
            .reserve(plan(
                1,
                PathMetricDirection::ClientToServer,
                first_fence.clone(),
            ))
            .expect_err("trial IDs are never reused"),
        TcpServiceValidationError::TrialNotIncreasing
    );
    let second_preparation = session
        .reserve(plan(
            2,
            PathMetricDirection::ServerToClient,
            second_fence.clone(),
        ))
        .expect("second direction preparation");
    let second_writer_lifecycle = second_preparation.writer_lifecycle();
    let mut second = session
        .activate(
            second_preparation,
            TcpServiceBoundary {
                ack_sequence: 2,
                acked_at: initial_at,
                writer: second_writer_lifecycle.point(initial_at),
            },
            initial_at,
            &second_fence,
        )
        .expect("second direction after release");
    second.withdraw(TcpServiceWithdrawalReason::DemandEnded);
    let second_cleanup = session
        .finish(&mut second, initial_at, &second_fence)
        .expect("second direction enters cleanup");
    assert!(session.complete_cleanup(second_cleanup));
    assert!(session.retire_candidate(first_fence.candidate));

    let third_preparation = session
        .reserve(plan(
            3,
            PathMetricDirection::ServerToClient,
            second_fence.clone(),
        ))
        .expect("third lifecycle preparation");
    let failure = session
        .activate(
            third_preparation,
            TcpServiceBoundary {
                ack_sequence: 3,
                acked_at: initial_at,
                writer: first_writer_lifecycle.point(initial_at),
            },
            initial_at,
            &second_fence,
        )
        .expect_err("an earlier lifecycle cannot activate frozen writers");
    assert_eq!(failure.error, TcpServiceValidationError::InvalidBoundary);
    let installation_cleanup = session
        .begin_installation_cleanup(
            failure.preparation,
            TcpServiceWithdrawalReason::InvalidEvidence,
            initial_at,
        )
        .expect("failed installation enters cleanup");
    assert_eq!(
        installation_cleanup.outcome().withdrawal_reason,
        Some(TcpServiceWithdrawalReason::InvalidEvidence)
    );
    assert_eq!(
        session
            .reserve(plan(
                4,
                PathMetricDirection::ServerToClient,
                second_fence.clone(),
            ))
            .expect_err("failed-install observers must clean before replacement"),
        TcpServiceValidationError::ValidationInProgress
    );
    assert!(session.complete_cleanup(installation_cleanup));
    assert!(!session.has_active_lifecycle());
    assert_eq!(
        session
            .reserve(plan(3, PathMetricDirection::ServerToClient, second_fence,))
            .expect_err("an installation trial ID is never reused"),
        TcpServiceValidationError::TrialNotIncreasing
    );
}

#[test]
fn windows_require_every_frozen_stream_and_keep_ack_events_indivisible() {
    let registered_at = Instant::now();
    let initial_at = registered_at + Duration::from_millis(1);
    let accepted = carrier(1, 11, 101);
    let candidate = carrier(2, 22, 202);
    let first_stream = stream(7, 100);
    let second_stream = stream(8, 100);
    let fence = TcpServiceValidationFence {
        range_generation: 1,
        demand: TcpServiceDemandFence::Local,
        accepted: vec![accepted],
        candidate,
        streams: vec![first_stream, second_stream],
    };
    let mut controller =
        TcpServiceSessionController::new(SessionId(9), 2).expect("session controller");
    let (mut validation, writer_lifecycle) = activate_plan(
        &mut controller,
        TcpServiceValidationPlan {
            session_id: SessionId(9),
            trial_id: 3,
            direction: PathMetricDirection::ClientToServer,
            carrier_group_id: carrier_group(1),
            fence: fence.clone(),
            limits: limits(),
            registered_at,
            absolute_deadline: registered_at + Duration::from_secs(10),
        },
        1,
        initial_at,
    );
    assert_eq!(validation.boundary().writer.at(), initial_at);
    controller.observe_saturation(
        &mut validation,
        TcpServiceSaturationObservation {
            observed_at: writer_lifecycle.point(initial_at + Duration::from_millis(1)),
            accepted_with_original_flight: fence.accepted.clone(),
            streams_with_fresh_demand: fence.streams.clone(),
            blocked_stream: first_stream,
            blocked_range: OffsetRange { start: 0, end: 1 },
        },
        initial_at + Duration::from_millis(1),
        &fence,
    );

    let first_commit = initial_at + Duration::from_millis(2);
    validation.observe_data_ack(
        TcpServiceDataAckEvent {
            sequence: 2,
            stream: first_stream,
            assigned_end: 300,
            acked_at: initial_at + Duration::from_millis(10),
            next_writer_boundary: writer_lifecycle.point(initial_at + Duration::from_millis(11)),
            releases: vec![TcpServiceAckRelease {
                carrier: accepted,
                range: OffsetRange { start: 0, end: 300 },
                committed_at: Some(writer_lifecycle.point(first_commit)),
                kind: TcpServiceReleaseKind::Original,
                unambiguous: true,
            }],
        },
        initial_at + Duration::from_millis(11),
        &fence,
    );
    assert_eq!(
        validation.phase(),
        TcpServiceValidationPhase::PreReference {
            completed_windows: 0
        },
        "aggregate coverage cannot substitute for the second stream"
    );

    let second_commit = initial_at + Duration::from_millis(12);
    validation.observe_data_ack(
        TcpServiceDataAckEvent {
            sequence: 3,
            stream: second_stream,
            assigned_end: 400,
            acked_at: initial_at + Duration::from_millis(20),
            next_writer_boundary: writer_lifecycle.point(initial_at + Duration::from_millis(21)),
            releases: vec![
                TcpServiceAckRelease {
                    carrier: accepted,
                    range: OffsetRange {
                        start: 300,
                        end: 400,
                    },
                    committed_at: Some(writer_lifecycle.point(second_commit)),
                    kind: TcpServiceReleaseKind::Original,
                    unambiguous: true,
                },
                TcpServiceAckRelease {
                    carrier: accepted,
                    range: OffsetRange {
                        start: 100,
                        end: 101,
                    },
                    committed_at: Some(writer_lifecycle.point(initial_at)),
                    kind: TcpServiceReleaseKind::Duplicate,
                    unambiguous: false,
                },
            ],
        },
        initial_at + Duration::from_millis(21),
        &fence,
    );
    assert_eq!(
        validation.phase(),
        TcpServiceValidationPhase::PreReference {
            completed_windows: 1
        }
    );
    assert_eq!(
        validation.pre_reference_rates[0].bytes, 400,
        "the completing ACK overshoot must remain whole"
    );

    let mut indivisible = ValidationHarness::new();
    indivisible.saturate();
    let boundary = indivisible.validation.boundary();
    let old_range = indivisible.allocate_range(1);
    let new_range = indivisible.allocate_range(200);
    indivisible.send_ack(
        vec![
            TcpServiceAckRelease {
                carrier: indivisible.accepted,
                range: old_range,
                committed_at: None,
                kind: TcpServiceReleaseKind::Original,
                unambiguous: true,
            },
            TcpServiceAckRelease {
                carrier: indivisible.accepted,
                range: new_range,
                committed_at: Some(
                    indivisible
                        .writer_lifecycle
                        .point(boundary.writer.at() + Duration::from_millis(1)),
                ),
                kind: TcpServiceReleaseKind::Original,
                unambiguous: true,
            },
        ],
        Duration::from_millis(2),
    );
    assert_eq!(
        indivisible.validation.phase(),
        TcpServiceValidationPhase::PreReference {
            completed_windows: 0
        },
        "one ACK transaction cannot be split at the writer boundary"
    );
    indivisible.clock += Duration::from_millis(1);
    let range = indivisible.allocate_range(200);
    let committed_at = indivisible.clock;
    indivisible.send_ack(
        vec![TcpServiceAckRelease {
            carrier: indivisible.accepted,
            range,
            committed_at: Some(indivisible.writer_lifecycle.point(committed_at)),
            kind: TcpServiceReleaseKind::Original,
            unambiguous: true,
        }],
        Duration::from_millis(20),
    );
    assert_eq!(indivisible.validation.pre_reference_rates[0].bytes, 200);

    let mut late_saturation = ValidationHarness::new();
    late_saturation.clock += Duration::from_millis(1);
    let range = late_saturation.allocate_range(200);
    let committed_at = late_saturation.clock;
    late_saturation.send_ack(
        vec![TcpServiceAckRelease {
            carrier: late_saturation.accepted,
            range,
            committed_at: Some(late_saturation.writer_lifecycle.point(committed_at)),
            kind: TcpServiceReleaseKind::Original,
            unambiguous: true,
        }],
        Duration::from_millis(1),
    );
    late_saturation.clock += Duration::from_millis(50);
    late_saturation.controller.observe_saturation(
        &mut late_saturation.validation,
        TcpServiceSaturationObservation {
            observed_at: late_saturation
                .writer_lifecycle
                .point(late_saturation.clock),
            accepted_with_original_flight: late_saturation.fence.accepted.clone(),
            streams_with_fresh_demand: late_saturation.fence.streams.clone(),
            blocked_stream: late_saturation.stream,
            blocked_range: OffsetRange {
                start: late_saturation.next_offset,
                end: late_saturation.next_offset + 1,
            },
        },
        late_saturation.clock,
        &late_saturation.fence,
    );
    assert_eq!(
        late_saturation.validation.phase(),
        TcpServiceValidationPhase::PreReference {
            completed_windows: 0
        },
        "a saturation after the evidence boundary cannot retro-complete a window"
    );
    late_saturation.clock += Duration::from_millis(1);
    let range = late_saturation.allocate_range(1);
    let committed_at = late_saturation.clock;
    late_saturation.send_ack(
        vec![TcpServiceAckRelease {
            carrier: late_saturation.accepted,
            range,
            committed_at: Some(late_saturation.writer_lifecycle.point(committed_at)),
            kind: TcpServiceReleaseKind::Original,
            unambiguous: true,
        }],
        Duration::from_millis(1),
    );
    assert_eq!(
        late_saturation.validation.phase(),
        TcpServiceValidationPhase::PreReference {
            completed_windows: 1
        }
    );

    let mut duplicate_saturation = ValidationHarness::new();
    duplicate_saturation.saturate();
    duplicate_saturation.clock += Duration::from_millis(1);
    let update = duplicate_saturation.controller.observe_saturation(
        &mut duplicate_saturation.validation,
        TcpServiceSaturationObservation {
            observed_at: duplicate_saturation
                .writer_lifecycle
                .point(duplicate_saturation.clock),
            accepted_with_original_flight: duplicate_saturation.fence.accepted.clone(),
            streams_with_fresh_demand: duplicate_saturation.fence.streams.clone(),
            blocked_stream: duplicate_saturation.stream,
            blocked_range: OffsetRange { start: 0, end: 2 },
        },
        duplicate_saturation.clock,
        &duplicate_saturation.fence,
    );
    assert_eq!(update, TcpServiceValidationUpdate::Settled);
    let outcome = duplicate_saturation
        .validation
        .outcome()
        .expect("provisional outcome");
    assert_eq!(
        outcome.withdrawal_reason,
        Some(TcpServiceWithdrawalReason::InvalidEvidence)
    );

    let mut persistent_saturation = ValidationHarness::new();
    persistent_saturation.saturate();
    persistent_saturation
        .validation
        .withdraw(TcpServiceWithdrawalReason::DemandEnded);
    let same_fence = persistent_saturation.fence.clone();
    persistent_saturation.restart_validation(4, same_fence);
    persistent_saturation.clock += Duration::from_millis(1);
    let update = persistent_saturation.controller.observe_saturation(
        &mut persistent_saturation.validation,
        TcpServiceSaturationObservation {
            observed_at: persistent_saturation
                .writer_lifecycle
                .point(persistent_saturation.clock),
            accepted_with_original_flight: persistent_saturation.fence.accepted.clone(),
            streams_with_fresh_demand: persistent_saturation.fence.streams.clone(),
            blocked_stream: persistent_saturation.stream,
            blocked_range: OffsetRange { start: 0, end: 1 },
        },
        persistent_saturation.clock,
        &persistent_saturation.fence,
    );
    assert_eq!(update, TcpServiceValidationUpdate::Settled);
    let outcome = persistent_saturation
        .validation
        .outcome()
        .expect("provisional outcome");
    assert_eq!(
        outcome.withdrawal_reason,
        Some(TcpServiceWithdrawalReason::InvalidEvidence)
    );

    let mut rejected_saturation = ValidationHarness::new();
    let observed_at = rejected_saturation
        .writer_lifecycle
        .point(rejected_saturation.clock + Duration::from_secs(1));
    let update = rejected_saturation.controller.observe_saturation(
        &mut rejected_saturation.validation,
        TcpServiceSaturationObservation {
            observed_at,
            accepted_with_original_flight: rejected_saturation.fence.accepted.clone(),
            streams_with_fresh_demand: rejected_saturation.fence.streams.clone(),
            blocked_stream: rejected_saturation.stream,
            blocked_range: OffsetRange { start: 0, end: 1 },
        },
        rejected_saturation.clock,
        &rejected_saturation.fence,
    );
    assert_eq!(update, TcpServiceValidationUpdate::Settled);
    let same_fence = rejected_saturation.fence.clone();
    rejected_saturation.restart_validation(4, same_fence);
    rejected_saturation.clock += Duration::from_millis(1);
    let update = rejected_saturation.controller.observe_saturation(
        &mut rejected_saturation.validation,
        TcpServiceSaturationObservation {
            observed_at: rejected_saturation
                .writer_lifecycle
                .point(rejected_saturation.clock),
            accepted_with_original_flight: rejected_saturation.fence.accepted.clone(),
            streams_with_fresh_demand: rejected_saturation.fence.streams.clone(),
            blocked_stream: rejected_saturation.stream,
            blocked_range: OffsetRange { start: 0, end: 1 },
        },
        rejected_saturation.clock,
        &rejected_saturation.fence,
    );
    assert_eq!(
        update,
        TcpServiceValidationUpdate::Pending,
        "rejected observations cannot consume persistent one-shot state"
    );

    let mut writer_span = ValidationHarness::new();
    let initial_boundary = writer_span.validation.boundary();
    writer_span.saturate();
    writer_span.clock += Duration::from_millis(1);
    let range = writer_span.allocate_range(200);
    let committed_at = writer_span.clock;
    let acked_at = committed_at + Duration::from_millis(1);
    let writer_at = committed_at + Duration::from_millis(50);
    let committed = writer_span.writer_lifecycle.point(committed_at);
    let writer_boundary = writer_span.writer_lifecycle.point(writer_at);
    writer_span.ack_sequence += 1;
    writer_span.validation.observe_data_ack(
        TcpServiceDataAckEvent {
            sequence: writer_span.ack_sequence,
            stream: writer_span.stream,
            assigned_end: writer_span.next_offset,
            acked_at,
            next_writer_boundary: writer_boundary,
            releases: vec![TcpServiceAckRelease {
                carrier: writer_span.accepted,
                range,
                committed_at: Some(committed),
                kind: TcpServiceReleaseKind::Original,
                unambiguous: true,
            }],
        },
        writer_at,
        &writer_span.fence,
    );
    assert_eq!(
        writer_span.validation.pre_reference_rates[0].elapsed,
        writer_at.duration_since(initial_boundary.writer.at()),
        "service time ends at the serialized writer boundary"
    );

    let update = writer_span.validation.observe_data_ack(
        TcpServiceDataAckEvent {
            sequence: writer_span.ack_sequence + 1,
            stream: writer_span.stream,
            assigned_end: writer_span.next_offset,
            acked_at: acked_at + Duration::from_millis(1),
            next_writer_boundary: writer_span
                .writer_lifecycle
                .point(acked_at + Duration::from_millis(2)),
            releases: Vec::new(),
        },
        writer_at + Duration::from_millis(1),
        &writer_span.fence,
    );
    assert_eq!(update, TcpServiceValidationUpdate::Settled);
    let outcome = writer_span
        .validation
        .outcome()
        .expect("provisional outcome");
    assert_eq!(
        outcome.withdrawal_reason,
        Some(TcpServiceWithdrawalReason::InvalidEvidence)
    );
}
