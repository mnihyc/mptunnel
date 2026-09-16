//! Model-trial tests, not assertions that the previous credit-only RFC was
//! implemented incorrectly. The old model excludes a reported covered head
//! whenever one byte of assignment credit remains.
use super::*;
use crate::model::capacity::reliable_path_startup_sample_limit_bytes;
use crate::runtime::relay::io::{
    begin_reliable_stream_ack, update_reinjection_authoritative_ack_snapshot,
};

// The old credit-only fixture has a rate prior but no qualified Product bytes
// on the alternate. A proven-gap route deliberately requires qualified service.
// Establish that through exact Original assignment and ACK rather than changing
// eligibility or inventing a has_bulk_rate_evidence flag.
fn qualify_reported_target(fixture: &mut CreditFrontierFixture) {
    let floor = reliable_path_startup_sample_limit_bytes(fixture.binding.mux_limits());
    let start = fixture.stream.next_offset();
    fixture.stream.update_max_offset(start + floor + 1);
    let frame = fixture
        .stream
        .send_data(Bytes::from(vec![0x41; floor as usize]))
        .unwrap();
    let target = credit_frontier_target(&fixture.binding, 2);
    fixture.binding.record_original_flight(target.key, &frame);
    let ack = begin_reliable_stream_ack(
        &fixture.stream,
        None,
        vec![OffsetRange {
            start,
            end: start + floor,
        }],
    )
    .unwrap();
    fixture
        .binding
        .release_normalized_acked_ranges(ack.ranges());
    fixture.stream.apply_ack(ack.ranges()).unwrap();
    fixture
        .binding
        .set_output_product_model_for_test(target.key, 100_000_000.0, 10.0);
    let targets = fixture
        .binding
        .sender_path_targets(TrafficClass::Throughput, 4096);
    let observation = &targets
        .iter()
        .find(|t| t.observation.key == target.key)
        .unwrap()
        .observation;
    assert!(observation.product_assignment_qualified);
    assert!(observation.has_bulk_rate_evidence);
}

fn report_response_ack(
    fixture: &mut CreditFrontierFixture,
    state: &mut AuthoritativeStreamAckSnapshot,
    scope: Option<u64>,
    range: OffsetRange,
) {
    let ack = begin_reliable_stream_ack(&fixture.stream, scope, vec![range]).unwrap();
    fixture.stream.apply_ack(ack.ranges()).unwrap();
    fixture
        .binding
        .release_normalized_acked_ranges(ack.ranges());
    update_reinjection_authoritative_ack_snapshot(state, &ack, &fixture.stream);
}

#[test]
fn model_trial_reported_frontier_response_partial_ack_and_stale_identity_revoke_apply() {
    for change in [
        "interior_ack",
        "prefix_ack",
        "frame",
        "owner",
        "target",
        "max",
    ] {
        let mut fixture = credit_frontier_fixture(8193, Duration::from_secs(10));
        qualify_reported_target(&mut fixture);
        let mut ack_state = AuthoritativeStreamAckSnapshot::default();
        report_response_ack(
            &mut fixture,
            &mut ack_state,
            Some(0),
            OffsetRange {
                start: 4096,
                end: 8192,
            },
        );
        let target = credit_frontier_target(&fixture.binding, 2);
        let mut candidate = fixture
            .sender
            .next_prepared_recovery(
                &fixture.binding,
                &fixture.stream,
                &ack_state,
                TrafficClass::Throughput,
                &fixture
                    .binding
                    .sender_path_targets(TrafficClass::Throughput, 4096),
                &[target],
                Instant::now(),
            )
            .candidate
            .unwrap();
        assert!(
            candidate
                .credit_frontier
                .as_ref()
                .unwrap()
                .reported_gap
                .is_some()
        );
        match change {
            "interior_ack" => report_response_ack(
                &mut fixture,
                &mut ack_state,
                Some(0),
                OffsetRange {
                    start: 512,
                    end: 1024,
                },
            ),
            "prefix_ack" => report_response_ack(
                &mut fixture,
                &mut ack_state,
                Some(0),
                OffsetRange {
                    start: 0,
                    end: 1024,
                },
            ),
            "frame" => candidate.frame = fixture.frames[1].clone(),
            "owner" => {
                candidate
                    .credit_frontier
                    .as_mut()
                    .unwrap()
                    .owner
                    .incarnation += 1
            }
            "target" => candidate.target.incarnation += 1,
            "max" => fixture
                .stream
                .update_max_offset(fixture.stream.peer_max_offset() + 4096),
            _ => unreachable!(),
        }
        if change == "interior_ack" {
            assert_eq!(
                fixture.stream.data_ack_frontier(),
                0,
                "F equality alone cannot validate an interior-ACK-stale proof"
            );
        }
        let before = fixture.sender.optional_reinjection.reinjected_bytes();
        let ready = fixture.receivers[2]
            .writer_ready_boundary(target.path_instance_id)
            .unwrap();
        let result = fixture.sender.commit_prepared_recovery(
            &fixture.binding,
            &fixture.stream,
            &candidate,
            ready,
            None,
        );
        if change == "max" {
            result.expect("new MAX does not erase an already reported, still-retained gap");
            assert!(!ready.receipt().is_current());
            let command = try_recv_reliable_path_command(&mut fixture.receivers[2]).unwrap();
            assert!(
                matches!(command,ReliablePathCommand::SendFrame(ref frame) if *frame==fixture.frames[0])
            );
            fixture.receivers[2].release_pending_command_bytes(
                crate::protocol::frame::reliable_path_frame_pacing_bytes(&fixture.frames[0]),
            );
        } else {
            assert!(result.is_err(), "stale {change} proof must not publish");
            assert!(ready.receipt().is_current());
            assert_eq!(
                fixture.sender.optional_reinjection.reinjected_bytes(),
                before
            );
            assert!(try_recv_reliable_path_command(&mut fixture.receivers[2]).is_none());
        }
    }
}

#[test]
fn model_trial_reported_frontier_response_requires_fallback_scope_and_real_ready() {
    for blocked in ["young", "scope_above_head", "not_ready"] {
        let mut fixture = credit_frontier_fixture(
            8193,
            if blocked == "young" {
                Duration::ZERO
            } else {
                Duration::from_secs(10)
            },
        );
        qualify_reported_target(&mut fixture);
        let mut ack_state = AuthoritativeStreamAckSnapshot::default();
        let (scope, range) = if blocked == "scope_above_head" {
            (
                Some(4096),
                OffsetRange {
                    start: 6144,
                    end: 8192,
                },
            )
        } else {
            (
                Some(0),
                OffsetRange {
                    start: 4096,
                    end: 8192,
                },
            )
        };
        report_response_ack(&mut fixture, &mut ack_state, scope, range);
        let target = credit_frontier_target(&fixture.binding, 2);
        let selected = if blocked == "not_ready" {
            vec![]
        } else {
            vec![target]
        };
        let now = Instant::now();
        let result = fixture.sender.next_prepared_recovery(
            &fixture.binding,
            &fixture.stream,
            &ack_state,
            TrafficClass::Latency,
            &fixture
                .binding
                .sender_path_targets(TrafficClass::Latency, 4096),
            &selected,
            now,
        );
        if blocked == "scope_above_head" {
            assert!(!ack_state.reports_frontier(&fixture.stream));
            assert!(result.candidate.is_none_or(|c| {
                crate::protocol::frame::reliable_stream_frame_extent(&c.frame)
                    .is_some_and(|(start, _, _)| start >= 4096)
            }));
        } else {
            assert!(
                result.candidate.is_none(),
                "{blocked} cannot publish covered head"
            );
            if blocked == "young" {
                assert!(result.next_deadline.is_some_and(|at| at > now));
            }
        }
    }
}

#[test]
fn model_trial_reported_frontier_response_uses_scoped_gap_not_spare_credit() {
    for scoped in [false, true] {
        let mut fixture = credit_frontier_fixture(8193, Duration::from_secs(10));
        qualify_reported_target(&mut fixture);
        let mut ack_state = AuthoritativeStreamAckSnapshot::default();
        let ack = begin_reliable_stream_ack(
            &fixture.stream,
            scoped.then_some(0),
            vec![OffsetRange {
                start: 4096,
                end: 8192,
            }],
        )
        .unwrap();
        fixture.stream.apply_ack(ack.ranges()).unwrap();
        fixture
            .binding
            .release_normalized_acked_ranges(ack.ranges());
        update_reinjection_authoritative_ack_snapshot(&mut ack_state, &ack, &fixture.stream);
        assert_eq!(ack_state.has_gaps(), scoped);
        assert_eq!(fixture.stream.data_ack_frontier(), 0);
        assert_eq!(
            fixture.stream.next_offset() + 1,
            fixture.stream.peer_max_offset()
        );
        let target = credit_frontier_target(&fixture.binding, 2);
        // Real writer occupancy, not a fabricated Ready counter.
        let ready = fixture.receivers[2]
            .writer_ready_boundary(target.path_instance_id)
            .unwrap();
        let now = Instant::now();
        let old_deadline = fixture
            .binding
            .reinjection_suppression_deadline(&fixture.frames[0])
            .unwrap();
        assert!(
            old_deadline > now,
            "the old accepted copy still suppresses the head"
        );
        let result = fixture.sender.next_prepared_recovery(
            &fixture.binding,
            &fixture.stream,
            &ack_state,
            TrafficClass::Throughput,
            &fixture
                .binding
                .sender_path_targets(TrafficClass::Throughput, 4096),
            &[target],
            now,
        );
        if !scoped {
            assert!(
                result.candidate.is_none(),
                "positive receipt without scope proves no missing head"
            );
            assert!(ready.receipt().is_current());
            continue;
        }
        let candidate = result.candidate.expect(
            "model trial: mature reported head can use a vacant slot despite spare assignment credit",
        );
        assert_eq!(candidate.frame, fixture.frames[0]);
        assert!(candidate.credit_frontier.is_some());
        assert!(!candidate.early_completion_copy);
        fixture
            .sender
            .commit_prepared_recovery(&fixture.binding, &fixture.stream, &candidate, ready, None)
            .unwrap();
        assert!(!ready.receipt().is_current());
        let command = try_recv_reliable_path_command(&mut fixture.receivers[2]).unwrap();
        assert!(
            matches!(&command, ReliablePathCommand::SendFrame(frame) if *frame == fixture.frames[0])
        );
        fixture.receivers[2].release_pending_command_bytes(
            crate::protocol::frame::reliable_path_frame_pacing_bytes(&fixture.frames[0]),
        );
        for index in [1, 2] {
            let identity = credit_frontier_target(&fixture.binding, index);
            assert_eq!(
                fixture.binding.accepted_reinjected_data_in_flight_bytes_at(
                    ServerReinjectionOutputIdentity {
                        key: identity.key,
                        incarnation: identity.incarnation
                    },
                ),
                4096,
                "old and new copies keep their own current debt"
            );
        }
        let next_ready = fixture.receivers[2]
            .writer_ready_boundary(target.path_instance_id)
            .unwrap();
        assert!(
            fixture
                .sender
                .commit_prepared_recovery(
                    &fixture.binding,
                    &fixture.stream,
                    &candidate,
                    next_ready,
                    None,
                )
                .is_err(),
            "the accepted slot is not renewed by another frontier query"
        );
        assert!(next_ready.receipt().is_current());
    }
}
