use super::*;
use crate::model::admission::BulkCandidatePosition;
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey, PathPolicy};
use crate::protocol::{ConfiguredMemberSlot, PathId, UnderlayProtocol};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::stream::response::{
    ResponseOutputAttachment, ResponseOutputAttachmentState, ResponsePathDetachOutcome,
    ResponseStreamAttachOutcome,
};
use crate::scheduler::TrafficClass;
use crate::transport::RateHint;

fn exact(path_id: u16, instance: u64, incarnation: u64) -> ResponseAcquisitionOutputId {
    ResponseAcquisitionOutputId {
        key: CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(path_id),
        },
        path_instance_id: CarrierPathInstanceId::from_raw(instance),
        incarnation,
    }
}

fn plan(
    trigger_bytes: u64,
    candidate_total: u8,
    phase: StreamAttachmentPhase,
    candidate_ordinal: u8,
) -> StreamReturnPlan {
    StreamReturnPlan {
        trigger_bytes,
        candidate_total,
        candidate_tier: PathUsage::Available,
        phase,
        candidate_ordinal,
    }
}

#[test]
fn canonical_singleton_is_ready_and_preserves_the_unbounded_old_sequence() {
    let opening = exact(1, 1, 1);
    let state = ResponseStartupPlanState::from_initial_open(
        plan(0, 1, StreamAttachmentPhase::Startup, 0),
        opening,
    )
    .expect("canonical singleton");

    assert_eq!(state.fresh_data_limit(0, 208), Some(208));
    assert_eq!(state.fresh_data_limit(208, 14_600), Some(14_600));
    assert_eq!(state.fresh_data_limit(14_808, 65_536), Some(65_536));
    assert!(
        state
            .prepare_attachment(plan(0, 1, StreamAttachmentPhase::Startup, 0), opening,)
            .is_ok(),
        "same exact initial retry is idempotent",
    );
    assert!(
        state
            .prepare_attachment(
                plan(0, 1, StreamAttachmentPhase::Startup, 0),
                exact(1, 2, 2),
            )
            .is_err(),
        "a successor cannot inherit the singleton ordinal",
    );
}

#[test]
fn backup_only_singleton_is_canonical_and_ungated() {
    let opening = exact(1, 1, 1);
    let state = ResponseStartupPlanState::from_initial_open(
        StreamReturnPlan {
            trigger_bytes: 0,
            candidate_total: 1,
            candidate_tier: PathUsage::Backup,
            phase: StreamAttachmentPhase::Startup,
            candidate_ordinal: 0,
        },
        opening,
    )
    .expect("backup-only singleton");

    assert_eq!(state.fresh_data_limit(0, 65_536), Some(65_536));
    assert!(
        state
            .prepare_attachment(
                StreamReturnPlan {
                    trigger_bytes: 0,
                    candidate_total: 1,
                    candidate_tier: PathUsage::Backup,
                    phase: StreamAttachmentPhase::Ordinary,
                    candidate_ordinal: 0,
                },
                exact(2, 2, 1),
            )
            .is_ok(),
    );
}

#[test]
fn unresolved_allowance_is_cumulative_and_never_ack_refilling() {
    let state = ResponseStartupPlanState::from_initial_open(
        plan(58_400, 3, StreamAttachmentPhase::Startup, 0),
        exact(1, 1, 1),
    )
    .expect("multipath startup plan");

    assert_eq!(state.fresh_data_limit(0, 208), Some(208));
    assert_eq!(state.fresh_data_limit(208, 14_600), Some(14_600));
    assert_eq!(state.fresh_data_limit(14_808, 65_536), Some(43_592));
    assert_eq!(state.fresh_data_limit(58_400, 1), None);
    // Data ACK is deliberately absent from this API: the cumulative unique
    // next offset is the sole coordinate and cannot be refunded.
    assert_eq!(state.fresh_data_limit(58_400, 65_536), None);
}

#[test]
fn ordinary_pre_final_attach_does_not_enroll_and_exact_final_is_absorbing() {
    let opening = exact(1, 1, 1);
    let alternate = exact(2, 2, 1);
    let mut state = ResponseStartupPlanState::from_initial_open(
        plan(58_400, 2, StreamAttachmentPhase::Startup, 0),
        opening,
    )
    .expect("multipath startup plan");

    let ordinary = state
        .prepare_attachment(
            plan(58_400, 2, StreamAttachmentPhase::Ordinary, 0),
            alternate,
        )
        .expect("ordinary recovery attachment remains legal before final");
    state.commit_attachment(ordinary);
    assert!(
        state.finalize_for_test(&[0, 1]).is_err(),
        "ordinary attachment cannot silently enroll a startup ordinal",
    );

    let startup = state
        .prepare_attachment(
            plan(58_400, 2, StreamAttachmentPhase::Startup, 1),
            alternate,
        )
        .expect("exact startup enrollment");
    state.commit_attachment(startup);
    assert_eq!(
        state.finalize_for_test(&[0, 1]).expect("exact final"),
        ResponseStartupFinalOutcome::Finalized {
            withdrawn_outputs: Vec::new(),
        },
    );
    assert_eq!(state.fresh_data_limit(58_400, 65_536), Some(65_536));
    assert_eq!(
        state
            .finalize_for_test(&[0, 1])
            .expect("equal duplicate final"),
        ResponseStartupFinalOutcome::Duplicate,
    );
    assert!(state.finalize_for_test(&[0]).is_err());
    assert!(
        state
            .prepare_attachment(
                plan(58_400, 2, StreamAttachmentPhase::Startup, 1),
                alternate,
            )
            .is_err(),
        "startup enrollment cannot rearm after final",
    );
}

#[test]
fn ordinary_attachment_rejects_noncanonical_ignored_ordinal() {
    let opening = exact(1, 1, 1);
    let state = ResponseStartupPlanState::from_initial_open(
        plan(58_400, 2, StreamAttachmentPhase::Startup, 0),
        opening,
    )
    .expect("multipath startup plan");

    assert!(
        state
            .prepare_attachment(
                plan(58_400, 2, StreamAttachmentPhase::Ordinary, 1),
                exact(2, 2, 1),
            )
            .is_err(),
        "ignored ordinary ordinal still has one canonical encoding",
    );
}

#[test]
fn one_exact_attachment_cannot_enroll_two_startup_ordinals() {
    let opening = exact(1, 1, 1);
    let mut state = ResponseStartupPlanState::from_initial_open(
        plan(58_400, 2, StreamAttachmentPhase::Startup, 0),
        opening,
    )
    .expect("multipath startup plan");

    assert!(
        state
            .prepare_attachment(plan(58_400, 2, StreamAttachmentPhase::Startup, 1), opening,)
            .is_err(),
        "one exact attachment cannot satisfy two frozen candidate slots",
    );
    assert!(
        state.finalize_for_test(&[0, 1]).is_err(),
        "rejected duplicate exact enrollment cannot enter the transcript",
    );
}

#[test]
fn final_rejects_out_of_range_and_unbound_but_empty_is_exact() {
    let opening = exact(1, 1, 1);
    let mut state = ResponseStartupPlanState::from_initial_open(
        plan(58_400, 2, StreamAttachmentPhase::Startup, 0),
        opening,
    )
    .expect("multipath startup plan");

    assert!(state.finalize_for_test(&[2]).is_err());
    assert!(state.finalize_for_test(&[1]).is_err());
    assert_eq!(
        state
            .finalize_for_test(&[])
            .expect("zero accepted attachments is an exact final transcript"),
        ResponseStartupFinalOutcome::Finalized {
            withdrawn_outputs: Vec::new(),
        },
    );
    assert_eq!(
        state.finalize_for_test(&[]).expect("equal empty duplicate"),
        ResponseStartupFinalOutcome::Duplicate,
    );
    assert!(state.finalize_for_test(&[0]).is_err());
}

#[test]
fn exact_enrollment_survives_detach_before_final_and_duplicate_is_stable() {
    let (opening_commands, _opening_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        crate::protocol::SessionId(77),
        UnderlayProtocol::Tcp,
        PathId(0),
        opening_commands,
        TrafficClass::Throughput,
    );
    let opening_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    binding.install_unresolved_response_startup_for_test(
        58_400,
        2,
        PathUsage::Available,
        opening_key,
    );

    let alternate_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let alternate_instance = super::super::next_server_carrier_path_instance_id();
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding
            .attach_output_with_return_plan_if_session_active(
                ResponseOutputAttachment {
                    key: alternate_key,
                    path_instance_id: alternate_instance,
                    configured_slot: ConfiguredMemberSlot(alternate_key.path_id.0),
                    local_policy: PathPolicy::default(),
                    startup_rate_prior: RateHint::Unknown,
                    commands: alternate_commands,
                    state: ResponseOutputAttachmentState::default(),
                },
                plan(58_400, 2, StreamAttachmentPhase::Startup, 1),
            )
            .expect("atomic output enrollment"),
        ResponseStreamAttachOutcome::Attached,
    );
    assert!(matches!(
        binding.begin_path_detach(alternate_key, alternate_instance),
        Some(ResponsePathDetachOutcome::Begun(_)),
    ));

    assert_eq!(
        binding
            .finalize_response_startup_plan(&[0, 1])
            .expect("historical enrollment survives detach"),
        ResponseStartupFinalOutcome::Finalized {
            withdrawn_outputs: Vec::new(),
        },
    );
    assert_eq!(
        binding
            .finalize_response_startup_plan(&[0, 1])
            .expect("equal duplicate remains stable"),
        ResponseStartupFinalOutcome::Duplicate,
    );
    assert!(binding.finalize_response_startup_plan(&[0]).is_err());
    assert_eq!(
        binding.response_startup_fresh_data_limit(58_400, 65_536),
        Some(65_536),
    );
}

#[test]
fn rejected_startup_enrollment_publishes_nothing_and_burns_no_exact_identity() {
    let (opening_commands, _opening_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        crate::protocol::SessionId(79),
        UnderlayProtocol::Tcp,
        PathId(0),
        opening_commands,
        TrafficClass::Throughput,
    );
    let opening_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    binding.install_unresolved_response_startup_for_test(
        58_400,
        2,
        PathUsage::Available,
        opening_key,
    );
    let alternate_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let alternate_instance = super::super::next_server_carrier_path_instance_id();
    let (invalid_commands, _invalid_receivers) = reliable_path_command_channels(8);
    let before_membership = binding.output_membership_generation();
    let before_model = binding.response_model_generation();
    assert!(
        binding
            .attach_output_with_return_plan_if_session_active(
                ResponseOutputAttachment {
                    key: alternate_key,
                    path_instance_id: alternate_instance,
                    configured_slot: ConfiguredMemberSlot(alternate_key.path_id.0),
                    local_policy: PathPolicy::default(),
                    startup_rate_prior: RateHint::Unknown,
                    commands: invalid_commands,
                    state: ResponseOutputAttachmentState::default(),
                },
                plan(58_401, 2, StreamAttachmentPhase::Startup, 1),
            )
            .is_err(),
        "a changed frozen signature is rejected before publication",
    );
    assert_eq!(binding.output_membership_generation(), before_membership);
    assert_eq!(binding.response_model_generation(), before_model);
    assert_eq!(
        binding
            .sender_path_targets(TrafficClass::Throughput, 1)
            .len(),
        1
    );

    let (valid_commands, _valid_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding
            .attach_output_with_return_plan_if_session_active(
                ResponseOutputAttachment {
                    key: alternate_key,
                    path_instance_id: alternate_instance,
                    configured_slot: ConfiguredMemberSlot(alternate_key.path_id.0),
                    local_policy: PathPolicy::default(),
                    startup_rate_prior: RateHint::Unknown,
                    commands: valid_commands,
                    state: ResponseOutputAttachmentState::default(),
                },
                plan(58_400, 2, StreamAttachmentPhase::Startup, 1),
            )
            .expect("the valid retry keeps the unspent identity"),
        ResponseStreamAttachOutcome::Attached,
    );
    let alternate = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|candidate| candidate.observation.key == alternate_key)
        .expect("valid enrollment publishes output");
    assert_eq!(alternate.observation.incarnation, 2);
}

struct EnrolledBindingFixture {
    binding: std::sync::Arc<ResponseStreamBinding>,
    opening_key: CarrierPathKey,
    opening_instance: CarrierPathInstanceId,
    _opening_receivers: crate::runtime::path::commands::ReliablePathCommandReceivers,
    alternate_key: CarrierPathKey,
    alternate_instance: CarrierPathInstanceId,
    alternate_receivers: crate::runtime::path::commands::ReliablePathCommandReceivers,
}

fn binding_with_enrolled_alternate() -> EnrolledBindingFixture {
    let (opening_commands, opening_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        crate::protocol::SessionId(78),
        UnderlayProtocol::Tcp,
        PathId(0),
        opening_commands,
        TrafficClass::Throughput,
    );
    let opening_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    binding.install_unresolved_response_startup_for_test(
        58_400,
        2,
        PathUsage::Available,
        opening_key,
    );
    let opening_instance = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|candidate| candidate.observation.key == opening_key)
        .expect("opening output")
        .observation
        .path_instance_id;
    let alternate_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let alternate_instance = super::super::next_server_carrier_path_instance_id();
    let (alternate_commands, alternate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding
            .attach_output_with_return_plan_if_session_active(
                ResponseOutputAttachment {
                    key: alternate_key,
                    path_instance_id: alternate_instance,
                    configured_slot: ConfiguredMemberSlot(alternate_key.path_id.0),
                    local_policy: PathPolicy::default(),
                    startup_rate_prior: RateHint::Unknown,
                    commands: alternate_commands,
                    state: ResponseOutputAttachmentState::default(),
                },
                plan(58_400, 2, StreamAttachmentPhase::Startup, 1),
            )
            .expect("atomic output enrollment"),
        ResponseStreamAttachOutcome::Attached,
    );
    EnrolledBindingFixture {
        binding,
        opening_key,
        opening_instance,
        _opening_receivers: opening_receivers,
        alternate_key,
        alternate_instance,
        alternate_receivers,
    }
}

#[test]
fn final_atomically_withdraws_omitted_output_before_removing_the_sender_cap() {
    let fixture = binding_with_enrolled_alternate();
    let binding = &fixture.binding;
    let alternate_key = fixture.alternate_key;
    let alternate_instance = fixture.alternate_instance;
    let target: super::super::ResponseDispatchTarget = binding
        .sender_path_targets(TrafficClass::Throughput, 1024)
        .into_iter()
        .find(|candidate| candidate.observation.key == alternate_key)
        .expect("enrolled output was schedulable before final")
        .into();

    let outcome = binding
        .finalize_response_startup_plan(&[0])
        .expect("historical retained membership");
    let ResponseStartupFinalOutcome::Finalized { withdrawn_outputs } = outcome else {
        panic!("first final must transition");
    };
    assert_eq!(
        withdrawn_outputs,
        vec![ResponseAcquisitionOutputId {
            key: target.key,
            path_instance_id: target.path_instance_id,
            incarnation: target.incarnation,
        }]
    );
    assert!(
        binding
            .sender_path_targets(TrafficClass::Throughput, 1024)
            .into_iter()
            .all(|candidate| candidate.observation.key != alternate_key),
        "omitted exact output cannot remain eligible for fresh placement",
    );
    assert!(matches!(
        binding.try_enqueue_data_frame_for_dispatch_target(
            &target,
            &super::super::test_support::stream_data_frame(1024),
            TrafficClass::Throughput,
            binding.response_model_generation(),
            BulkCandidatePosition::FirstPath,
        ),
        Err(crate::runtime::RuntimeError::SenderServiceBlocked),
    ));
    assert!(matches!(
        binding.begin_path_detach(alternate_key, alternate_instance),
        Some(ResponsePathDetachOutcome::Pending(_)),
    ));
    assert_eq!(
        binding
            .finalize_response_startup_plan(&[0])
            .expect("equal duplicate"),
        ResponseStartupFinalOutcome::Duplicate,
    );
}

#[test]
fn detach_before_final_and_final_before_detach_have_the_same_withdrawn_state() {
    let fixture = binding_with_enrolled_alternate();
    let binding = &fixture.binding;
    let alternate_key = fixture.alternate_key;
    let alternate_instance = fixture.alternate_instance;
    assert!(matches!(
        binding.begin_path_detach(alternate_key, alternate_instance),
        Some(ResponsePathDetachOutcome::Begun(_)),
    ));
    assert_eq!(
        binding
            .finalize_response_startup_plan(&[0])
            .expect("historical enrollment survives earlier detach"),
        ResponseStartupFinalOutcome::Finalized {
            withdrawn_outputs: Vec::new(),
        },
    );
    assert!(matches!(
        binding.begin_path_detach(alternate_key, alternate_instance),
        Some(ResponsePathDetachOutcome::Pending(_)),
    ));
    assert_eq!(
        binding.response_startup_fresh_data_limit(58_400, 65_536),
        Some(65_536),
    );
}

#[test]
fn retained_dead_output_is_valid_historical_acceptance() {
    let fixture = binding_with_enrolled_alternate();
    drop(fixture.alternate_receivers);
    let binding = &fixture.binding;

    assert_eq!(
        binding
            .finalize_response_startup_plan(&[0, 1])
            .expect("liveness cannot rewrite peer acceptance"),
        ResponseStartupFinalOutcome::Finalized {
            withdrawn_outputs: Vec::new(),
        },
    );
}

#[test]
fn empty_final_withdraws_every_enrolled_ghost_exactly_once() {
    let fixture = binding_with_enrolled_alternate();
    let binding = &fixture.binding;
    let outcome = binding
        .finalize_response_startup_plan(&[])
        .expect("no accepted startup attachment");
    let ResponseStartupFinalOutcome::Finalized { withdrawn_outputs } = outcome else {
        panic!("first final must transition");
    };
    assert_eq!(withdrawn_outputs.len(), 2);
    assert!(
        binding
            .sender_path_targets(TrafficClass::Throughput, 1)
            .is_empty()
    );
    assert_eq!(
        binding
            .finalize_response_startup_plan(&[])
            .expect("equal empty duplicate"),
        ResponseStartupFinalOutcome::Duplicate,
    );
    assert!(binding.finalize_response_startup_plan(&[0]).is_err());
}

#[test]
fn omitted_ghost_range_crosses_the_membership_boundary_exactly_once() {
    let final_first = binding_with_enrolled_alternate();
    let opening_target: super::super::ResponseDispatchTarget = final_first
        .binding
        .sender_path_targets(TrafficClass::Throughput, 58_400)
        .into_iter()
        .find(|candidate| candidate.observation.key == final_first.opening_key)
        .expect("opening ordinal zero target")
        .into();
    final_first.binding.record_original_flight(
        final_first.opening_key,
        &super::super::test_support::stream_data_frame(58_400),
    );

    let outcome = final_first
        .binding
        .finalize_response_startup_plan(&[1])
        .expect("client retained only sibling ordinal one");
    let ResponseStartupFinalOutcome::Finalized { withdrawn_outputs } = outcome else {
        panic!("first final must transition");
    };
    assert_eq!(
        withdrawn_outputs,
        vec![ResponseAcquisitionOutputId {
            key: opening_target.key,
            path_instance_id: opening_target.path_instance_id,
            incarnation: opening_target.incarnation,
        }],
    );
    assert!(matches!(
        final_first
            .binding
            .try_enqueue_data_frame_for_dispatch_target(
                &opening_target,
                &super::super::test_support::stream_data_frame_at(58_400, 1),
                TrafficClass::Throughput,
                final_first.binding.response_model_generation(),
                BulkCandidatePosition::FirstPath,
            ),
        Err(crate::runtime::RuntimeError::SenderServiceBlocked),
    ));
    let expected = vec![crate::protocol::OffsetRange {
        start: 0,
        end: 58_400,
    }];
    assert_eq!(
        final_first.binding.uncovered_failed_original_ranges(),
        expected,
        "startup final removes the omitted ghost from current membership and transfers recovery",
    );
    final_first.binding.complete_path_detach(
        opening_target.key,
        opening_target.path_instance_id,
        opening_target.incarnation,
    );
    final_first.binding.complete_path_detach(
        opening_target.key,
        opening_target.path_instance_id,
        opening_target.incarnation,
    );
    assert_eq!(
        final_first.binding.uncovered_failed_original_ranges(),
        expected,
        "a later duplicate physical detach cannot duplicate recovery debt",
    );
    assert_eq!(
        final_first
            .binding
            .begin_path_detach(final_first.opening_key, final_first.opening_instance),
        None,
    );

    let detach_first = binding_with_enrolled_alternate();
    detach_first.binding.record_original_flight(
        detach_first.opening_key,
        &super::super::test_support::stream_data_frame(58_400),
    );
    let incarnation = match detach_first
        .binding
        .begin_path_detach(detach_first.opening_key, detach_first.opening_instance)
        .expect("physical detach begins first")
    {
        ResponsePathDetachOutcome::Begun(incarnation) => incarnation,
        ResponsePathDetachOutcome::Pending(_) => panic!("first detach cannot already be pending"),
    };
    assert_eq!(
        detach_first.binding.uncovered_failed_original_ranges(),
        expected,
        "detach start is the exact current-membership transfer boundary",
    );
    assert_eq!(
        detach_first
            .binding
            .finalize_response_startup_plan(&[1])
            .expect("historical ordinal zero may already be detaching"),
        ResponseStartupFinalOutcome::Finalized {
            withdrawn_outputs: Vec::new(),
        },
    );
    detach_first.binding.complete_path_detach(
        detach_first.opening_key,
        detach_first.opening_instance,
        incarnation,
    );
    assert_eq!(
        detach_first.binding.uncovered_failed_original_ranges(),
        expected,
        "DETACH then FINAL converges to the same one-copy recovery range",
    );
}
