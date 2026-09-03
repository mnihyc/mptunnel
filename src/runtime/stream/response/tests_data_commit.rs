use super::super::ResponseStreamBinding;
use super::super::attachment::{ResponseDispatchTarget, ResponseProductRateEpoch};
use super::super::evidence::server_output_product_assignment_qualified;
use super::super::snapshot::server_bulk_output_snapshot_at;
use super::super::test_support::{
    native_response_binding_fixture, qualify_product_assignment, stream_data_frame,
    stream_data_frame_at,
};
use crate::model::admission::{BulkCandidatePosition, bulk_original_data_assignment_authority};
use crate::model::capacity::{
    RELIABLE_INITIAL_WINDOW_PACKETS, reliable_bulk_product_windows,
    reliable_path_startup_sample_limit_bytes,
};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, OffsetRange, PathId, SessionId, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    reliable_path_command_channels, reliable_path_command_pending_bytes,
    try_recv_reliable_path_command,
};
use crate::scheduler::TrafficClass;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Fixture {
    binding: Arc<ResponseStreamBinding>,
    key: CarrierPathKey,
    commands: ReliablePathCommandSender,
    receivers: ReliablePathCommandReceivers,
    target: ResponseDispatchTarget,
}

fn fixture(queue_capacity: usize) -> Fixture {
    fixture_with_limits(queue_capacity, MuxLimits::default())
}

fn fixture_with_limits(queue_capacity: usize, mux_limits: MuxLimits) -> Fixture {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (commands, receivers) = reliable_path_command_channels(queue_capacity);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(188),
        key.underlay,
        key.path_id,
        commands.clone(),
        TrafficClass::Throughput,
        mux_limits,
    );
    let target = binding
        .sender_path_targets(TrafficClass::Throughput, 1024)
        .into_iter()
        .next()
        .expect("initial response path is schedulable")
        .into();
    Fixture {
        binding,
        key,
        commands,
        receivers,
        target,
    }
}

fn attached_target(
    fixture: &Fixture,
    key: CarrierPathKey,
) -> (
    ReliablePathCommandSender,
    ReliablePathCommandReceivers,
    ResponseDispatchTarget,
) {
    let (commands, receivers) = reliable_path_command_channels(8);
    fixture.binding.attach(
        key.underlay,
        key.path_id,
        commands.clone(),
        TrafficClass::Throughput,
    );
    let target = fixture
        .binding
        .sender_path_targets(TrafficClass::Throughput, 1024)
        .into_iter()
        .find(|candidate| candidate.observation.key == key)
        .expect("attached response path is schedulable")
        .into();
    (commands, receivers, target)
}

fn enqueue(
    fixture: &Fixture,
    target: &ResponseDispatchTarget,
    frame: &Frame,
    generation: u64,
) -> Result<(), RuntimeError> {
    enqueue_on_lane(fixture, target, frame, TrafficClass::Throughput, generation)
}

fn enqueue_on_lane(
    fixture: &Fixture,
    target: &ResponseDispatchTarget,
    frame: &Frame,
    lane: TrafficClass,
    generation: u64,
) -> Result<(), RuntimeError> {
    fixture.binding.try_enqueue_data_frame_for_dispatch_target(
        target,
        frame,
        lane,
        generation,
        BulkCandidatePosition::FirstPath,
    )
}

#[test]
fn stale_attachment_or_model_generation_cannot_commit() {
    let mut fixture = fixture(2);
    let frame = stream_data_frame(1024);
    let generation = fixture.binding.response_model_generation();
    let mut stale_target = fixture.target;
    stale_target.incarnation = stale_target.incarnation.wrapping_add(1);

    assert!(matches!(
        enqueue(&fixture, &stale_target, &frame, generation),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    fixture.binding.set_sender_queue_bytes(1);
    assert!(matches!(
        enqueue(&fixture, &fixture.target, &frame, generation),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&frame)
            .is_empty()
    );
    assert!(try_recv_reliable_path_command(&mut fixture.receivers).is_none());
}

#[test]
fn full_carrier_queue_rolls_back_without_publishing_flight() {
    let mut fixture = fixture(1);
    let frame = stream_data_frame(1024);
    fixture
        .commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame_at(1024, 1024),
            TrafficClass::Throughput,
        )
        .expect("fill carrier queue");
    let generation = fixture.binding.response_model_generation();

    assert!(matches!(
        enqueue(&fixture, &fixture.target, &frame, generation),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(fixture.binding.response_model_generation(), generation);
    assert!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&frame)
            .is_empty()
    );
    let filler = try_recv_reliable_path_command(&mut fixture.receivers)
        .expect("only the queue filler was published");
    fixture
        .receivers
        .release_pending_command_bytes(reliable_path_command_pending_bytes(&filler));
    assert!(try_recv_reliable_path_command(&mut fixture.receivers).is_none());
}

#[test]
fn native_original_final_precommit_rejects_stale_activation_after_real_reservation() {
    let mut fixture = native_response_binding_fixture(1, Some(120_000_000));
    let target: ResponseDispatchTarget = fixture
        .binding
        .sender_path_targets(TrafficClass::Throughput, 4_096)
        .into_iter()
        .next()
        .expect("current fenced Native response target")
        .into();
    let expected_stamp = target
        .native_authority_stamp
        .expect("Native response target carries its exact stamp");
    assert_eq!(
        fixture.authority.stamp().expect("current authority stamp"),
        expected_stamp,
    );
    let frame = stream_data_frame(4_096);
    let generation = fixture.binding.response_model_generation();
    let qualification_before = fixture
        .binding
        .outputs
        .lock()
        .expect("test response outputs lock")
        .entries[0]
        .product_qualification
        .invariant();

    let result = fixture
        .binding
        .try_enqueue_data_frame_for_dispatch_target_with_apply_clock(
            &target,
            &frame,
            TrafficClass::Throughput,
            generation,
            BulkCandidatePosition::FirstPath,
            Instant::now,
            || {
                assert!(
                    fixture.commands.pending_bytes() > 0,
                    "the exact writer slot and byte charge exist before final A/G validation",
                );
                fixture
                    .authority
                    .advance_transport_activation_for_test(2)
                    .expect("advance Quinn A after the real reservation");
            },
        );

    assert!(matches!(result, Err(RuntimeError::SenderServiceBlocked)));
    assert_eq!(
        fixture.authority.stamp().expect("coordinator stamp"),
        expected_stamp,
        "advancing only the transport fence invalidates A without rewriting central G",
    );
    assert_eq!(fixture.commands.pending_bytes(), 0);
    assert_eq!(fixture.binding.response_model_generation(), generation);
    assert!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&frame)
            .is_empty(),
        "a stale Native A cannot publish Product ownership",
    );
    {
        let outputs = fixture
            .binding
            .outputs
            .lock()
            .expect("test response outputs lock");
        assert_eq!(outputs.original_data_in_flight_bytes, 0);
        assert_eq!(outputs.entries[0].original_data_in_flight_bytes, 0);
        assert_eq!(outputs.entries[0].bytes_in_flight, 0);
        assert_eq!(
            outputs.entries[0].product_qualification.invariant(),
            qualification_before,
        );
    }
    assert!(try_recv_reliable_path_command(&mut fixture.receivers).is_none());

    let reservation = fixture
        .commands
        .try_reserve_admitted_frame(frame.clone(), TrafficClass::Throughput)
        .expect("rejected final precommit refunds the one-slot writer queue");
    assert!(fixture.commands.pending_bytes() > 0);
    drop(reservation);
    assert_eq!(fixture.commands.pending_bytes(), 0);
}

#[test]
fn qualification_rejection_rolls_back_reserved_command_and_all_product_state() {
    let mux_limits = MuxLimits {
        max_reliable_relay_chunk_bytes: 1024,
        ..MuxLimits::default()
    };
    let mut fixture = fixture_with_limits(2, mux_limits);
    let oversized_quantum = stream_data_frame(2048);
    let generation = fixture.binding.response_model_generation();
    let ledger_before = fixture
        .binding
        .outputs
        .lock()
        .expect("test response outputs lock")
        .entries[0]
        .product_qualification
        .invariant();

    assert!(matches!(
        enqueue(&fixture, &fixture.target, &oversized_quantum, generation,),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(fixture.commands.pending_bytes(), 0);
    assert_eq!(fixture.commands.active_flow_counts(), (0, 0));
    assert_eq!(fixture.binding.response_model_generation(), generation);
    assert!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&oversized_quantum)
            .is_empty()
    );
    let outputs = fixture
        .binding
        .outputs
        .lock()
        .expect("test response outputs lock");
    assert_eq!(outputs.original_data_in_flight_bytes, 0);
    assert_eq!(outputs.entries[0].original_data_in_flight_bytes, 0);
    assert_eq!(outputs.entries[0].bytes_in_flight, 0);
    assert_eq!(
        outputs.entries[0].product_qualification.invariant(),
        ledger_before,
        "a rejected tag cannot freeze or otherwise mutate qualification"
    );
    drop(outputs);
    assert!(try_recv_reliable_path_command(&mut fixture.receivers).is_none());
}

#[test]
fn successful_commit_publishes_exact_flight_before_carrier_work() {
    let mut fixture = fixture(1);
    let frame = stream_data_frame_at(4096, 1536);
    let generation = fixture.binding.response_model_generation();

    enqueue(&fixture, &fixture.target, &frame, generation).expect("commit response data");

    assert_eq!(
        fixture.binding.response_model_generation(),
        generation.wrapping_add(1)
    );
    assert_eq!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&frame),
        vec![(fixture.key, fixture.target.incarnation)]
    );
    let outputs = fixture
        .binding
        .outputs
        .lock()
        .expect("test response outputs lock");
    let output = outputs.entries.first().expect("initial response output");
    assert_eq!(output.original_data_in_flight_bytes, 1536);
    assert_eq!(outputs.original_data_in_flight_bytes, 1536);
    assert_eq!(output.bytes_in_flight, 1536);
    drop(outputs);
    let flights = fixture
        .binding
        .flights
        .lock()
        .expect("test response flight lock");
    let flight = flights
        .get(&4096)
        .and_then(|flights| flights.first())
        .expect("exact range flight");
    assert_eq!(flight.key, fixture.key);
    assert_eq!(flight.output_incarnation, fixture.target.incarnation);
    assert_eq!(flight.end, 4096 + 1536);
    assert_eq!(flight.bytes, 1536);
    drop(flights);
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 4096,
            ..
        }))
    ));
}

#[test]
fn response_final_acquisition_quantum_tags_only_the_remaining_prefix() {
    let fixture = fixture(2);
    let floor = reliable_path_startup_sample_limit_bytes(MuxLimits::default());
    let first_bytes = usize::try_from(floor - 1).expect("test qualification floor");
    let first = stream_data_frame_at(0, first_bytes);
    enqueue(
        &fixture,
        &fixture.target,
        &first,
        fixture.binding.response_model_generation(),
    )
    .expect("commit the first bounded acquisition quantum");
    fixture
        .binding
        .release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: floor - 1,
        }]);

    {
        let outputs = fixture
            .binding
            .outputs
            .lock()
            .expect("test response outputs lock");
        let ledger = outputs.entries[0].product_qualification.invariant();
        assert_eq!(ledger.floor_bytes, Some(floor));
        assert_eq!(ledger.verified_bytes, floor - 1);
        assert_eq!(ledger.outstanding_tag_bytes, 0);
        assert!(!outputs.entries[0].product_qualification.qualified());
        assert!(ledger.holds());
    }

    let final_offset = floor - 1;
    let final_bytes = 4096_usize;
    let final_quantum = stream_data_frame_at(final_offset, final_bytes);
    enqueue(
        &fixture,
        &fixture.target,
        &final_quantum,
        fixture.binding.response_model_generation(),
    )
    .expect("commit a complete final quantum larger than the one-byte deficit");

    {
        let outputs = fixture
            .binding
            .outputs
            .lock()
            .expect("test response outputs lock");
        let entry = &outputs.entries[0];
        let ledger = entry.product_qualification.invariant();
        assert_eq!(ledger.verified_bytes, floor - 1);
        assert_eq!(ledger.outstanding_tag_bytes, 1);
        assert_eq!(entry.original_data_in_flight_bytes, final_bytes as u64);
        assert!(ledger.holds(), "the useful surplus cannot inflate M");
    }

    fixture
        .binding
        .release_normalized_acked_ranges(&[OffsetRange {
            start: floor,
            end: final_offset + final_bytes as u64,
        }]);
    {
        let outputs = fixture
            .binding
            .outputs
            .lock()
            .expect("test response outputs lock");
        let entry = &outputs.entries[0];
        assert!(entry.original_data_acked_bytes >= floor);
        assert!(
            !entry.product_qualification.qualified(),
            "ACK diagnostics for untagged surplus cannot qualify assignment"
        );
        assert_eq!(
            entry
                .product_qualification
                .invariant()
                .outstanding_tag_bytes,
            1
        );
    }

    fixture
        .binding
        .release_normalized_acked_ranges(&[OffsetRange {
            start: final_offset,
            end: floor,
        }]);
    let outputs = fixture
        .binding
        .outputs
        .lock()
        .expect("test response outputs lock");
    let entry = &outputs.entries[0];
    assert!(entry.product_qualification.qualified());
    let ledger = entry.product_qualification.invariant();
    assert_eq!(ledger.verified_bytes, floor);
    assert_eq!(ledger.outstanding_tag_bytes, 0);
    assert!(ledger.holds());
}

#[test]
fn shared_product_window_survives_detach_and_only_data_ack_reopens_commit() {
    let stream_window = 64 * 1024_u64;
    let mux_limits = MuxLimits {
        max_stream_window_bytes: stream_window,
        max_repair_bytes: stream_window as usize,
        max_reorder_bytes: stream_window as usize,
        max_path_flight_bytes: stream_window as usize,
        ..MuxLimits::default()
    };
    let fixture = fixture_with_limits(8, mux_limits);
    let alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (alternate_commands, mut alternate_receivers, alternate_target) =
        attached_target(&fixture, alternate);
    {
        let mut outputs = fixture
            .binding
            .outputs
            .lock()
            .expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == fixture.key)
            .expect("initial output");
        qualify_product_assignment(entry, mux_limits);
    }
    let original = stream_data_frame_at(0, stream_window as usize);
    fixture
        .binding
        .record_original_flight(fixture.key, &original);

    let (path_instance_id, incarnation) = {
        let outputs = fixture
            .binding
            .outputs
            .lock()
            .expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == fixture.key)
            .expect("initial output");
        assert_eq!(outputs.original_data_in_flight_bytes, stream_window);
        (entry.path_instance_id, entry.incarnation)
    };
    fixture
        .binding
        .begin_path_detach(fixture.key, path_instance_id)
        .expect("begin initial detach");
    {
        let outputs = fixture
            .binding
            .outputs
            .lock()
            .expect("test response outputs lock");
        let detached = outputs
            .detaching
            .iter()
            .find(|entry| entry.incarnation == incarnation)
            .expect("ordered detaching output");
        assert_eq!(detached.product_qualification.deficit_bytes(), None);
        assert!(!detached.product_qualification.qualified());
        assert_eq!(outputs.original_data_in_flight_bytes, stream_window);
    }
    fixture
        .binding
        .complete_path_detach(fixture.key, path_instance_id, incarnation);

    let next = stream_data_frame_at(stream_window, 1024);
    let blocked_generation = fixture.binding.response_model_generation();
    assert!(matches!(
        fixture.binding.try_enqueue_data_frame_for_dispatch_target(
            &alternate_target,
            &next,
            TrafficClass::Throughput,
            blocked_generation,
            BulkCandidatePosition::AdditionalPath,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(alternate_commands.pending_bytes(), 0);
    assert!(try_recv_reliable_path_command(&mut alternate_receivers).is_none());
    assert!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&next)
            .is_empty()
    );

    fixture
        .binding
        .release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: stream_window,
        }]);
    assert_eq!(
        fixture
            .binding
            .outputs
            .lock()
            .expect("test response outputs lock")
            .original_data_in_flight_bytes,
        0,
        "only the exact MPP DataACK releases shared Product debt",
    );
    fixture
        .binding
        .try_enqueue_data_frame_for_dispatch_target(
            &alternate_target,
            &next,
            TrafficClass::Throughput,
            fixture.binding.response_model_generation(),
            BulkCandidatePosition::FirstPath,
        )
        .expect("DataACK-released W admits the next exact output");
    assert!(matches!(
        try_recv_reliable_path_command(&mut alternate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset,
            ..
        })) if offset == stream_window
    ));
}

#[test]
fn rate_expiry_after_real_reservation_preserves_durable_additional_output_limit() {
    let per_output_window = 2 * 1024 * 1024_u64;
    let stream_window = 4 * per_output_window;
    let mux_limits = MuxLimits {
        max_stream_window_bytes: stream_window,
        max_repair_bytes: stream_window as usize,
        max_reorder_bytes: stream_window as usize,
        max_path_flight_bytes: per_output_window as usize,
        ..MuxLimits::default()
    };
    assert_eq!(
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes,
        per_output_window,
    );
    let fixture = fixture_with_limits(8, mux_limits);
    let alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (alternate_commands, mut alternate_receivers, alternate_target) =
        attached_target(&fixture, alternate);
    let observed_at = Instant::now();
    let expires_at = observed_at + Duration::from_secs(60);
    let fresh_at = expires_at - Duration::from_nanos(1);
    let sample_floor = reliable_path_startup_sample_limit_bytes(mux_limits);
    let (fresh_limit, expired_limit) = {
        let mut outputs = fixture
            .binding
            .outputs
            .lock()
            .expect("test response outputs lock");
        let data_level_queue_bytes = outputs.data_level_queue_bytes;
        let entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == alternate)
            .expect("alternate output");
        entry.product_rate_epoch = Some(ResponseProductRateEpoch {
            rate_bps: 100_000_000.0,
            sample_count: 1,
            sample_bytes: sample_floor,
            observed_at,
            expires_at,
        });
        entry.original_data_acked_bytes = sample_floor;
        entry.delivery_samples = RELIABLE_INITIAL_WINDOW_PACKETS as u32;
        qualify_product_assignment(entry, mux_limits);
        let product_assignment_qualified =
            server_output_product_assignment_qualified(entry, mux_limits);
        assert!(product_assignment_qualified);
        let fresh = server_bulk_output_snapshot_at(
            entry,
            data_level_queue_bytes,
            TrafficClass::Throughput,
            mux_limits,
            fresh_at,
        );
        let expired = server_bulk_output_snapshot_at(
            entry,
            data_level_queue_bytes,
            TrafficClass::Throughput,
            mux_limits,
            expires_at,
        );
        (
            bulk_original_data_assignment_authority(
                fresh,
                1024,
                mux_limits,
                BulkCandidatePosition::AdditionalPath,
                product_assignment_qualified,
            )
            .assignment_limit_bytes,
            bulk_original_data_assignment_authority(
                expired,
                1024,
                mux_limits,
                BulkCandidatePosition::AdditionalPath,
                product_assignment_qualified,
            )
            .assignment_limit_bytes,
        )
    };
    assert_eq!(fresh_limit, per_output_window);
    assert_eq!(
        expired_limit, fresh_limit,
        "numeric rate expiry cannot revoke durable exact-volume assignment qualification",
    );

    fixture
        .binding
        .record_original_flight(fixture.key, &stream_data_frame_at(0, 1));
    let retained_alternate_debt = expired_limit - 1024;
    let mut retained_offset = 1_u64;
    let mut retained_remaining = retained_alternate_debt;
    let quantum = crate::model::capacity::reliable_relay_buffer_len(mux_limits) as u64;
    while retained_remaining > 0 {
        let bytes = retained_remaining.min(quantum);
        fixture.binding.record_original_flight(
            alternate,
            &stream_data_frame_at(retained_offset, bytes as usize),
        );
        retained_offset += bytes;
        retained_remaining -= bytes;
    }
    let next_offset = 1 + retained_alternate_debt;
    let next = stream_data_frame_at(next_offset, 1024);
    let generation = fixture.binding.response_model_generation();
    let pending_before = alternate_commands.pending_bytes();

    fixture
        .binding
        .try_enqueue_data_frame_for_dispatch_target_with_apply_clock(
            &alternate_target,
            &next,
            TrafficClass::Throughput,
            generation,
            BulkCandidatePosition::AdditionalPath,
            || {
                assert!(
                    alternate_commands.pending_bytes() > pending_before,
                    "the real writer command must be reserved before evidence is revalidated",
                );
                expires_at
            },
            || {},
        )
        .expect("durable exact Product volume must survive rate expiry at commit");
    assert_eq!(
        alternate_commands.pending_bytes(),
        pending_before + 1024,
        "successful exact authority retains the command reservation",
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut alternate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { offset, .. }))
            if offset == next_offset
    ));
    assert_eq!(
        fixture
            .binding
            .original_flight_outputs_overlapping_frame(&next),
        vec![(alternate, alternate_target.incarnation)],
    );
}

#[test]
fn latency_commit_cannot_bypass_exact_product_headroom() {
    let mut fixture = fixture(2);
    let initial = fixture
        .binding
        .sender_path_targets(TrafficClass::Latency, 1024)
        .into_iter()
        .next()
        .expect("initial response target");
    let product_limit = initial.observation.snapshot.data_level_limit_bytes;
    // Fill the Product window with legal relay quanta. A single synthetic
    // frame spanning the whole Product limit is not a frame the runtime can
    // admit and would test the qualification quantum guard instead of the
    // intended exact-headroom boundary.
    let quantum = u64::try_from(fixture.binding.mux_limits.max_reliable_relay_chunk_bytes)
        .expect("test relay quantum");
    let mut offset = 0_u64;
    while offset < product_limit {
        let bytes = quantum.min(product_limit - offset);
        fixture.binding.record_original_flight(
            fixture.key,
            &stream_data_frame_at(offset, usize::try_from(bytes).expect("test frame quantum")),
        );
        offset += bytes;
    }
    let target: ResponseDispatchTarget = fixture
        .binding
        .sender_path_targets(TrafficClass::Latency, 1024)
        .into_iter()
        .next()
        .expect("attached response target")
        .into();
    let generation = fixture.binding.response_model_generation();
    let next = stream_data_frame_at(product_limit, 1024);

    assert!(matches!(
        enqueue_on_lane(&fixture, &target, &next, TrafficClass::Latency, generation,),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(try_recv_reliable_path_command(&mut fixture.receivers).is_none());
}

#[test]
fn latency_data_commit_is_not_blocked_behind_queued_bulk() {
    let mut fixture = fixture(1);
    let bulk = stream_data_frame_at(4096, 1024);
    fixture
        .commands
        .try_enqueue_admitted_frame(bulk, TrafficClass::Throughput)
        .expect("fill bulk queue");
    let latency = stream_data_frame_at(0, 128);
    let generation = fixture.binding.response_model_generation();

    enqueue_on_lane(
        &fixture,
        &fixture.target,
        &latency,
        TrafficClass::Latency,
        generation,
    )
    .expect("latency response uses independent priority capacity");

    let first =
        try_recv_reliable_path_command(&mut fixture.receivers).expect("latency response command");
    assert!(matches!(
        &first,
        ReliablePathCommand::SendFrame(Frame::StreamData { offset: 0, .. })
    ));
    fixture
        .receivers
        .release_pending_command_bytes(reliable_path_command_pending_bytes(&first));
    let second =
        try_recv_reliable_path_command(&mut fixture.receivers).expect("queued bulk command");
    assert!(matches!(
        second,
        ReliablePathCommand::SendFrame(Frame::StreamData { offset: 4096, .. })
    ));
}
