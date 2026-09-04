use super::{
    FixedNativeRateEpoch, FixedProductRateEpoch, FixedReliablePathOutput, ReliablePathStream,
    ReliablePathStreamHandle, ReliablePathStreamInput, ReliablePathStreamOutput,
    RequalificationAttempt, ServerReliableStreamEvent, TargetCarrierCapacityWait,
};
use crate::model::capacity::{
    MIN_RATE_SAMPLE_BYTES, PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS,
    reliable_bulk_product_windows,
};
use crate::model::carrier_rate_authority::{CarrierRateAuthorityBasis, CarrierRateAuthorityScope};
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::model::service_rate::{DirectionalServiceRate, DirectionalServiceRateScope};
use crate::model::work::CarrierWorkKind;
use crate::mux::MuxLimits;
use crate::protocol::PathMetricDirection;
use crate::protocol::frame::{reliable_stream_frame_accounted_bytes, reliable_stream_frame_extent};
use crate::protocol::{Frame, OffsetRange, PathId, ResetReason, StreamId, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::CarrierNativeWindowSample;
use crate::runtime::path::authority::NativeCarrierRateAuthorityHandle;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, reliable_path_command_channels,
    try_recv_reliable_path_command, try_recv_reliable_path_priority_command,
};
use crate::runtime::stream::reliable_stream_recv_progress_interval;
use crate::scheduler::{PathRateScope, PathSnapshot, TrafficClass};
use crate::transport::RateHint;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc};

fn stream_data_frame(payload_len: usize) -> Frame {
    stream_data_frame_at(0, payload_len)
}

fn stream_data_frame_at(offset: u64, payload_len: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(7),
        offset,
        payload: Bytes::from(vec![0x5a; payload_len]),
    }
}

fn refresh_native_shape(
    authority: &NativeCarrierRateAuthorityHandle,
    scope: CarrierRateAuthorityScope,
    activation: u64,
    operational_rate_bps: Option<u64>,
) {
    let _ = authority
        .refresh_scheduling_shape_for_test(
            scope,
            activation,
            7,
            operational_rate_bps.map(u128::from),
            Duration::from_millis(80),
            Duration::from_millis(12),
            2 * 1024 * 1024,
            256 * 1024,
            1400,
            Some(100_000_000),
            false,
        )
        .expect("matching native scheduling shape fixture");
}

fn native_fixed_output(
    output_instance: u64,
    authority_instance: u64,
    activation: u64,
    native_rate_bps: u64,
) -> (
    Arc<FixedReliablePathOutput>,
    Arc<NativeCarrierRateAuthorityHandle>,
) {
    let mux_limits = MuxLimits::default();
    let scope = CarrierRateAuthorityScope::new(
        CarrierPathInstanceId::from_raw(authority_instance),
        PathMetricDirection::ClientToServer,
    );
    let authority = NativeCarrierRateAuthorityHandle::from_observation_for_test(
        scope,
        25_000_000,
        activation,
        7,
        Some(u128::from(native_rate_bps)),
    )
    .expect("checked native authority fixture");
    refresh_native_shape(&authority, scope, activation, Some(native_rate_bps));
    let (commands, _receivers) = reliable_path_command_channels(8);
    let commands = commands.with_native_rate_authority(authority.clone());
    let mut startup = PathSnapshot::new(PathId(14), UnderlayProtocol::Udp, 100.0, 900_000_000.0);
    startup.carrier_delivery_rate_bps = Some(900_000_000.0);
    startup.carrier_inflight_limit_bytes = 4 * 1024 * 1024;
    let observed_at = Instant::now();
    let fixed = FixedReliablePathOutput::new_with_snapshot_and_path_instance(
        startup,
        startup,
        CarrierPathInstanceId::from_raw(output_instance),
        Some(CarrierNativeWindowSample {
            inflight_limit_bytes: startup.carrier_inflight_limit_bytes,
            observed_at,
            expires_at: observed_at + Duration::from_secs(60),
        }),
        Some(FixedNativeRateEpoch {
            rate_bps: 900_000_000.0,
            observed_at,
            expires_at: observed_at + Duration::from_secs(60),
        }),
        commands,
        mux_limits,
    );
    (fixed, authority)
}

fn native_fixed_output_transaction(
    instance: u64,
    activation: u64,
    native_rate_bps: Option<u64>,
    queue: usize,
) -> (
    Arc<FixedReliablePathOutput>,
    Arc<NativeCarrierRateAuthorityHandle>,
    ReliablePathCommandReceivers,
) {
    let mux_limits = MuxLimits::default();
    let carrier_instance = CarrierPathInstanceId::from_raw(instance);
    let scope =
        CarrierRateAuthorityScope::new(carrier_instance, PathMetricDirection::ClientToServer);
    let authority = NativeCarrierRateAuthorityHandle::from_observation_for_test(
        scope,
        25_000_000,
        activation,
        7,
        native_rate_bps.map(u128::from),
    )
    .expect("checked native transaction fixture");
    refresh_native_shape(&authority, scope, activation, native_rate_bps);
    let (commands, receivers) = reliable_path_command_channels(queue);
    let commands = commands.with_native_rate_authority(authority.clone());
    let startup = PathSnapshot::new(PathId(49), UnderlayProtocol::Udp, 100.0, 900_000_000.0);
    let fixed = FixedReliablePathOutput::new_with_snapshot_and_path_instance(
        startup,
        startup,
        carrier_instance,
        None,
        None,
        commands,
        mux_limits,
    );
    (fixed, authority, receivers)
}

fn assert_fixed_output_has_no_flight(fixed: &FixedReliablePathOutput) {
    let model = fixed.model.lock().expect("fixed output model lock");
    assert_eq!(model.original_data_in_flight_bytes, 0);
    assert_eq!(model.carrier_work_in_flight_bytes, 0);
    assert!(model.flights.is_empty());
}

fn assert_reserved_original_capacity_refunded(fixed: &FixedReliablePathOutput, frame: &Frame) {
    let reservation = fixed
        .commands()
        .try_reserve_admitted_frame(frame.clone(), TrafficClass::Throughput)
        .expect("rejected transaction refunds the one-slot original queue");
    assert!(fixed.commands().pending_bytes() > 0);
    drop(reservation);
    assert_eq!(fixed.commands().pending_bytes(), 0);
}

fn assert_reserved_reinjection_capacity_refunded(fixed: &FixedReliablePathOutput, frame: &Frame) {
    let reservation = fixed
        .commands()
        .try_reserve_reinjection_frame(frame.clone(), TrafficClass::Throughput)
        .expect("rejected transaction refunds the one-slot reinjection queue");
    assert!(fixed.commands().pending_bytes() > 0);
    drop(reservation);
    assert_eq!(fixed.commands().pending_bytes(), 0);
}

#[test]
fn native_original_precommit_rejects_stale_activation_and_refunds_reservation() {
    let (fixed, authority, mut receivers) =
        native_fixed_output_transaction(50, 1, Some(120_000_000), 1);
    let frame = stream_data_frame(4096);
    let original_stamp = authority.stamp().expect("initial native authority stamp");
    let (offset, end, bytes) = reliable_stream_frame_extent(&frame).expect("stream-data extent");
    let command = fixed
        .commands()
        .try_reserve_admitted_frame(frame.clone(), TrafficClass::Throughput)
        .expect("reserve the exact writer slot first");
    let decision = fixed
        .current_rate_decision()
        .expect("capture the current Bop decision");
    assert!(fixed.commands().pending_bytes() > 0);
    authority
        .advance_transport_activation_for_test(2)
        .expect("advance only the Quinn activation fence");

    let result = fixed.commit_reserved_original_data_frame(
        command,
        decision,
        offset,
        end,
        bytes,
        TrafficClass::Throughput,
    );

    assert!(matches!(result, Err(RuntimeError::SenderServiceBlocked)));
    assert_eq!(
        authority
            .stamp()
            .expect("coordinator stamp remains readable"),
        original_stamp,
        "changing A at the Quinn fence must invalidate an otherwise unchanged central stamp",
    );
    assert_eq!(fixed.commands().pending_bytes(), 0);
    assert_fixed_output_has_no_flight(&fixed);
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
    assert_reserved_original_capacity_refunded(&fixed, &frame);
}

#[test]
fn native_reinjection_precommit_rejects_c0_to_bop_and_refunds_reservation() {
    let (fixed, authority, mut receivers) = native_fixed_output_transaction(51, 1, None, 1);
    let frame = stream_data_frame(4096);
    let scope = CarrierRateAuthorityScope::new(
        CarrierPathInstanceId::from_raw(51),
        PathMetricDirection::ClientToServer,
    );
    let c0_stamp = authority.stamp().expect("initial C0 stamp");
    assert_eq!(
        authority
            .decision_snapshot(scope)
            .expect("fenced C0 decision")
            .basis(),
        CarrierRateAuthorityBasis::StartupPrior,
    );
    let (offset, end, bytes) = reliable_stream_frame_extent(&frame).expect("stream-data extent");
    let command = fixed
        .commands()
        .try_reserve_reinjection_frame(frame.clone(), TrafficClass::Throughput)
        .expect("reserve the exact reinjection writer slot first");
    let decision = fixed
        .current_rate_decision()
        .expect("capture the current C0 decision");
    assert!(fixed.commands().pending_bytes() > 0);
    authority
        .publish_observation_for_test(1, 7, Some(120_000_000))
        .expect("same-activation C0 to Bop publication");

    let result = fixed.commit_reserved_reinjected_frame(
        command,
        decision,
        offset,
        end,
        bytes,
        TrafficClass::Throughput,
        0,
        reliable_stream_frame_accounted_bytes(&frame),
    );

    assert!(matches!(result, Err(RuntimeError::SenderServiceBlocked)));
    assert_ne!(
        authority.stamp().expect("current Bop stamp"),
        c0_stamp,
        "C0 to Bop changes G even while A and I remain unchanged",
    );
    assert_eq!(
        authority
            .decision_snapshot(scope)
            .expect("current Bop decision")
            .basis(),
        CarrierRateAuthorityBasis::NativeOperational,
    );
    assert_eq!(fixed.commands().pending_bytes(), 0);
    assert_fixed_output_has_no_flight(&fixed);
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
    assert_reserved_reinjection_capacity_refunded(&fixed, &frame);
}

#[test]
fn native_fixed_reinjection_precommit_uses_current_same_stamp_recovery_clock() {
    let (fixed, authority, mut receivers) =
        native_fixed_output_transaction(54, 1, Some(120_000_000), 1);
    let frame = stream_data_frame(4_096);
    let scope = CarrierRateAuthorityScope::new(
        CarrierPathInstanceId::from_raw(54),
        PathMetricDirection::ClientToServer,
    );
    let expected_stamp = authority.stamp().expect("initial native authority stamp");
    let (offset, end, bytes) = reliable_stream_frame_extent(&frame).expect("stream-data extent");
    let command = fixed
        .commands()
        .try_reserve_reinjection_frame(frame.clone(), TrafficClass::Throughput)
        .expect("reserve the exact reinjection writer slot first");
    let decision = fixed
        .current_rate_decision()
        .expect("capture the advisory Native shape");
    let refreshed = authority
        .refresh_scheduling_shape_for_test(
            scope,
            1,
            7,
            Some(120_000_000),
            Duration::from_secs(5),
            Duration::from_secs(1),
            2 * 1024 * 1024,
            256 * 1024,
            1_400,
            Some(100_000_000),
            false,
        )
        .expect("refresh only Quinn timing shape after reservation");
    assert_eq!(refreshed.stamp(), expected_stamp);
    assert_eq!(
        authority.stamp().expect("unchanged central stamp"),
        expected_stamp
    );
    let before = Instant::now();

    let deadline = fixed
        .commit_reserved_reinjected_frame(
            command,
            decision,
            offset,
            end,
            bytes,
            TrafficClass::Throughput,
            0,
            bytes,
        )
        .expect("current same-stamp shape still permits reinjection");

    assert!(
        deadline.duration_since(before) >= Duration::from_secs(8),
        "the committed repair clock must come from current 5s/1s Quinn timing",
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(received)) if received == frame
    ));
}

#[test]
fn native_original_product_rejection_refunds_reserved_queue_without_flight() {
    let (fixed, _authority, mut receivers) =
        native_fixed_output_transaction(52, 1, Some(120_000_000), 1);
    let frame = stream_data_frame(4096);
    let product_limit =
        reliable_bulk_product_windows(MuxLimits::default()).per_output_product_limit_bytes;
    {
        let mut model = fixed.model.lock().expect("fixed output model lock");
        model.original_data_in_flight_bytes = product_limit;
    }
    let (offset, end, bytes) = reliable_stream_frame_extent(&frame).expect("stream-data extent");
    let command = fixed
        .commands()
        .try_reserve_admitted_frame(frame.clone(), TrafficClass::Throughput)
        .expect("reserve the exact writer slot before Product revalidation");
    let decision = fixed
        .current_rate_decision()
        .expect("capture the current Bop decision");
    assert!(
        fixed.commands().pending_bytes() > 0,
        "the actual writer slot must be reserved before Product revalidation",
    );
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());

    let result = fixed.commit_reserved_original_data_frame(
        command,
        decision,
        offset,
        end,
        bytes,
        TrafficClass::Throughput,
    );

    assert!(matches!(result, Err(RuntimeError::SenderServiceBlocked)));
    assert_eq!(fixed.commands().pending_bytes(), 0);
    {
        let model = fixed.model.lock().expect("fixed output model lock");
        assert_eq!(model.original_data_in_flight_bytes, product_limit);
        assert_eq!(model.carrier_work_in_flight_bytes, 0);
        assert!(model.flights.is_empty());
    }
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
    assert_reserved_original_capacity_refunded(&fixed, &frame);
}

#[test]
fn native_c0_success_records_product_flight_before_command_publication() {
    let (fixed, authority, mut receivers) = native_fixed_output_transaction(53, 1, None, 1);
    let frame = stream_data_frame(4096);
    let expected_bytes = reliable_stream_frame_accounted_bytes(&frame) as u64;
    let scope = CarrierRateAuthorityScope::new(
        CarrierPathInstanceId::from_raw(53),
        PathMetricDirection::ClientToServer,
    );
    assert_eq!(
        authority
            .decision_snapshot(scope)
            .expect("current fenced C0 decision")
            .basis(),
        CarrierRateAuthorityBasis::StartupPrior,
    );

    fixed
        .try_enqueue_original_data_frame(&frame, TrafficClass::Throughput)
        .expect("current fenced C0 is bootstrap authority");

    {
        let model = fixed.model.lock().expect("fixed output model lock");
        assert_eq!(model.original_data_in_flight_bytes, expected_bytes);
        assert_eq!(model.carrier_work_in_flight_bytes, expected_bytes);
        assert_eq!(model.flights.len(), 1);
    }
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(received)) if received == frame
    ));
}

#[test]
fn tcp_original_precommit_preserves_unfenced_legacy_transaction() {
    let mux_limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let output =
        ReliablePathStreamOutput::fixed(UnderlayProtocol::Tcp, PathId(54), commands, mux_limits);
    let ReliablePathStreamOutput::Fixed(fixed) = output else {
        panic!("expected fixed TCP output");
    };
    let frame = stream_data_frame(4096);
    let expected_bytes = reliable_stream_frame_accounted_bytes(&frame) as u64;

    fixed
        .try_enqueue_original_data_frame(&frame, TrafficClass::Throughput)
        .expect("TCP legacy admission remains valid without a native authority handle");

    {
        let model = fixed.model.lock().expect("fixed output model lock");
        assert_eq!(model.original_data_in_flight_bytes, expected_bytes);
        assert_eq!(model.carrier_work_in_flight_bytes, expected_bytes);
        assert_eq!(model.flights.len(), 1);
    }
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(received)) if received == frame
    ));
}

#[test]
fn tcp_reinjection_precommit_preserves_unfenced_legacy_transaction() {
    let mux_limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let output =
        ReliablePathStreamOutput::fixed(UnderlayProtocol::Tcp, PathId(55), commands, mux_limits);
    let ReliablePathStreamOutput::Fixed(fixed) = output else {
        panic!("expected fixed TCP output");
    };
    assert!(
        fixed.commands().native_rate_authority().is_none(),
        "TCP Legacy has no Quinn activation fence",
    );
    let frame = stream_data_frame(4096);
    let expected_bytes = reliable_stream_frame_accounted_bytes(&frame) as u64;

    fixed
        .try_enqueue_reinjected_frame(&frame, TrafficClass::Throughput, 0, expected_bytes as usize)
        .expect("TCP Legacy reinjection requires no native authority handle");

    {
        let model = fixed.model.lock().expect("fixed output model lock");
        assert_eq!(
            model.original_data_in_flight_bytes, 0,
            "a repair copy cannot mint unique OriginalData debt",
        );
        assert_eq!(model.carrier_work_in_flight_bytes, expected_bytes);
        assert_eq!(model.flights.len(), 1);
        assert_eq!(
            model
                .flights
                .values()
                .flatten()
                .filter_map(|flight| flight.reinjected_data_bytes())
                .sum::<usize>() as u64,
            expected_bytes,
        );
    }
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(received)) if received == frame
    ));
}

#[tokio::test]
async fn target_local_requalification_wait_retains_release_before_actor_poll() {
    let notify = Arc::new(Notify::new());
    let attempt = RequalificationAttempt::CapacityBlocked {
        targets: vec![TargetCarrierCapacityWait::arm(7u8, notify.clone())],
    };
    let wait = attempt
        .into_capacity_wait()
        .expect("target-local capacity wait");

    // The writer release races after the capacity check but before the actor
    // reaches select. An armed OwnedNotified must retain that edge.
    notify.notify_waiters();
    tokio::time::timeout(Duration::from_millis(100), wait)
        .await
        .expect("pre-armed exact target wake was lost");
}

#[test]
fn fixed_priority_path_proof_preserves_attachment_liveness_ordering() {
    let mux_limits = MuxLimits::default();
    let path_id = PathId(4);
    let (commands, mut receivers) = reliable_path_command_channels(4);
    commands
        .try_enqueue_admitted_frame(stream_data_frame(32), TrafficClass::Throughput)
        .expect("queue earlier stream data");
    let stream = ReliablePathStreamHandle {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: mux_limits.max_payload_bytes,
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            path_id,
            commands,
            mux_limits,
        ),
    };

    let proof_id = stream
        .enqueue_path_proof()
        .expect("queue priority path proof")
        .expect("fixed output has a carrier path");

    match try_recv_reliable_path_priority_command(&mut receivers) {
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
            path_id: queued_path_id,
            proof_id: queued_proof_id,
            payload,
        })) => {
            assert_eq!(queued_path_id, path_id);
            assert_eq!(queued_proof_id, proof_id);
            assert_eq!(payload.len(), 8, "validation uses one challenge token");
        }
        _ => panic!("attachment-liveness proof must retain priority ordering"),
    }
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
}

#[tokio::test]
async fn fixed_output_publishes_terminal_reset_as_one_ordered_transaction() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(8);
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let output =
        ReliablePathStreamOutput::fixed(UnderlayProtocol::Tcp, PathId(4), commands, mux_limits);

    output
        .reset_and_close_stream_ordered(
            stream_id,
            ResetReason::RemoteClosed,
            TrafficClass::Throughput,
        )
        .await;

    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::ResetAndCloseStream {
            stream_id: received,
            reason: ResetReason::RemoteClosed,
        }) if received == stream_id
    ));
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
}

#[test]
fn tcp_fixed_output_product_lower_bound_cannot_downshift_startup_prior() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(64);
    let startup_rate = 500_000_000.0;
    let startup = PathSnapshot::new(PathId(8), UnderlayProtocol::Tcp, 20.0, startup_rate);
    let output = ReliablePathStreamOutput::fixed_with_snapshot(startup, commands, mux_limits);
    let ReliablePathStreamOutput::Fixed(fixed) = &output else {
        panic!("expected fixed output");
    };
    assert_eq!(
        fixed.send_path_snapshot().rate_scope,
        PathRateScope::PathCapacity
    );
    let mut offset = 0_u64;

    for _ in 0..RELIABLE_INITIAL_WINDOW_PACKETS {
        let frame = stream_data_frame_at(offset, MIN_RATE_SAMPLE_BYTES as usize);
        let end = offset + reliable_stream_frame_accounted_bytes(&frame) as u64;
        fixed.record_original_flight(&frame);
        std::thread::sleep(Duration::from_millis(20));
        output.release_normalized_acked_ranges(&[OffsetRange { start: offset, end }]);
        offset = end;
    }

    let learned_rate = fixed
        .model
        .lock()
        .expect("fixed output model lock")
        .product_rate_epoch
        .map(|epoch| epoch.rate_bps)
        .expect("persistent samples produce a delivery model");
    assert!(learned_rate < startup_rate * 0.5);

    let snapshot = output
        .send_path_snapshot(TrafficClass::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
        .expect("response binding exposes learned path model");
    assert_eq!(
        snapshot.product_progress_rate_bps,
        Some(learned_rate),
        "the exact Product interval remains visible as a historical lower bound",
    );
    assert_eq!(
        snapshot.delivery_rate_bps, startup_rate,
        "a placement- and feedback-limited Product lower bound cannot downshift the configured baseline",
    );
    assert_eq!(snapshot.rate_scope, PathRateScope::PathCapacity);
}

#[test]
fn tcp_fixed_output_carrier_diagnostic_cannot_downshift_startup_prior() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let instance = CarrierPathInstanceId::from_raw(81);
    let service_rate = DirectionalServiceRate::from_startup_hint(
        DirectionalServiceRateScope::new(instance, PathMetricDirection::ClientToServer),
        RateHint::BitsPerSecond(500_000_000),
    )
    .expect("configured TCP startup rate");
    let startup = PathSnapshot::new(PathId(81), UnderlayProtocol::Tcp, 20.0, 500_000_000.0)
        .with_scheduling_service_rate(service_rate);
    let observed_at = Instant::now();
    let fixed = FixedReliablePathOutput::new_with_snapshot_and_path_instance(
        startup,
        startup,
        instance,
        None,
        Some(FixedNativeRateEpoch {
            rate_bps: 100_000_000.0,
            observed_at,
            expires_at: observed_at + Duration::from_secs(1),
        }),
        commands,
        mux_limits,
    );

    let snapshot = fixed.send_path_snapshot_at(TrafficClass::Throughput, observed_at);
    assert_eq!(snapshot.delivery_rate_bps, 500_000_000.0);
    assert_eq!(snapshot.scheduling_service_rate(), Some(service_rate));
    assert_eq!(snapshot.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(snapshot.carrier_delivery_rate_bps, Some(100_000_000.0));
}

#[test]
fn native_fixed_output_reads_every_same_activation_rate_and_product_cannot_override_bop() {
    let (fixed, authority) = native_fixed_output(41, 41, 1, 80_000_000);
    let observed_at = Instant::now();
    {
        let mut model = fixed.model.lock().expect("fixed output model lock");
        model.product_progress_bytes =
            crate::model::capacity::reliable_path_startup_sample_limit_bytes(fixed.mux_limits);
        model.product_rate_epoch = Some(FixedProductRateEpoch {
            rate_bps: 800_000_000.0,
            sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            sample_bytes: model.product_progress_bytes,
            observed_at,
            expires_at: observed_at + Duration::from_secs(1),
        });
        model.srtt_ms = Some(900.0);
    }

    let initial = fixed.send_path_snapshot_at(TrafficClass::Throughput, observed_at);
    assert_eq!(initial.delivery_rate_bps, 80_000_000.0);
    assert_eq!(initial.carrier_delivery_rate_bps, Some(80_000_000.0));
    assert_eq!(initial.product_progress_rate_bps, None);
    assert_eq!(initial.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(initial.carrier_inflight_limit_bytes, 2 * 1024 * 1024);
    assert_eq!(initial.bytes_in_flight, 256 * 1024);
    assert_eq!(initial.pacing_rate_bps, 100_000_000.0);
    assert_eq!(
        initial.srtt_ms, 80.0,
        "native timing must come from the matching activation, not Product timing"
    );

    authority
        .publish_observation_for_test(1, 7, Some(160_000_000))
        .expect("same-A native increase");
    refresh_native_shape(
        &authority,
        authority.stamp().unwrap().scope(),
        1,
        Some(160_000_000),
    );
    let raised = fixed.send_path_snapshot_at(TrafficClass::Throughput, observed_at);
    assert_eq!(raised.delivery_rate_bps, 160_000_000.0);

    authority
        .publish_observation_for_test(1, 7, Some(40_000_000))
        .expect("same-A native decrease");
    refresh_native_shape(
        &authority,
        authority.stamp().unwrap().scope(),
        1,
        Some(40_000_000),
    );
    let lowered = fixed.send_path_snapshot_at(TrafficClass::Throughput, observed_at);
    assert_eq!(lowered.delivery_rate_bps, 40_000_000.0);
    assert_eq!(lowered.product_progress_rate_bps, None);
    assert_eq!(
        authority
            .decision_snapshot(CarrierRateAuthorityScope::new(
                CarrierPathInstanceId::from_raw(41),
                PathMetricDirection::ClientToServer,
            ))
            .expect("live scoped decision")
            .basis(),
        CarrierRateAuthorityBasis::NativeOperational
    );
}

#[test]
fn native_fixed_output_has_no_clock_expiry_back_to_startup_prior() {
    let (fixed, _authority) = native_fixed_output(42, 42, 1, 120_000_000);
    let far_future = Instant::now() + Duration::from_secs(24 * 60 * 60);

    let snapshot = fixed.send_path_snapshot_at(TrafficClass::Throughput, far_future);

    assert_eq!(snapshot.delivery_rate_bps, 120_000_000.0);
    assert_eq!(snapshot.carrier_delivery_rate_bps, Some(120_000_000.0));
    assert_eq!(snapshot.product_progress_rate_bps, None);
    assert_eq!(snapshot.carrier_inflight_limit_bytes, 2 * 1024 * 1024);
}

#[test]
fn native_fixed_output_current_fenced_startup_prior_bootstraps_until_bop() {
    let mux_limits = MuxLimits::default();
    let instance = CarrierPathInstanceId::from_raw(48);
    let scope = CarrierRateAuthorityScope::new(instance, PathMetricDirection::ClientToServer);
    let authority =
        NativeCarrierRateAuthorityHandle::from_observation_for_test(scope, 25_000_000, 1, 7, None)
            .expect("absent native observation retains central C0");
    refresh_native_shape(&authority, scope, 1, None);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let commands = commands.with_native_rate_authority(authority.clone());
    let startup = PathSnapshot::new(PathId(48), UnderlayProtocol::Udp, 100.0, 900_000_000.0);
    let fixed = FixedReliablePathOutput::new_with_snapshot_and_path_instance(
        startup, startup, instance, None, None, commands, mux_limits,
    );

    let bootstrap = fixed
        .try_send_path_snapshot_at(TrafficClass::Throughput, Instant::now())
        .expect("current fenced NativeMode C0 bootstraps scheduling");
    assert_eq!(bootstrap.delivery_rate_bps, 25_000_000.0);
    assert_eq!(bootstrap.carrier_delivery_rate_bps, Some(25_000_000.0));
    assert_eq!(bootstrap.product_progress_rate_bps, None);
    assert_eq!(
        authority
            .decision_snapshot(scope)
            .expect("current fenced C0 decision")
            .basis(),
        CarrierRateAuthorityBasis::StartupPrior
    );

    authority
        .publish_observation_for_test(1, 7, Some(120_000_000))
        .expect("first valid Bop replaces C0");
    refresh_native_shape(&authority, scope, 1, Some(120_000_000));
    let operational = fixed
        .try_send_path_snapshot_at(TrafficClass::Throughput, Instant::now())
        .expect("current fenced Bop decision");
    assert_eq!(operational.delivery_rate_bps, 120_000_000.0);
    assert_eq!(
        authority
            .decision_snapshot(scope)
            .expect("current fenced Bop authority")
            .basis(),
        CarrierRateAuthorityBasis::NativeOperational
    );
}

#[test]
fn native_fixed_output_invalid_authority_states_fail_closed() {
    let (wrong_scope, _authority) = native_fixed_output(43, 44, 1, 80_000_000);
    assert!(
        wrong_scope
            .try_send_path_snapshot_at(TrafficClass::Throughput, Instant::now())
            .is_none(),
        "a foreign carrier-direction scope cannot project scheduling capacity"
    );

    let (terminal, authority) = native_fixed_output(45, 45, u64::MAX - 2, 80_000_000);
    authority
        .terminate_exhaustion_for_test()
        .expect("terminal authority fixture");
    assert!(
        terminal
            .try_send_path_snapshot_at(TrafficClass::Throughput, Instant::now())
            .is_none(),
        "terminal authority cannot fall back to a frozen rate"
    );

    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let missing = FixedReliablePathOutput::new_with_snapshot_and_path_instance(
        PathSnapshot::new(PathId(46), UnderlayProtocol::Udp, 100.0, 25_000_000.0),
        PathSnapshot::new(PathId(46), UnderlayProtocol::Udp, 100.0, 25_000_000.0),
        CarrierPathInstanceId::from_raw(46),
        None,
        None,
        commands,
        mux_limits,
    );
    assert!(
        missing
            .try_send_path_snapshot_at(TrafficClass::Throughput, Instant::now())
            .is_none(),
        "QUIC without its native handle cannot use the legacy startup scalar"
    );
}

#[test]
fn tcp_fixed_output_keeps_startup_authority_across_diagnostic_freshness() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let instance = CarrierPathInstanceId::from_raw(47);
    let service_rate = DirectionalServiceRate::from_startup_hint(
        DirectionalServiceRateScope::new(instance, PathMetricDirection::ClientToServer),
        RateHint::BitsPerSecond(500_000_000),
    )
    .expect("configured TCP startup rate");
    let startup = PathSnapshot::new(PathId(47), UnderlayProtocol::Tcp, 20.0, 500_000_000.0)
        .with_scheduling_service_rate(service_rate);
    let observed_at = Instant::now();
    let fixed = FixedReliablePathOutput::new_with_snapshot_and_path_instance(
        startup,
        startup,
        instance,
        None,
        Some(FixedNativeRateEpoch {
            rate_bps: 100_000_000.0,
            observed_at,
            expires_at: observed_at + Duration::from_millis(10),
        }),
        commands,
        mux_limits,
    );

    assert_eq!(
        fixed
            .send_path_snapshot_at(TrafficClass::Throughput, observed_at)
            .delivery_rate_bps,
        500_000_000.0
    );
    assert_eq!(
        fixed
            .send_path_snapshot_at(TrafficClass::Throughput, observed_at)
            .carrier_delivery_rate_bps,
        Some(100_000_000.0),
    );
    let expired = fixed.send_path_snapshot_at(
        TrafficClass::Throughput,
        observed_at + Duration::from_millis(11),
    );
    assert_eq!(expired.delivery_rate_bps, 500_000_000.0);
    assert_eq!(expired.carrier_delivery_rate_bps, None);
    assert_eq!(expired.scheduling_service_rate(), Some(service_rate));
}

#[test]
fn fixed_output_request_feedback_snapshot_preserves_send_path_timing() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let startup = PathSnapshot::new(PathId(9), UnderlayProtocol::Tcp, 123.0, 8_000_000.0);
    let output = ReliablePathStreamOutput::fixed_with_snapshot(startup, commands, mux_limits);
    let send_snapshot = output
        .send_path_snapshot(TrafficClass::Latency, PATH_OPEN_SCORE_BYTES)
        .expect("fixed output has a send path snapshot");
    let request_feedback_snapshot = output
        .request_feedback_path_snapshot(TrafficClass::Latency)
        .expect("fixed output has a request-feedback path snapshot");

    assert_eq!(request_feedback_snapshot.id, send_snapshot.id);
    assert_eq!(request_feedback_snapshot.underlay, send_snapshot.underlay);
    assert_eq!(request_feedback_snapshot.srtt_ms, send_snapshot.srtt_ms);
    assert_eq!(
        reliable_stream_recv_progress_interval(Some(request_feedback_snapshot)),
        reliable_stream_recv_progress_interval(Some(send_snapshot)),
        "fixed-path replay cadence must remain unchanged"
    );
}

#[test]
fn fixed_reinjection_does_not_increase_unique_original_product_debt() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let output =
        ReliablePathStreamOutput::fixed(UnderlayProtocol::Tcp, PathId(10), commands, mux_limits);
    let ReliablePathStreamOutput::Fixed(fixed) = &output else {
        panic!("expected fixed output");
    };
    let frame = stream_data_frame_at(4096, 4096);

    assert_eq!(fixed.send_path_snapshot().data_level_bytes_in_flight, 0);
    fixed.record_reinjected_flight(&frame);
    assert_eq!(
        fixed.send_path_snapshot().data_level_bytes_in_flight,
        0,
        "a repair copy consumes carrier work but cannot mint unique OriginalData debt",
    );
}

#[test]
fn fixed_ambiguous_data_ack_releases_original_debt_without_minting_evidence() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let output =
        ReliablePathStreamOutput::fixed(UnderlayProtocol::Tcp, PathId(12), commands, mux_limits);
    let ReliablePathStreamOutput::Fixed(fixed) = &output else {
        panic!("expected fixed output");
    };
    let frame = stream_data_frame_at(0, 4096);
    let acked = OffsetRange {
        start: 0,
        end: reliable_stream_frame_accounted_bytes(&frame) as u64,
    };

    fixed.record_original_flight(&frame);
    fixed.record_reinjected_flight(&frame);
    assert_eq!(
        fixed.send_path_snapshot().data_level_bytes_in_flight,
        4096,
        "only the unique original copy owns Product debt",
    );

    output.release_normalized_acked_ranges(&[acked]);

    let snapshot = fixed.send_path_snapshot();
    assert_eq!(
        snapshot.data_level_bytes_in_flight, 0,
        "Data ACK releases unique Product debt even when a repair copy makes path evidence ambiguous",
    );
    let model = fixed.model.lock().expect("fixed output model lock");
    assert_eq!(model.carrier_work_in_flight_bytes, 0);
    assert_eq!(
        model.product_progress_bytes, 0,
        "ambiguous ACK coverage must not become path-proving delivery evidence",
    );
    assert_eq!(model.delivery_samples, 0);
    assert!(model.product_rate_epoch.is_none());
}

#[test]
fn fixed_native_window_and_rate_evidence_expire_without_rewriting_product_authority() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut startup = PathSnapshot::new(PathId(11), UnderlayProtocol::Tcp, 100.0, 400_000_000.0);
    startup.carrier_delivery_rate_bps = Some(400_000_000.0);
    startup.carrier_inflight_limit_bytes = 14_600;
    let portable_startup =
        PathSnapshot::new(PathId(11), UnderlayProtocol::Tcp, 100.0, 25_000_000.0);
    let observed_at = std::time::Instant::now();
    let fixed = FixedReliablePathOutput::new_with_snapshot_and_path_instance(
        startup,
        portable_startup,
        CarrierPathInstanceId::from_raw(11),
        Some(CarrierNativeWindowSample {
            inflight_limit_bytes: startup.carrier_inflight_limit_bytes,
            observed_at,
            expires_at: observed_at + Duration::from_millis(10),
        }),
        Some(FixedNativeRateEpoch {
            rate_bps: 400_000_000.0,
            observed_at,
            expires_at: observed_at + Duration::from_millis(20),
        }),
        commands,
        mux_limits,
    );
    {
        let mut model = fixed.model.lock().expect("fixed output model lock");
        model.product_progress_bytes =
            crate::model::capacity::reliable_path_startup_sample_limit_bytes(mux_limits);
        model.product_rate_epoch = Some(FixedProductRateEpoch {
            rate_bps: 500_000_000.0,
            sample_count: 1,
            sample_bytes: model.product_progress_bytes,
            observed_at,
            expires_at: observed_at + Duration::from_millis(30),
        });
    }

    let both_fresh = fixed.send_path_snapshot_at(
        TrafficClass::Throughput,
        observed_at + Duration::from_millis(5),
    );
    let rates_fresh_without_native_window = fixed.send_path_snapshot_at(
        TrafficClass::Throughput,
        observed_at + Duration::from_millis(15),
    );
    let product_rate_only = fixed.send_path_snapshot_at(
        TrafficClass::Throughput,
        observed_at + Duration::from_millis(25),
    );
    let all_expired = fixed.send_path_snapshot_at(
        TrafficClass::Throughput,
        observed_at + Duration::from_millis(35),
    );
    assert!(both_fresh.carrier_delivery_rate_bps.is_some());
    assert!(both_fresh.product_progress_rate_bps.is_some());
    assert!(
        rates_fresh_without_native_window
            .carrier_delivery_rate_bps
            .is_some()
    );
    assert_eq!(
        rates_fresh_without_native_window.carrier_inflight_limit_bytes,
        0
    );
    assert!(
        rates_fresh_without_native_window
            .product_progress_rate_bps
            .is_some()
    );
    let product_limit = reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes;
    for snapshot in [
        both_fresh,
        rates_fresh_without_native_window,
        product_rate_only,
        all_expired,
    ] {
        assert_eq!(
            snapshot.data_level_limit_bytes, product_limit,
            "freshness changes native diagnostics and completion ranking, never configured Product authority",
        );
    }
    assert_eq!(product_rate_only.carrier_delivery_rate_bps, None);
    assert!(product_rate_only.product_progress_rate_bps.is_some());
    assert_eq!(all_expired.carrier_delivery_rate_bps, None);
    assert_eq!(all_expired.product_progress_rate_bps, None);
    assert_eq!(all_expired.delivery_rate_bps, 25_000_000.0);
    assert_eq!(all_expired.pacing_rate_bps, 25_000_000.0);
    assert_eq!(all_expired.confidence, portable_startup.confidence);
    assert_eq!(
        all_expired.data_level_limit_bytes, product_limit,
        "expired C/R leave the configured Product envelope intact",
    );

    // An expired low-QoS scalar is diagnostic only. The next exact ACK starts
    // a new epoch from its own bytes/time instead of EWMA-blending the stale R.
    let reacquired_at = observed_at + Duration::from_millis(40);
    let sample_bytes = 64 * 1024;
    {
        let mut model = fixed.model.lock().expect("fixed output model lock");
        model.product_rate_epoch = Some(FixedProductRateEpoch {
            rate_bps: 1_000_000.0,
            sample_count: 1,
            sample_bytes,
            observed_at,
            expires_at: observed_at + Duration::from_millis(30),
        });
        fixed.record_product_flight_with_model(
            &mut model,
            0,
            sample_bytes,
            sample_bytes as usize,
            reacquired_at - Duration::from_millis(10),
            CarrierWorkKind::OriginalData,
            None,
        );
    }
    fixed.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: 0,
            end: sample_bytes,
        }],
        reacquired_at,
    );
    let reacquired = fixed
        .model
        .lock()
        .expect("fixed output model lock")
        .product_rate_epoch
        .expect("new exact Product epoch");
    let expected_rate = sample_bytes as f64 * 8.0 / 0.010;
    assert_eq!(reacquired.rate_bps, expected_rate);
    assert_eq!(reacquired.sample_count, 1);
    assert_eq!(reacquired.sample_bytes, sample_bytes);
    let reacquired_snapshot = fixed.send_path_snapshot_at(TrafficClass::Throughput, reacquired_at);
    assert_eq!(reacquired_snapshot.product_progress_rate_bps, None);
    assert_eq!(reacquired_snapshot.delivery_rate_bps, 25_000_000.0);
    assert!(!reacquired_snapshot.has_durable_product_progress);
}

#[test]
fn t02_fixed_product_rate_stays_diagnostic_beside_configured_service_rate() {
    let mux_limits = MuxLimits {
        max_repair_bytes: 32 * 1024 * 1024,
        max_reorder_bytes: 32 * 1024 * 1024,
        max_stream_window_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let instance = CarrierPathInstanceId::from_raw(12);
    let startup_service_rate = DirectionalServiceRate::from_startup_hint(
        DirectionalServiceRateScope::new(instance, PathMetricDirection::ClientToServer),
        RateHint::BitsPerSecond(80_000_000),
    )
    .expect("configured startup service rate");
    let mut startup = PathSnapshot::new(PathId(12), UnderlayProtocol::Tcp, 100.0, 80_000_000.0)
        .with_scheduling_service_rate(startup_service_rate);
    startup.carrier_inflight_limit_bytes = 1024 * 1024;
    let observed_at = Instant::now() - Duration::from_millis(20);
    let fixed = FixedReliablePathOutput::new_with_snapshot_and_path_instance(
        startup,
        startup,
        instance,
        Some(CarrierNativeWindowSample {
            inflight_limit_bytes: startup.carrier_inflight_limit_bytes,
            observed_at,
            expires_at: observed_at + Duration::from_secs(1),
        }),
        None,
        commands,
        mux_limits,
    );
    {
        let mut model = fixed.model.lock().expect("fixed output model lock");
        model.product_progress_bytes =
            crate::model::capacity::reliable_path_startup_sample_limit_bytes(mux_limits);
        model.product_rate_epoch = Some(FixedProductRateEpoch {
            rate_bps: 500_000_000.0,
            sample_count: 1,
            sample_bytes: model.product_progress_bytes,
            observed_at: observed_at + Duration::from_millis(10),
            expires_at: observed_at + Duration::from_secs(1),
        });
    }

    let snapshot = fixed.send_path_snapshot_at(
        TrafficClass::Throughput,
        observed_at + Duration::from_millis(20),
    );
    let expected = reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes;

    assert_eq!(snapshot.carrier_inflight_limit_bytes, 1024 * 1024);
    assert_eq!(snapshot.delivery_rate_bps, 80_000_000.0);
    assert_eq!(
        snapshot.scheduling_service_rate(),
        Some(startup_service_rate)
    );
    assert_eq!(snapshot.product_progress_rate_bps, Some(500_000_000.0));
    assert_eq!(snapshot.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(
        snapshot.data_level_limit_bytes, expected,
        "Product completion remains diagnostic and cannot replace configured carrier C",
    );
    let output = ReliablePathStreamOutput::Fixed(fixed);
    let (_, source_window) = output
        .send_path_snapshot_and_source_window(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES);
    assert_eq!(
        source_window as u64, expected,
        "fixed response source staging uses the same configured Product authority",
    );
}

#[test]
fn fixed_product_rate_requires_the_exact_epoch_byte_boundary() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let instance = CarrierPathInstanceId::from_raw(13);
    let service_rate = DirectionalServiceRate::from_startup_hint(
        DirectionalServiceRateScope::new(instance, PathMetricDirection::ClientToServer),
        RateHint::BitsPerSecond(25_000_000),
    )
    .expect("configured TCP startup rate");
    let startup = PathSnapshot::new(PathId(13), UnderlayProtocol::Tcp, 100.0, 25_000_000.0)
        .with_scheduling_service_rate(service_rate);
    let fixed = FixedReliablePathOutput::new_with_snapshot_and_path_instance(
        startup, startup, instance, None, None, commands, mux_limits,
    );
    let observed_at = Instant::now();
    let sample_floor = crate::model::capacity::reliable_path_startup_sample_limit_bytes(mux_limits);
    let raw_rate = 900_000_000.0;
    {
        let mut model = fixed.model.lock().expect("fixed output model lock");
        model.product_progress_bytes = sample_floor;
        model.product_rate_epoch = Some(FixedProductRateEpoch {
            rate_bps: raw_rate,
            sample_count: 1,
            sample_bytes: sample_floor - 1,
            observed_at,
            expires_at: observed_at + Duration::from_secs(1),
        });
    }

    let partial = fixed.send_path_snapshot_at(TrafficClass::Throughput, observed_at);
    assert_eq!(partial.delivery_rate_bps, 25_000_000.0);
    assert_eq!(partial.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(partial.product_progress_rate_bps, None);
    assert!(!partial.has_durable_product_progress);

    {
        let mut model = fixed.model.lock().expect("fixed output model lock");
        model
            .product_rate_epoch
            .as_mut()
            .expect("raw epoch")
            .sample_bytes = sample_floor;
    }
    let qualified = fixed.send_path_snapshot_at(TrafficClass::Throughput, observed_at);
    assert_eq!(qualified.delivery_rate_bps, 25_000_000.0);
    assert_eq!(qualified.scheduling_service_rate(), Some(service_rate));
    assert_eq!(qualified.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(qualified.product_progress_rate_bps, Some(raw_rate));
    assert!(qualified.has_durable_product_progress);
}

fn server_stream_with_events(
    events: mpsc::Receiver<ServerReliableStreamEvent>,
) -> ReliablePathStream {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let startup = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, 100_000_000.0);
    ReliablePathStream {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: mux_limits.max_payload_bytes,
        output: ReliablePathStreamOutput::fixed_with_snapshot(startup, commands, mux_limits),
        frames: ReliablePathStreamInput::server(events),
    }
}

#[tokio::test]
async fn server_input_coalesces_only_the_contiguous_feedback_backlog() {
    let stream_id = StreamId(7);
    let (events, events_rx) = mpsc::channel(16);
    for event in [
        ServerReliableStreamEvent::Frame(Frame::StreamAck {
            stream_id,
            complete: true,
            ranges: vec![
                OffsetRange { start: 0, end: 100 },
                OffsetRange {
                    start: 120,
                    end: 130,
                },
            ],
        }),
        ServerReliableStreamEvent::Frame(Frame::StreamMaxData {
            stream_id,
            max_offset: 1_000,
        }),
        // A complete snapshot from another carrier may be older. Received
        // coverage is monotonic, so the actor must union rather than replace.
        ServerReliableStreamEvent::Frame(Frame::StreamAck {
            stream_id,
            complete: true,
            ranges: vec![OffsetRange { start: 0, end: 80 }],
        }),
        ServerReliableStreamEvent::Frame(Frame::StreamAck {
            stream_id,
            complete: false,
            ranges: vec![OffsetRange {
                start: 100,
                end: 120,
            }],
        }),
        ServerReliableStreamEvent::Frame(Frame::StreamMaxData {
            stream_id,
            max_offset: 2_000,
        }),
        ServerReliableStreamEvent::Frame(Frame::StreamMaxData {
            stream_id,
            max_offset: 1_500,
        }),
        ServerReliableStreamEvent::Frame(Frame::StreamFin {
            stream_id,
            final_offset: 140,
        }),
    ] {
        events.try_send(event).expect("queue server stream event");
    }

    let mut stream = server_stream_with_events(events_rx);
    assert_eq!(
        stream.recv_frame().await.expect("merged ACK delta"),
        Frame::StreamAck {
            stream_id,
            complete: false,
            ranges: vec![OffsetRange {
                start: 100,
                end: 120,
            }],
        }
    );
    assert_eq!(
        stream.recv_frame().await.expect("merged complete ACK"),
        Frame::StreamAck {
            stream_id,
            complete: true,
            ranges: vec![
                OffsetRange { start: 0, end: 100 },
                OffsetRange {
                    start: 120,
                    end: 130,
                },
            ],
        }
    );
    assert_eq!(
        stream.recv_frame().await.expect("greatest receive window"),
        Frame::StreamMaxData {
            stream_id,
            max_offset: 2_000,
        }
    );
    assert_eq!(
        stream
            .recv_frame()
            .await
            .expect("terminal ordering boundary"),
        Frame::StreamFin {
            stream_id,
            final_offset: 140,
        }
    );
}

#[tokio::test]
async fn server_input_retains_an_empty_complete_ack_snapshot() {
    let stream_id = StreamId(7);
    let (events, events_rx) = mpsc::channel(2);
    events
        .try_send(ServerReliableStreamEvent::Frame(Frame::StreamAck {
            stream_id,
            complete: true,
            ranges: Vec::new(),
        }))
        .expect("queue empty ACK snapshot");

    let mut stream = server_stream_with_events(events_rx);
    assert_eq!(
        stream.recv_frame().await.expect("empty complete ACK"),
        Frame::StreamAck {
            stream_id,
            complete: true,
            ranges: Vec::new(),
        }
    );
}

#[tokio::test]
async fn server_input_does_not_coalesce_feedback_across_path_detach() {
    let stream_id = StreamId(7);
    let (events, events_rx) = mpsc::channel(4);
    let first_ack = Frame::StreamAck {
        stream_id,
        complete: true,
        ranges: vec![OffsetRange { start: 0, end: 64 }],
    };
    let second_ack = Frame::StreamAck {
        stream_id,
        complete: true,
        ranges: vec![OffsetRange { start: 0, end: 128 }],
    };
    for event in [
        ServerReliableStreamEvent::Frame(first_ack.clone()),
        ServerReliableStreamEvent::PathDetached {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(0),
            },
            path_instance_id: CarrierPathInstanceId::from_raw(1),
            output_incarnation: 1,
        },
        ServerReliableStreamEvent::Frame(second_ack.clone()),
    ] {
        events.try_send(event).expect("queue server stream event");
    }

    let mut stream = server_stream_with_events(events_rx);
    assert_eq!(
        stream.recv_frame().await.expect("ACK before detach"),
        first_ack
    );
    assert_eq!(
        stream.recv_frame().await.expect("ACK after detach"),
        second_ack
    );
}

#[tokio::test]
async fn ready_server_input_never_crosses_a_path_detach_lifecycle_boundary() {
    let first_data = stream_data_frame_at(0, 16);
    let second_data = stream_data_frame_at(16, 16);
    let (events, events_rx) = mpsc::channel(4);
    for event in [
        ServerReliableStreamEvent::Frame(first_data.clone()),
        ServerReliableStreamEvent::PathDetached {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(0),
            },
            path_instance_id: CarrierPathInstanceId::from_raw(5),
            output_incarnation: 5,
        },
        ServerReliableStreamEvent::Frame(second_data.clone()),
    ] {
        events.try_send(event).expect("queue server stream event");
    }

    let mut stream = server_stream_with_events(events_rx);
    assert_eq!(stream.ready_frame_count(), 3);
    assert_eq!(
        stream
            .try_recv_frame()
            .expect("first data is already queued")
            .expect("first data frame"),
        first_data
    );
    assert!(
        stream.try_recv_frame().is_none(),
        "ready-only drain must stop with path detach still ordered"
    );
    assert_eq!(
        stream
            .recv_frame()
            .await
            .expect("ordinary receive processes detach before later data"),
        second_data
    );
}
