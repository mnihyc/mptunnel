use super::super::test_support::{
    client_test_context_with_paths, consume_client_path_proof_for_test, opened_test_relay_stream,
    opened_test_relay_stream_with_underlay, seed_client_bulk_evidence_for_test,
};
use super::*;
use crate::model::request_evidence::RequestPerFlowRateModel;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::runtime::path::commands::{
    recv_reliable_path_command, reliable_path_command_channels, reliable_path_command_pending_bytes,
};
use crate::runtime::stream::ReliableRelayRemoteSet;
use bytes::Bytes;

fn data_frame(stream_id: StreamId, offset: u64, payload_bytes: usize) -> Frame {
    Frame::StreamData {
        stream_id,
        offset,
        payload: Bytes::from(vec![0x5a; payload_bytes]),
    }
}

async fn mixed_remote_set() -> (
    ClientPathContext,
    ReliableRelayRemoteSet,
    RelayPathInstance,
    RelayPathInstance,
) {
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "udp://127.0.0.1:10252"]);
    let stream_id = StreamId(17);
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, tcp_commands), 8);
    let tcp = remotes.paths[0].instance();

    let (udp_commands, _udp_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("UDP attachment")
        .instance();
    (context, remotes, tcp, udp)
}

#[tokio::test]
async fn bounded_ack_gap_uses_an_active_unmeasured_alternate() {
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "udp://127.0.0.1:10252"]);
    let stream_id = StreamId(17);
    let (tcp_commands, mut tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, tcp_commands), 8);
    let tcp = remotes.paths[0].instance();
    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("UDP attachment")
        .instance();
    consume_client_path_proof_for_test(&mut tcp_receivers);
    consume_client_path_proof_for_test(&mut udp_receivers);

    let controller = RequestMultipathController::new(stream_id);
    let frame = data_frame(stream_id, 0, 4096);

    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[tcp],
        )
        .expect("bounded repair needs an active alternate, not a mature rate model");
    assert_eq!(remotes.paths[selected].instance(), udp);

    assert!(matches!(
        controller.choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::PersistentAckGapReinjection,
            &[tcp],
        ),
        Err(RuntimeError::ReliablePathSessionClosed)
    ));
}

#[tokio::test]
async fn ack_gap_avoidance_does_not_exclude_a_same_key_replacement() {
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251?srtt-ms=20&rate-mbps=200"]);
    let stream_id = StreamId(32);
    let (old_commands, _old_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, old_commands), 8);
    let old = remotes.paths[0].instance();
    let frame = data_frame(stream_id, 0, 4096);
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(old, &frame);

    drop(remotes.remove_path_instance(old));
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    remotes.attach(opened_test_relay_stream(
        stream_id,
        old.key.index,
        replacement_commands,
    ));
    let replacement = remotes.paths[0].instance();
    assert_eq!(replacement.key, old.key);
    assert_ne!(replacement, old);
    assert!(
        controller
            .reinjection_avoid_instances(&frame, RelaySendCause::AckGapReinjection, &remotes,)
            .is_empty(),
        "an original attachment that is no longer live cannot exclude its replacement",
    );

    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[old],
        )
        .expect("the replacement is a distinct carrier output");
    assert_eq!(remotes.paths[selected].instance(), replacement);
}

#[tokio::test]
async fn ack_gap_repair_history_does_not_exhaust_non_original_outputs() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10251?srtt-ms=20&rate-mbps=200",
        "udp://127.0.0.1:10252?srtt-ms=15&rate-mbps=250",
        "udp://127.0.0.1:10253?srtt-ms=10&rate-mbps=300",
    ]);
    let stream_id = StreamId(33);
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, original_commands), 8);
    let original = remotes.paths[0].instance();

    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        first_commands,
    ));
    let first_repair = remotes
        .paths
        .iter()
        .find(|path| {
            path.key()
                == RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index: 0,
                }
        })
        .expect("first repair attachment")
        .instance();

    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        1,
        second_commands,
    ));
    let second_repair = remotes
        .paths
        .iter()
        .find(|path| {
            path.key()
                == RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index: 1,
                }
        })
        .expect("second repair attachment")
        .instance();

    let frame = data_frame(stream_id, 0, 4096);
    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .flights
        .record_original_frame_instance(original, &frame);
    for repair in [first_repair, second_repair] {
        controller
            .request
            .flights
            .record_reinjection_frame_instance(repair, &frame);
    }
    let avoid =
        controller.reinjection_avoid_instances(&frame, RelaySendCause::AckGapReinjection, &remotes);
    assert_eq!(
        avoid,
        vec![original],
        "repair history is duplicate suppression evidence, not a permanent path blacklist",
    );
    assert!(matches!(
        controller.choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &avoid,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    controller
        .request
        .flights
        .age_reinjected_flights_for_test(Duration::from_secs(1));
    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &avoid,
        )
        .expect("a live non-original attachment remains available");
    assert!(
        [first_repair, second_repair].contains(&remotes.paths[selected].instance()),
        "the repair must use a non-original live attachment",
    );
}

#[tokio::test]
async fn portable_quic_repair_waits_for_exact_stream_flight_but_failure_recovery_does_not() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10251?srtt-ms=20&rate-mbps=200",
        "udp://127.0.0.1:10252?srtt-ms=10&rate-mbps=300",
    ]);
    let stream_id = StreamId(35);
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, original_commands), 8);
    let original = remotes.paths[0].instance();
    let (quic_commands, _quic_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        quic_commands,
    ));
    let quic = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("QUIC attachment")
        .instance();
    let mut controller = RequestMultipathController::new(stream_id);
    let repair = data_frame(stream_id, 0, 4096);
    let quic_original = data_frame(stream_id, 4096, 4096);
    controller.record_original_frame_for_test(quic, &quic_original);

    assert!(matches!(
        controller.choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &repair,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[original],
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    drop(remotes.remove_path_instance(original));
    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &repair,
            TrafficClass::Throughput,
            RelaySendCause::PathFailureReinjection,
            &[original],
        )
        .expect("confirmed failure may reschedule onto the only live survivor");
    assert_eq!(remotes.paths[selected].instance(), quic);

    controller
        .request
        .flights
        .release_normalized_acked_ranges(&[OffsetRange {
            start: 4096,
            end: 8192,
        }]);
    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &repair,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[original],
        )
        .expect("Data ACK drains exact QUIC-stream ownership");
    assert_eq!(remotes.paths[selected].instance(), quic);
}

#[tokio::test]
async fn repair_waits_for_writer_dequeued_bytes_to_flush() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10251?srtt-ms=20&rate-mbps=200",
        "udp://127.0.0.1:10252?srtt-ms=10&rate-mbps=300",
    ]);
    let stream_id = StreamId(36);
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, original_commands), 8);
    let original = remotes.paths[0].instance();
    let (quic_commands, mut quic_receivers) = reliable_path_command_channels(8);
    let quic_commands_for_writer = quic_commands.clone();
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        quic_commands,
    ));
    let quic = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("QUIC attachment")
        .instance();
    consume_client_path_proof_for_test(&mut quic_receivers);
    quic_commands_for_writer
        .try_enqueue_stream_ordered_frame(
            data_frame(stream_id, 4096, 4096),
            TrafficClass::Throughput,
        )
        .expect("queue fresh carrier work");
    let dequeued = recv_reliable_path_command(&mut quic_receivers)
        .await
        .expect("writer dequeues fresh carrier work");
    let controller = RequestMultipathController::new(stream_id);
    let repair = data_frame(stream_id, 0, 4096);

    assert!(matches!(
        controller.choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &repair,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[original],
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    quic_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&dequeued));
    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &repair,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[original],
        )
        .expect("flushed writer batch reopens repair eligibility");
    assert_eq!(remotes.paths[selected].instance(), quic);
}

#[tokio::test]
async fn unbound_reinjection_requires_an_idle_tcp_carrier() {
    let context = client_test_context_with_paths(&[
        "udp://127.0.0.1:10251?srtt-ms=20&rate-mbps=200",
        "tcp://127.0.0.1:10252?srtt-ms=2&rate-mbps=1000",
        "tcp://127.0.0.1:10253?srtt-ms=30&rate-mbps=100",
    ]);
    let stream_id = StreamId(34);
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            original_commands,
        ),
        8,
    );
    let original = remotes.paths[0].instance();

    let (busy_commands, _busy_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 0, busy_commands));
    let busy = remotes
        .paths
        .iter()
        .find(|path| {
            path.key()
                == RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: 0,
                }
        })
        .expect("busy TCP attachment")
        .instance();

    let (idle_commands, _idle_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, idle_commands));
    let idle = remotes
        .paths
        .iter()
        .find(|path| {
            path.key()
                == RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: 1,
                }
        })
        .expect("idle TCP attachment")
        .instance();

    {
        let mut health = context.health().lock().expect("path health lock");
        health.tcp[busy.key.index].carrier_bytes_in_flight = 64 * 1024;
        health.tcp[busy.key.index].carrier_queue_bytes = 8 * 1024;
        health.tcp[busy.key.index].carrier_inflight_limit_bytes = 512 * 1024;
    }
    let frame = data_frame(stream_id, 0, 4096);
    let controller = RequestMultipathController::new(stream_id);
    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[original],
        )
        .expect("the idle TCP carrier remains eligible");
    assert_eq!(
        remotes.paths[selected].instance(),
        idle,
        "unbound repair must not queue behind native flight on a faster TCP carrier",
    );

    let busy_snapshot = context
        .reliable_path_snapshot(busy.key)
        .expect("busy TCP snapshot");
    let bound_cause = RelaySendCause::persistent_client_ack_gap_reinjection(
        ClientReinjectionOutputIdentity { instance: busy },
        busy_snapshot,
    );
    assert!(matches!(
        controller.choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            bound_cause,
            &[original],
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
}

#[tokio::test]
async fn capacity_reference_is_the_fastest_mature_attached_path() {
    let (context, remotes, tcp, udp) = mixed_remote_set().await;
    seed_client_bulk_evidence_for_test(&context, tcp.key);
    seed_client_bulk_evidence_for_test(&context, udp.key);
    let mut controller = RequestMultipathController::new(StreamId(17));
    controller
        .request
        .path_states
        .get_mut(tcp)
        .set_per_flow_rate(RequestPerFlowRateModel {
            rate_bps: 120_000_000.0,
            delivery_samples: 10,
        });
    assert_eq!(
        context
            .reliable_path_snapshot(tcp.key)
            .map(|path| path.state),
        Some(scheduler::PathState::Active)
    );
    assert!(
        controller
            .request
            .path_states
            .get(tcp)
            .and_then(|state| state.per_flow_rate())
            .is_some()
    );
    controller
        .request
        .path_states
        .get_mut(udp)
        .set_per_flow_rate(RequestPerFlowRateModel {
            rate_bps: 240_000_000.0,
            delivery_samples: 10,
        });

    let (reference, model) = controller
        .request_capacity_reference(&context, &remotes)
        .expect("mature path reference");
    assert_eq!(reference, udp);
    assert_eq!(model.rate_bps, 240_000_000.0);

    context.mark_relay_path_failure(udp.key.underlay, udp.key.index);
    assert_eq!(
        controller
            .request_capacity_reference(&context, &remotes)
            .map(|value| value.0),
        Some(tcp)
    );
}

#[tokio::test]
async fn capacity_reference_ignores_immature_higher_rate_samples() {
    let (context, remotes, tcp, udp) = mixed_remote_set().await;
    seed_client_bulk_evidence_for_test(&context, tcp.key);
    seed_client_bulk_evidence_for_test(&context, udp.key);
    let mut controller = RequestMultipathController::new(StreamId(17));
    controller
        .request
        .path_states
        .get_mut(tcp)
        .set_per_flow_rate(RequestPerFlowRateModel {
            rate_bps: 100_000_000.0,
            delivery_samples: 10,
        });
    controller
        .request
        .path_states
        .get_mut(udp)
        .set_per_flow_rate(RequestPerFlowRateModel {
            rate_bps: 900_000_000.0,
            delivery_samples: 1,
        });
    assert_eq!(
        context
            .reliable_path_snapshot(tcp.key)
            .map(|path| path.state),
        Some(scheduler::PathState::Active)
    );
    assert!(
        controller
            .request
            .path_states
            .get(tcp)
            .and_then(|state| state.per_flow_rate())
            .is_some()
    );

    assert_eq!(
        controller
            .request_capacity_reference(&context, &remotes)
            .map(|value| value.0),
        Some(tcp)
    );
}

#[tokio::test]
async fn duplicated_product_ack_does_not_invent_exact_path_progress() {
    let (context, remotes, tcp, udp) = mixed_remote_set().await;
    let stream_id = StreamId(17);
    let payload_bytes = 64 * 1024;
    let frame = data_frame(stream_id, 0, payload_bytes);
    let (_, end, _) = reliable_stream_frame_extent(&frame).expect("data extent");
    let range = OffsetRange::new(0, end).expect("ACK range");
    let mut controller = RequestMultipathController::new(stream_id);
    assert!(controller.mark_path_stale(tcp));
    assert_eq!(
        controller
            .request
            .flights
            .record_original_frame_instance(tcp, &frame),
        payload_bytes
    );
    assert_eq!(
        controller
            .request
            .flights
            .record_reinjection_frame_instance(udp, &frame),
        payload_bytes
    );

    let data_ack_progress_paths =
        controller.apply_product_ack(&context, &remotes, &[range], Instant::now());
    assert!(
        data_ack_progress_paths.is_empty(),
        "a Data ACK cannot identify which duplicate delivered the range"
    );
    assert!(
        controller.path_is_stale(tcp),
        "ambiguous reinjection progress must not reactivate the original path"
    );
    assert!(
        controller
            .latest_unacked_ranges_for_path_instance(tcp)
            .is_empty()
    );
    assert!(
        controller
            .latest_unacked_ranges_for_path_instance(udp)
            .is_empty()
    );
}

#[tokio::test]
async fn product_ack_returns_the_exact_path_that_made_progress() {
    let (context, remotes, tcp, _udp) = mixed_remote_set().await;
    let stream_id = StreamId(18);
    let payload_bytes = 4096;
    let frame = data_frame(stream_id, 0, payload_bytes);
    let mut controller = RequestMultipathController::new(stream_id);
    assert!(controller.mark_path_stale(tcp));
    controller
        .request
        .flights
        .record_original_frame_instance(tcp, &frame);

    let data_ack_progress_paths = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: 0,
            end: payload_bytes as u64,
        }],
        Instant::now(),
    );

    assert_eq!(data_ack_progress_paths.as_slice(), &[tcp]);
    assert!(
        !controller.path_is_stale(tcp),
        "exact Data ACK progress makes the path schedulable again"
    );
}

#[tokio::test]
async fn stale_path_is_not_selected_for_new_request_data() {
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "udp://127.0.0.1:10252"]);
    let stream_id = StreamId(19);
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, tcp_commands), 8);
    let tcp = remotes.paths[0].instance();
    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands.clone(),
    ));
    super::super::test_support::consume_client_path_proof_for_test(&mut udp_receivers);
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("UDP attachment")
        .instance();
    seed_client_bulk_evidence_for_test(&context, tcp.key);
    seed_client_bulk_evidence_for_test(&context, udp.key);
    let frame = data_frame(stream_id, 0, 4096);
    assert!(udp_commands.can_enqueue_frame_now(&frame, TrafficClass::Throughput));
    let mut controller = RequestMultipathController::new(stream_id);
    assert!(controller.mark_path_stale(tcp));

    let observation = observe_request_relay_scheduling(
        &context,
        stream_id,
        remotes.membership_generation(),
        &remotes.paths,
        Some(&frame),
        TrafficClass::Latency,
        4096,
        false,
        &controller.request.stale_paths,
    );
    assert!(
        !observation
            .path_by_instance(tcp)
            .expect("stale path observation")
            .can_enqueue_stream_lane
    );
    assert!(
        observation
            .path_by_instance(udp)
            .expect("alternate path observation")
            .can_enqueue_stream_lane
    );
    assert!(matches!(
        choose_observed_ordinary_data_path(&observation, TrafficClass::Latency, 4096, 0, &[]),
        ObservedOrdinaryPathChoice::Selected(instance) if instance == udp
    ));

    let plan = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &frame,
            TrafficClass::Latency,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("the progressing path remains available");

    assert_eq!(plan.target().1, udp);

    let fin = Frame::StreamFin {
        stream_id,
        final_offset: 4096,
    };
    let fin_plan = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &fin,
            TrafficClass::Latency,
            RelaySendCause::StreamFin,
            &[],
        )
        .expect("connection FIN remains schedulable on the progressing path");
    assert_eq!(fin_plan.target().1, udp);
}

#[tokio::test]
async fn reinjected_data_ack_clocks_the_next_stale_path_range() {
    let (context, remotes, tcp, udp) = mixed_remote_set().await;
    let stream_id = StreamId(20);
    let first = data_frame(stream_id, 0, 4096);
    let second = data_frame(stream_id, 4096, 4096);
    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .flights
        .record_original_frame_instance(tcp, &first);
    controller
        .request
        .flights
        .record_original_frame_instance(tcp, &second);
    controller
        .request
        .flights
        .record_reinjection_frame_instance(udp, &first);
    assert!(controller.mark_path_stale(tcp));
    controller.record_reinjection_attempts(&[tcp], Instant::now());
    assert!(
        controller
            .stale_paths_requiring_reinjection(&remotes, Duration::from_secs(60))
            .is_empty()
    );

    let data_ack_progress_paths = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: 0,
            end: 4096,
        }],
        Instant::now(),
    );

    assert!(
        data_ack_progress_paths.is_empty(),
        "a Data ACK for duplicated data does not identify the delivering path"
    );
    assert!(controller.path_is_stale(tcp));
    assert_eq!(
        controller.stale_paths_requiring_reinjection(&remotes, Duration::from_secs(60)),
        vec![tcp],
        "acknowledging one reinjected range immediately admits the next range"
    );
}

#[tokio::test]
async fn live_reinjected_flight_suppresses_duplicate_reinjection() {
    let stream_id = StreamId(21);
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, tcp_commands), 8);
    let tcp = remotes.paths[0].instance();
    let (udp_commands, _udp_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("UDP attachment")
        .instance();
    let frame = data_frame(stream_id, 0, 4096);
    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .flights
        .record_original_frame_instance(tcp, &frame);
    controller
        .request
        .flights
        .record_reinjection_frame_instance(udp, &frame);
    assert!(controller.mark_path_stale(tcp));

    assert!(
        controller
            .stale_paths_requiring_reinjection(&remotes, Duration::ZERO)
            .is_empty(),
        "the alternate carrier owns recovery for its live reinjected flight"
    );

    drop(remotes.remove_path_instance(udp));
    assert_eq!(
        controller.stale_paths_requiring_reinjection(&remotes, Duration::ZERO),
        vec![tcp],
        "loss of the reinjection path makes the range eligible again"
    );
}

#[tokio::test]
async fn missing_owner_detection_is_fenced_by_attachment_instance() {
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10251"]);
    let stream_id = StreamId(31);
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, first_commands), 8);
    let old_instance = remotes.paths[0].instance();
    let frame = data_frame(stream_id, 0, 4096);
    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .flights
        .record_original_frame_instance(old_instance, &frame);

    drop(remotes.remove_path_instance(old_instance));
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    remotes.attach(opened_test_relay_stream(stream_id, 0, replacement_commands));
    let replacement = remotes.paths[0].instance();
    assert_eq!(replacement.key, old_instance.key);
    assert_ne!(replacement, old_instance);
    assert!(
        controller
            .data_ack_gap_reinjection_model(&context, &remotes, &frame, TrafficClass::Throughput)
            .original_path_timing
            .is_none(),
        "a replacement attachment cannot lend path timing to the prior instance's Data ACK gap"
    );

    assert_eq!(
        controller.unreported_missing_owner_instances(&remotes, Duration::ZERO),
        vec![old_instance]
    );
    controller.record_reinjection_attempts(&[old_instance], Instant::now());
    assert!(
        controller
            .unreported_missing_owner_instances(&remotes, Duration::from_secs(1))
            .is_empty()
    );

    controller.release_all(&context);
}
