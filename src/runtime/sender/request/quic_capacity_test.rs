use super::super::test_support::{
    client_test_context_with_paths, opened_test_relay_stream_with_underlay,
};
use super::*;
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS,
    reliable_capacity_measurement_session_limit_bytes,
};
use crate::model::path::RelayPathInstance;
use crate::model::request_capacity::request_capacity_stable_candidate_share_bytes;
use crate::model::request_evidence::RequestPerFlowRateModel;
use crate::protocol::Frame;
use crate::runtime::path::PathProofObservation;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, reliable_path_command_channels,
    try_recv_reliable_path_command, try_recv_reliable_path_priority_command,
};
use crate::runtime::stream::ReliableRelayAttachOutcome;
use crate::runtime::stream::request::RequestStreamState;

struct QuicCapacityFixture {
    stream_id: StreamId,
    context: ClientPathContext,
    remotes: ReliableRelayRemoteSet,
    reference: RelayPathInstance,
    tcp_candidate_rx: ReliablePathCommandReceivers,
    candidate: RelayPathInstance,
    candidate_rx: ReliablePathCommandReceivers,
}

impl QuicCapacityFixture {
    fn new(stream_id: StreamId) -> Self {
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10320?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10321?srtt-ms=30&rate-mbps=500",
            "udp://127.0.0.1:10322?srtt-ms=40&rate-mbps=500",
            // This configured path is deliberately unattached. It still owns
            // one topology-stable share of the bounded session budget.
            "udp://127.0.0.1:10323?srtt-ms=60&rate-mbps=500",
        ]);
        let (reference_commands, mut reference_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream_with_underlay(
                stream_id,
                UnderlayProtocol::Tcp,
                0,
                reference_commands,
            ),
            8,
        );
        consume_attachment_proof(&mut reference_rx);

        let (tcp_candidate_commands, mut tcp_candidate_rx) = reliable_path_command_channels(8);
        assert_eq!(
            remotes.attach(opened_test_relay_stream_with_underlay(
                stream_id,
                UnderlayProtocol::Tcp,
                1,
                tcp_candidate_commands,
            )),
            ReliableRelayAttachOutcome::Attached
        );
        consume_attachment_proof(&mut tcp_candidate_rx);

        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
        assert_eq!(
            remotes.attach(opened_test_relay_stream_with_underlay(
                stream_id,
                UnderlayProtocol::Udp,
                0,
                candidate_commands,
            )),
            ReliableRelayAttachOutcome::Attached
        );
        consume_attachment_proof(&mut candidate_rx);

        let reference = path_instance(&remotes, UnderlayProtocol::Tcp, 0);
        let candidate = path_instance(&remotes, UnderlayProtocol::Udp, 0);
        Self {
            stream_id,
            context,
            remotes,
            reference,
            tcp_candidate_rx,
            candidate,
            candidate_rx,
        }
    }

    fn mature_reference(&self, request: &mut RequestStreamState) -> RequestPerFlowRateModel {
        let model = RequestPerFlowRateModel {
            rate_bps: 100_000_000.0,
            delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        };
        request
            .path_states
            .get_mut(self.reference)
            .set_per_flow_rate(model);
        model
    }

    fn confirm_candidate_attachment(&self) {
        confirm_attachment_proof(
            &self.context,
            &self.remotes,
            self.candidate,
            Duration::from_millis(10),
        );
    }
}

fn path_instance(
    remotes: &ReliableRelayRemoteSet,
    underlay: UnderlayProtocol,
    index: usize,
) -> RelayPathInstance {
    remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == underlay && path.key().index == index)
        .expect("attached path")
        .instance()
}

fn consume_attachment_proof(receivers: &mut ReliablePathCommandReceivers) {
    assert!(matches!(
        try_recv_reliable_path_priority_command(receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
}

fn confirm_attachment_proof(
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    instance: RelayPathInstance,
    elapsed: Duration,
) {
    let path = remotes
        .paths
        .iter()
        .find(|path| path.instance() == instance)
        .expect("attached path instance");
    let proof_id = path.path_proof_id.expect("queued attachment proof");
    context.mark_relay_path_proof_observation(
        instance.key.underlay,
        instance.key.index,
        PathProofObservation {
            proof_id,
            bytes: PATH_OPEN_SCORE_BYTES as u64,
            elapsed,
            sent_at: Instant::now(),
        },
    );
    assert!(context.relay_path_has_fresh_proof(
        instance.key.underlay,
        instance.key.index,
        proof_id,
        path.attached_at,
    ));
}

fn take_probe(receivers: &mut ReliablePathCommandReceivers) -> QuicCapacityProbeCommand {
    match try_recv_reliable_path_command(receivers) {
        Some(ReliablePathCommand::SendQuicCapacityProbe(probe)) => probe,
        _ => panic!("expected QUIC capacity probe"),
    }
}

#[tokio::test]
async fn quic_capacity_requires_bulk_reference_and_exact_udp_attachment_proof() {
    let mut fixture = QuicCapacityFixture::new(StreamId(208));
    let mut request = RequestStreamState::default();
    let reference_model = fixture.mature_reference(&mut request);
    let mut controller = RequestQuicCapacityController::default();

    controller.try_start(
        fixture.stream_id,
        &request,
        &fixture.context,
        &fixture.remotes,
        TrafficClass::Throughput,
        Some((fixture.reference, reference_model)),
    );
    assert!(
        controller.active.is_none(),
        "an unconfirmed attachment is not data evidence"
    );

    fixture.confirm_candidate_attachment();
    controller.try_start(
        fixture.stream_id,
        &request,
        &fixture.context,
        &fixture.remotes,
        TrafficClass::Throughput,
        None,
    );
    controller.try_start(
        fixture.stream_id,
        &request,
        &fixture.context,
        &fixture.remotes,
        TrafficClass::Latency,
        Some((fixture.reference, reference_model)),
    );
    assert!(
        controller.active.is_none(),
        "capacity traffic requires both a mature reference and a bulk flow"
    );

    controller.try_start(
        fixture.stream_id,
        &request,
        &fixture.context,
        &fixture.remotes,
        TrafficClass::Throughput,
        Some((fixture.reference, reference_model)),
    );

    assert!(try_recv_reliable_path_command(&mut fixture.tcp_candidate_rx).is_none());
    let probe = take_probe(&mut fixture.candidate_rx);
    assert_eq!(probe.stream_id, fixture.stream_id);
    assert_eq!(probe.path_instance, fixture.candidate);
    assert_eq!(probe.path_id, PathId(fixture.candidate.key.index as u16));
}

#[tokio::test]
async fn quic_capacity_respects_flight_and_budget_bounds_and_does_not_restart() {
    let mut fixture = QuicCapacityFixture::new(StreamId(209));
    fixture.confirm_candidate_attachment();
    let mut request = RequestStreamState::default();
    let reference_model = fixture.mature_reference(&mut request);
    let reference = Some((fixture.reference, reference_model));
    let mut controller = RequestQuicCapacityController::default();

    fixture
        .context
        .health()
        .lock()
        .expect("path health lock")
        .udp[fixture.candidate.key.index]
        .relay_bytes_in_flight = 1;
    controller.try_start(
        fixture.stream_id,
        &request,
        &fixture.context,
        &fixture.remotes,
        TrafficClass::Throughput,
        reference,
    );
    assert!(controller.active.is_none());
    assert!(controller.attempted_paths.is_empty());

    {
        let mut health = fixture.context.health().lock().expect("path health lock");
        health.udp[fixture.candidate.key.index].relay_bytes_in_flight = 0;
        health.udp[fixture.candidate.key.index].carrier_queue_bytes = 1;
    }
    controller.try_start(
        fixture.stream_id,
        &request,
        &fixture.context,
        &fixture.remotes,
        TrafficClass::Throughput,
        reference,
    );
    assert!(controller.active.is_none());

    fixture
        .context
        .health()
        .lock()
        .expect("path health lock")
        .udp[fixture.candidate.key.index]
        .carrier_queue_bytes = 0;
    let eligible = fixture
        .context
        .automatic_bulk_path_count(UnderlayProtocol::Udp, None);
    assert_eq!(eligible, 2);
    let stable_share =
        request_capacity_stable_candidate_share_bytes(fixture.context.mux_limits, eligible);
    let session_before = fixture
        .context
        .request_quic_capacity_probe_remaining_bytes();
    controller.try_start(
        fixture.stream_id,
        &request,
        &fixture.context,
        &fixture.remotes,
        TrafficClass::Throughput,
        reference,
    );

    let probe = take_probe(&mut fixture.candidate_rx);
    assert!(probe.train_payload_bytes <= stable_share);
    assert!(probe.sample_floor_bytes <= probe.train_payload_bytes);
    assert!(probe.required_timed_carrier_bytes <= probe.sample_floor_bytes);
    assert_eq!(
        fixture
            .context
            .request_quic_capacity_probe_remaining_bytes(),
        session_before - probe.train_payload_bytes
    );
    assert_eq!(
        controller.attempted_paths,
        HashSet::from([fixture.candidate.key.index])
    );
    let active = controller.active.as_ref().expect("active measurement");
    assert_eq!(active.target, fixture.candidate);
    assert_eq!(active.token, probe.measurement_id);
    let query = controller.reconciliation_query().expect("exact query");
    assert_eq!(query.target, fixture.candidate);
    assert_eq!(query.token, probe.measurement_id);

    controller.try_start(
        fixture.stream_id,
        &request,
        &fixture.context,
        &fixture.remotes,
        TrafficClass::Throughput,
        reference,
    );
    assert!(
        try_recv_reliable_path_command(&mut fixture.candidate_rx).is_none(),
        "one stream cannot duplicate an active QUIC transaction"
    );
}

#[tokio::test]
async fn durable_native_udp_evidence_is_capacity_admitted_without_another_probe() {
    let mut fixture = QuicCapacityFixture::new(StreamId(210));
    fixture.confirm_candidate_attachment();
    let mut request = RequestStreamState::default();
    let reference_model = fixture.mature_reference(&mut request);
    let observed_at = Instant::now();
    {
        let mut health = fixture.context.health().lock().expect("path health lock");
        let candidate = &mut health.udp[fixture.candidate.key.index];
        candidate.carrier_delivery_rate_bps = Some(120_000_000.0);
        candidate.carrier_delivery_samples = 1;
        candidate.carrier_delivery_sample_bytes =
            reliable_capacity_measurement_session_limit_bytes(fixture.context.mux_limits);
        candidate.carrier_last_delivery_at = Some(observed_at);
        candidate.carrier_app_limited = false;
        candidate.carrier_ack_derived_data_seen = true;
    }

    assert_eq!(
        RequestQuicCapacityController::default()
            .native_evidence_targets(
                &fixture.context,
                &fixture.remotes,
                observed_at + Duration::from_nanos(1),
            )
            .collect::<Vec<_>>(),
        vec![fixture.candidate]
    );

    let mut controller = RequestQuicCapacityController::default();
    controller.try_start(
        fixture.stream_id,
        &request,
        &fixture.context,
        &fixture.remotes,
        TrafficClass::Throughput,
        Some((fixture.reference, reference_model)),
    );
    assert!(controller.active.is_none());
    assert!(controller.attempted_paths.is_empty());
    assert!(try_recv_reliable_path_command(&mut fixture.candidate_rx).is_none());
}
