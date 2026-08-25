use super::super::test_support::*;
use super::*;
use crate::model::capacity::RELIABLE_INITIAL_WINDOW_PACKETS;
use crate::model::request_capacity::{
    request_capacity_stable_candidate_share_bytes, request_tcp_capacity_measurement_geometry,
};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, reliable_path_command_channels,
    try_recv_reliable_path_command,
};
use crate::runtime::stream::request::{RequestAckClockOperation, RequestStreamState};
use std::collections::HashSet;

struct TcpCapacityFixture {
    stream_id: StreamId,
    context: ClientPathContext,
    remotes: ReliableRelayRemoteSet,
    reference: RelayPathInstance,
    candidate: RelayPathInstance,
    candidate_rx: ReliablePathCommandReceivers,
}

impl TcpCapacityFixture {
    fn new(stream_id: StreamId) -> Self {
        let context = client_test_context_with_paths(&[
            "quic://127.0.0.1:10330?initial-srtt-ms=20&initial-rate-mbps=500",
            "tcp://127.0.0.1:10331?initial-srtt-ms=80&initial-rate-mbps=500",
        ]);
        let (reference_commands, mut reference_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream_with_underlay(
                stream_id,
                UnderlayProtocol::Udp,
                0,
                reference_commands,
            ),
            8,
        );
        consume_client_path_proof_for_test(&mut reference_rx);
        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
        remotes.attach(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Tcp,
            0,
            candidate_commands,
        ));
        consume_client_path_proof_for_test(&mut candidate_rx);
        let reference = remotes
            .paths
            .iter()
            .find(|path| path.key().underlay == UnderlayProtocol::Udp)
            .expect("QUIC reference attachment")
            .instance();
        let candidate = remotes
            .paths
            .iter()
            .find(|path| path.key().underlay == UnderlayProtocol::Tcp)
            .expect("TCP candidate attachment")
            .instance();
        context.install_relay_path_instance_for_test(reference);
        context.install_relay_path_instance_for_test(candidate);
        Self {
            stream_id,
            context,
            remotes,
            reference,
            candidate,
            candidate_rx,
        }
    }

    fn mark_candidate_proven(&self) {
        mark_client_path_proof_fresh_for_test(
            &self.context,
            &self.remotes,
            self.candidate,
            std::time::Duration::from_millis(20),
        );
    }

    fn start(
        &self,
        controller: &mut RequestTcpCapacityController,
        request: &RequestStreamState,
        reference: Option<(RelayPathInstance, RequestPerFlowRateModel)>,
    ) {
        controller.try_start(
            self.stream_id,
            request,
            &self.context,
            &self.remotes,
            TrafficClass::Throughput,
            reference,
        );
    }
}

fn mature_reference_model() -> RequestPerFlowRateModel {
    RequestPerFlowRateModel {
        rate_bps: 200_000_000.0,
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    }
}

fn request_with_reference(
    reference: RelayPathInstance,
    model: RequestPerFlowRateModel,
) -> RequestStreamState {
    let mut request = RequestStreamState::default();
    request
        .path_states
        .get_mut(reference)
        .set_per_flow_rate(model);
    request
}

#[tokio::test]
async fn tcp_measurement_requires_fresh_candidate_proof_and_mature_reference() {
    let mut fixture = TcpCapacityFixture::new(StreamId(211));
    let mature = mature_reference_model();
    let request = request_with_reference(fixture.reference, mature);
    let mut controller = RequestTcpCapacityController::default();

    fixture.start(&mut controller, &request, None);
    fixture.start(&mut controller, &request, Some((fixture.reference, mature)));
    assert!(controller.measurements.is_empty());
    assert!(try_recv_reliable_path_command(&mut fixture.candidate_rx).is_none());

    fixture.mark_candidate_proven();
    let immature = RequestPerFlowRateModel {
        rate_bps: mature.rate_bps,
        delivery_samples: 1,
    };
    fixture.start(
        &mut controller,
        &request,
        Some((fixture.reference, immature)),
    );
    assert!(controller.measurements.is_empty());

    fixture.start(&mut controller, &request, Some((fixture.reference, mature)));
    let probe = match try_recv_reliable_path_command(&mut fixture.candidate_rx) {
        Some(ReliablePathCommand::SendTcpCapacityProbe(probe)) => probe,
        _ => panic!("expected one TCP receipt train"),
    };
    assert!(probe.valid_request_tcp_train());
    assert_eq!(probe.path_instance, fixture.candidate);
    assert_eq!(controller.measurements.len(), 1);
    assert!(controller.measurements.contains_key(&fixture.candidate));
}

#[tokio::test]
async fn tcp_measurement_bounds_existing_carrier_flight_and_does_not_restart() {
    let mut fixture = TcpCapacityFixture::new(StreamId(212));
    fixture.mark_candidate_proven();
    let model = mature_reference_model();
    let request = request_with_reference(fixture.reference, model);
    let mut controller = RequestTcpCapacityController::default();

    fixture
        .context
        .health()
        .lock()
        .expect("path health lock")
        .tcp[fixture.candidate.key.index]
        .relay_bytes_in_flight = 1;
    fixture.start(&mut controller, &request, Some((fixture.reference, model)));
    assert!(controller.measurements.is_empty());
    assert!(try_recv_reliable_path_command(&mut fixture.candidate_rx).is_none());

    const CARRIER_FLIGHT_BYTES: u64 = 64 * 1024;
    {
        let mut health = fixture.context.health().lock().expect("path health lock");
        health.tcp[fixture.candidate.key.index].relay_bytes_in_flight = 0;
        health.tcp[fixture.candidate.key.index].carrier_bytes_in_flight = CARRIER_FLIGHT_BYTES;
    }
    let candidate_snapshot = fixture
        .context
        .reliable_path_snapshot_for_instance(fixture.candidate)
        .expect("candidate snapshot");
    let eligible = fixture
        .context
        .automatic_bulk_path_count(UnderlayProtocol::Tcp, None);
    let stable_share =
        request_capacity_stable_candidate_share_bytes(fixture.context.mux_limits, eligible);
    let geometry = request_tcp_capacity_measurement_geometry(
        candidate_snapshot,
        model,
        fixture.context.mux_limits,
        stable_share,
    )
    .expect("bounded train geometry");

    fixture.start(&mut controller, &request, Some((fixture.reference, model)));
    let probe = match try_recv_reliable_path_command(&mut fixture.candidate_rx) {
        Some(ReliablePathCommand::SendTcpCapacityProbe(probe)) => probe,
        _ => panic!("expected bounded TCP receipt train"),
    };
    assert_eq!(probe.train_payload_bytes, geometry.train_bytes);
    assert!(probe.train_payload_bytes <= stable_share);
    assert!(probe.warmup_carrier_bytes >= CARRIER_FLIGHT_BYTES);

    fixture.start(&mut controller, &request, Some((fixture.reference, model)));
    assert!(try_recv_reliable_path_command(&mut fixture.candidate_rx).is_none());
    assert_eq!(controller.measurements.len(), 1);
    assert_eq!(
        controller.attempted_paths,
        HashSet::from([fixture.candidate.key.index]),
    );
}

#[tokio::test]
async fn tcp_measurement_fences_only_the_exact_ack_clock_attachment() {
    let mut fixture = TcpCapacityFixture::new(StreamId(213));
    fixture.mark_candidate_proven();
    let model = mature_reference_model();
    let mut exact = request_with_reference(fixture.reference, model);
    exact
        .path_states
        .get_mut(fixture.candidate)
        .mark_capacity_admitted();
    exact.ack_clock_operation = Some(RequestAckClockOperation::Pending {
        reference: fixture.reference,
        candidate: fixture.candidate,
    });
    let mut controller = RequestTcpCapacityController::default();

    fixture.start(&mut controller, &exact, Some((fixture.reference, model)));
    assert!(controller.measurements.is_empty());
    assert!(try_recv_reliable_path_command(&mut fixture.candidate_rx).is_none());

    let stale_candidate = RelayPathInstance {
        attachment_id: fixture.candidate.attachment_id.wrapping_add(1),
        ..fixture.candidate
    };
    let mut stale = request_with_reference(fixture.reference, model);
    stale
        .path_states
        .get_mut(stale_candidate)
        .mark_capacity_admitted();
    stale.ack_clock_operation = Some(RequestAckClockOperation::Pending {
        reference: fixture.reference,
        candidate: stale_candidate,
    });
    fixture.start(&mut controller, &stale, Some((fixture.reference, model)));
    let probe = match try_recv_reliable_path_command(&mut fixture.candidate_rx) {
        Some(ReliablePathCommand::SendTcpCapacityProbe(probe)) => probe,
        _ => panic!("expected current attachment measurement"),
    };
    assert_eq!(probe.path_instance, fixture.candidate);
}
