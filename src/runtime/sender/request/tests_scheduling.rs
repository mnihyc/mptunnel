use super::{
    BulkRelayPathChoice, BulkRelayPathRequest, ObservedBulkPathCandidate,
    ObservedOrdinaryPathChoice, RequestRelayPathObservation, RequestRelaySchedulingObservation,
    RequestSchedulingState, choose_bulk_relay_path_for_extent_avoiding,
    choose_observed_ordinary_data_path, observed_request_ack_clock_measurement_transaction,
    relay_path_snapshot_for_bulk_choice,
};
use crate::model::admission::BulkPathCandidate;
use crate::model::capacity::reliable_bulk_carrier_feed_quantum_bytes;
use crate::model::path::{
    CarrierPathInstanceId, RelayPathInstance, RelayPathKey, RelayPathProofEpoch,
};
use crate::model::request_evidence::RequestPerFlowRateModel;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathId, PathUsage, StreamId, UnderlayProtocol};
use crate::runtime::path::ReliableRequestTcpPathEvidence;
use crate::runtime::stream::request::{
    RequestAckClockOperation, RequestFlightLedger, RequestPathStates,
};
use crate::scheduler::{self, PathSnapshot, PathState, TrafficClass};
use bytes::Bytes;
use smallvec::SmallVec;
use std::time::Instant;

const PAYLOAD_BYTES: usize = 16 * 1024;

fn instance(underlay: UnderlayProtocol, index: usize, id: u64) -> RelayPathInstance {
    RelayPathInstance {
        key: RelayPathKey { underlay, index },
        path_instance_id: CarrierPathInstanceId::from_raw(id.max(1)),
        attachment_id: id,
    }
}

fn snapshot(instance: RelayPathInstance, srtt_ms: f64, rate_bps: f64) -> PathSnapshot {
    let mut snapshot = PathSnapshot::new(
        PathId(instance.key.index as u16),
        instance.key.underlay,
        srtt_ms,
        rate_bps,
    );
    snapshot.carrier_inflight_limit_bytes = 8 * 1024 * 1024;
    snapshot
}

fn observed_path(
    instance: RelayPathInstance,
    srtt_ms: f64,
    rate_bps: f64,
) -> RequestRelayPathObservation {
    let snapshot = snapshot(instance, srtt_ms, rate_bps);
    RequestRelayPathObservation {
        instance,
        can_enqueue_frame: true,
        can_enqueue_stream_lane: true,
        load_owned: true,
        shared_snapshot: Some(snapshot),
        tcp: (instance.key.underlay == UnderlayProtocol::Tcp).then_some(
            ReliableRequestTcpPathEvidence {
                startup_snapshot: snapshot,
                rate_hint_unknown: false,
            },
        ),
        has_bulk_model_evidence: true,
        has_fresh_native_carrier_rate_evidence: false,
        fresh_proof: Some(RelayPathProofEpoch {
            proof_id: id_for_proof(instance),
            proof_generation: 0,
            attached_at: Instant::now(),
        }),
        config_ordinal: instance.key.index,
        member_ordinal: 0,
    }
}

fn id_for_proof(instance: RelayPathInstance) -> u64 {
    instance.attachment_id.max(1)
}

fn scheduling_observation(
    paths: impl IntoIterator<Item = RequestRelayPathObservation>,
) -> RequestRelaySchedulingObservation {
    let paths = paths.into_iter().collect::<SmallVec<[_; 4]>>();
    let global_bulk_candidates = paths
        .iter()
        .filter_map(|path| {
            let snapshot = path.shared_snapshot?;
            let eta_ms = scheduler::score_path(snapshot, TrafficClass::Throughput, PAYLOAD_BYTES)
                .map_or(f64::INFINITY, |score| score.eta_ms);
            Some(ObservedBulkPathCandidate {
                candidate: BulkPathCandidate {
                    key: path.instance.key,
                    eta_ms,
                    has_liveness_evidence: true,
                    has_path_proof_evidence: path.fresh_proof.is_some(),
                    has_ack_data_evidence: path.has_bulk_model_evidence,
                    has_bulk_rate_evidence: path.has_bulk_model_evidence,
                    has_sender_delivery_evidence: path.has_bulk_model_evidence,
                    snapshot,
                },
                config_ordinal: path.config_ordinal,
                member_ordinal: path.member_ordinal,
            })
        })
        .collect();
    RequestRelaySchedulingObservation {
        stream_id: StreamId(7),
        membership_generation: 1,
        mux_limits: MuxLimits::default(),
        paths,
        global_bulk_candidates,
        latency_pressure: false,
    }
}

fn data_frame(offset: u64, len: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(7),
        offset,
        payload: Bytes::from(vec![0x5a; len]),
    }
}

fn original_flights(owner: RelayPathInstance) -> RequestFlightLedger {
    let mut flights = RequestFlightLedger::default();
    flights.record_original_frame_instance(owner, &data_frame(0, PAYLOAD_BYTES));
    flights
}

#[derive(Default)]
struct RequestEvidence {
    operation: Option<RequestAckClockOperation>,
    path_states: RequestPathStates,
}

impl RequestEvidence {
    fn prove_rate(mut self, instances: impl IntoIterator<Item = RelayPathInstance>) -> Self {
        for instance in instances {
            let state = self.path_states.get_mut(instance);
            state.mark_product_delivery_proven();
            state.mark_capacity_admitted();
        }
        self
    }

    fn prove_tcp_capacity(
        mut self,
        instances: impl IntoIterator<Item = RelayPathInstance>,
    ) -> Self {
        for instance in instances {
            let state = self.path_states.get_mut(instance);
            state.mark_capacity_admitted();
            state.mark_ack_clock_proven();
            state.set_per_flow_rate(RequestPerFlowRateModel {
                rate_bps: 800_000_000.0,
                delivery_samples: 10,
            });
        }
        self
    }

    fn prove_quic_product_delivery(mut self, instance: RelayPathInstance) -> Self {
        let state = self.path_states.get_mut(instance);
        state.mark_product_delivery_proven();
        state.mark_capacity_admitted();
        self
    }

    fn with_per_flow_rate(
        mut self,
        instance: RelayPathInstance,
        rate_bps: f64,
        delivery_samples: u32,
    ) -> Self {
        let state = self.path_states.get_mut(instance);
        state.mark_capacity_admitted();
        state.mark_ack_clock_proven();
        state.set_per_flow_rate(RequestPerFlowRateModel {
            rate_bps,
            delivery_samples,
        });
        self
    }

    fn pending_measurement(
        mut self,
        reference: RelayPathInstance,
        candidate: RelayPathInstance,
    ) -> Self {
        self.operation = Some(RequestAckClockOperation::Pending {
            reference,
            candidate,
        });
        let state = self.path_states.get_mut(candidate);
        state.mark_capacity_admitted();
        state.mark_ack_clock_first_window();
        self
    }

    fn admit_ack_clock_candidate(mut self, candidate: RelayPathInstance) -> Self {
        let state = self.path_states.get_mut(candidate);
        state.mark_capacity_admitted();
        state.mark_ack_clock_first_window();
        self
    }

    fn state(&self) -> RequestSchedulingState<'_> {
        RequestSchedulingState {
            operation: self.operation,
            path_states: &self.path_states,
        }
    }
}

fn choose_bulk(
    observation: &RequestRelaySchedulingObservation,
    flights: &RequestFlightLedger,
    evidence: Option<&RequestEvidence>,
) -> BulkRelayPathChoice {
    choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
        observation,
        lane: TrafficClass::Throughput,
        offset: PAYLOAD_BYTES as u64,
        payload_bytes: PAYLOAD_BYTES,
        cursor: 0,
        avoid_instances: &[],
        path_flights: Some(flights),
        request_state: evidence.map(RequestEvidence::state),
    })
}

#[test]
fn ordinary_data_chooses_lowest_eta_independent_of_attachment_order() {
    let slow = instance(UnderlayProtocol::Udp, 0, 10);
    let fast = instance(UnderlayProtocol::Udp, 1, 11);
    let slow_path = observed_path(slow, 80.0, 50_000_000.0);
    let fast_path = observed_path(fast, 8.0, 500_000_000.0);

    for paths in [[slow_path, fast_path], [fast_path, slow_path]] {
        let observation = scheduling_observation(paths);
        let no_flights = RequestFlightLedger::default();
        assert_eq!(
            choose_observed_ordinary_data_path(
                &observation,
                TrafficClass::Throughput,
                PAYLOAD_BYTES,
                0,
                &[],
            ),
            ObservedOrdinaryPathChoice::Selected(fast),
        );
        assert_eq!(
            choose_bulk(&observation, &no_flights, None),
            BulkRelayPathChoice::Selected(fast),
            "attachment order must not become scheduling authority",
        );
    }
}

#[test]
fn ordinary_data_uses_available_path_before_faster_backup() {
    let available = instance(UnderlayProtocol::Tcp, 0, 12);
    let backup = instance(UnderlayProtocol::Udp, 0, 13);
    let available_path = observed_path(available, 80.0, 50_000_000.0);
    let mut backup_path = observed_path(backup, 5.0, 1_000_000_000.0);
    backup_path
        .shared_snapshot
        .as_mut()
        .expect("backup snapshot")
        .peer_usage = Some(PathUsage::Backup);
    let observation = scheduling_observation([backup_path, available_path]);

    assert_eq!(
        choose_observed_ordinary_data_path(
            &observation,
            TrafficClass::Throughput,
            PAYLOAD_BYTES,
            0,
            &[],
        ),
        ObservedOrdinaryPathChoice::Selected(available),
    );
}

#[test]
fn ordinary_data_avoidance_ranks_only_within_available_paths() {
    let available = instance(UnderlayProtocol::Tcp, 0, 14);
    let backup = instance(UnderlayProtocol::Udp, 0, 15);
    let available_path = observed_path(available, 80.0, 50_000_000.0);
    let mut peer_backup = observed_path(backup, 5.0, 1_000_000_000.0);
    peer_backup
        .shared_snapshot
        .as_mut()
        .expect("backup snapshot")
        .peer_usage = Some(PathUsage::Backup);
    let observation = scheduling_observation([peer_backup, available_path]);

    assert_eq!(
        choose_observed_ordinary_data_path(
            &observation,
            TrafficClass::Throughput,
            PAYLOAD_BYTES,
            0,
            &[available],
        ),
        ObservedOrdinaryPathChoice::Selected(available),
        "reuse avoidance cannot promote a peer Backup over a schedulable Available path",
    );

    let mut local_backup = observed_path(backup, 5.0, 1_000_000_000.0);
    local_backup
        .shared_snapshot
        .as_mut()
        .expect("backup snapshot")
        .policy
        .backup = true;
    let observation =
        scheduling_observation([local_backup, observed_path(available, 80.0, 50_000_000.0)]);
    assert_eq!(
        choose_observed_ordinary_data_path(
            &observation,
            TrafficClass::Throughput,
            PAYLOAD_BYTES,
            0,
            &[available],
        ),
        ObservedOrdinaryPathChoice::Selected(available),
        "reuse avoidance cannot promote a locally configured Backup either",
    );
}

#[test]
fn ordinary_data_excludes_failed_and_draining_paths() {
    let failed = instance(UnderlayProtocol::Udp, 0, 20);
    let draining = instance(UnderlayProtocol::Tcp, 1, 21);
    let available = instance(UnderlayProtocol::Udp, 2, 22);
    let mut failed_path = observed_path(failed, 1.0, 10_000_000_000.0);
    failed_path
        .shared_snapshot
        .as_mut()
        .expect("snapshot")
        .state = PathState::Failed;
    let mut draining_path = observed_path(draining, 1.0, 10_000_000_000.0);
    draining_path
        .shared_snapshot
        .as_mut()
        .expect("snapshot")
        .state = PathState::Draining;
    let available_path = observed_path(available, 100.0, 10_000_000.0);
    let observation = scheduling_observation([failed_path, draining_path, available_path]);

    assert_eq!(
        choose_observed_ordinary_data_path(
            &observation,
            TrafficClass::Throughput,
            PAYLOAD_BYTES,
            0,
            &[],
        ),
        ObservedOrdinaryPathChoice::Selected(available),
    );
}

#[test]
fn exact_lower_flight_owner_continues_within_measured_hysteresis() {
    let challenger = instance(UnderlayProtocol::Udp, 0, 30);
    let owner = instance(UnderlayProtocol::Udp, 1, 31);
    let mut owner_path = observed_path(owner, 10.0, 500_000_000.0);
    let owner_snapshot = owner_path.shared_snapshot.as_mut().expect("snapshot");
    owner_snapshot.jitter_ms = 2.0;
    owner_snapshot.data_level_limit_bytes = PAYLOAD_BYTES as u64;
    owner_snapshot.data_level_bytes_in_flight = (2 * PAYLOAD_BYTES) as u64;
    let mut challenger_path = observed_path(challenger, 8.0, 500_000_000.0);
    challenger_path
        .shared_snapshot
        .as_mut()
        .expect("snapshot")
        .jitter_ms = 2.0;
    let observation = scheduling_observation([challenger_path, owner_path]);
    let flights = original_flights(owner);
    let evidence = RequestEvidence::default().prove_rate([owner, challenger]);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(owner),
        "the exact lower-flight owner remains native-work-conserving inside measured jitter",
    );
}

#[test]
fn immature_per_flow_rate_does_not_replace_durable_tcp_capacity() {
    let path_instance = instance(UnderlayProtocol::Tcp, 0, 35);
    let mut path = observed_path(path_instance, 180.0, 200_000_000.0);
    path.tcp
        .as_mut()
        .expect("TCP evidence")
        .startup_snapshot
        .delivery_rate_bps = 1_000_000.0;
    let observation = scheduling_observation([path]);
    let immature = RequestEvidence::default().with_per_flow_rate(path_instance, 3_000_000.0, 1);

    let retained = relay_path_snapshot_for_bulk_choice(
        &observation,
        path_instance,
        Some(path_instance.key),
        Some(immature.state()),
        true,
    )
    .expect("retained capacity snapshot");
    assert_eq!(retained.delivery_rate_bps, 200_000_000.0);
    assert_eq!(
        retained.rate_scope,
        crate::scheduler::PathRateScope::PathCapacity
    );

    let mature = RequestEvidence::default().with_per_flow_rate(path_instance, 3_000_000.0, 10);
    let measured = relay_path_snapshot_for_bulk_choice(
        &observation,
        path_instance,
        Some(path_instance.key),
        Some(mature.state()),
        true,
    )
    .expect("mature per-flow snapshot");
    assert_eq!(measured.delivery_rate_bps, 3_000_000.0);
    assert_eq!(
        measured.rate_scope,
        crate::scheduler::PathRateScope::PerFlowGoodput
    );
}

#[test]
fn quic_request_snapshot_publishes_native_service_window() {
    let path_instance = instance(UnderlayProtocol::Udp, 0, 36);
    let mut path = observed_path(path_instance, 180.0, 2_000_000.0);
    let native_window = 2 * 1024 * 1024;
    let shared = path.shared_snapshot.as_mut().expect("shared snapshot");
    shared.carrier_inflight_limit_bytes = native_window;
    shared.app_limited = true;
    let observation = scheduling_observation([path]);

    let snapshot = relay_path_snapshot_for_bulk_choice(
        &observation,
        path_instance,
        Some(path_instance.key),
        None,
        true,
    )
    .expect("QUIC request snapshot");

    assert_eq!(
        snapshot.data_level_limit_bytes,
        native_window + reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default()) as u64,
    );
    assert_eq!(snapshot.delivery_rate_bps, 2_000_000.0);
}

#[test]
fn tcp_request_snapshot_publishes_native_service_window() {
    let path_instance = instance(UnderlayProtocol::Tcp, 0, 39);
    let mut path = observed_path(path_instance, 180.0, 2_000_000.0);
    let native_window = 2 * 1024 * 1024;
    let shared = path.shared_snapshot.as_mut().expect("shared snapshot");
    shared.carrier_inflight_limit_bytes = native_window;
    shared.app_limited = true;
    let observation = scheduling_observation([path]);

    let snapshot = relay_path_snapshot_for_bulk_choice(
        &observation,
        path_instance,
        Some(path_instance.key),
        None,
        true,
    )
    .expect("TCP request snapshot");

    assert_eq!(
        snapshot.data_level_limit_bytes,
        native_window + reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default()) as u64,
        "TCP request scheduling must feed the native window without replacing socket backpressure",
    );
    assert_eq!(snapshot.delivery_rate_bps, 2_000_000.0);
}

#[test]
fn retained_completion_evidence_outweighs_an_open_app_limited_window() {
    let reference = instance(UnderlayProtocol::Udp, 0, 37);
    let candidate = instance(UnderlayProtocol::Udp, 1, 38);
    let mut reference_path = observed_path(reference, 40.0, 400_000_000.0);
    let reference_snapshot = reference_path
        .shared_snapshot
        .as_mut()
        .expect("reference snapshot");
    reference_snapshot.app_limited = true;
    reference_snapshot.queue_bytes = reference_snapshot.carrier_inflight_limit_bytes;
    let mut candidate_path = observed_path(candidate, 40.0, 1_000_000.0);
    candidate_path
        .shared_snapshot
        .as_mut()
        .expect("candidate snapshot")
        .app_limited = true;
    let observation = scheduling_observation([reference_path, candidate_path]);
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default().prove_rate([reference, candidate]);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(reference),
        "an app-limited instant cannot revoke the candidate's retained completion evidence",
    );
}

#[test]
fn blocked_validated_quic_path_does_not_serialize_an_independent_quic_path() {
    let reference = instance(UnderlayProtocol::Tcp, 0, 40);
    let candidate = instance(UnderlayProtocol::Udp, 1, 41);
    let second_candidate = instance(UnderlayProtocol::Udp, 2, 42);
    let reference_path = observed_path(reference, 20.0, 200_000_000.0);
    let mut candidate_path = observed_path(candidate, 30.0, 150_000_000.0);
    candidate_path.has_bulk_model_evidence = false;
    candidate_path.can_enqueue_frame = false;
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default().prove_rate([reference]);

    let mut second_path = observed_path(second_candidate, 1.0, 1_000_000_000.0);
    second_path.has_bulk_model_evidence = false;
    let with_second = scheduling_observation([reference_path, candidate_path, second_path]);
    assert_eq!(
        choose_bulk(&with_second, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(second_candidate),
        "one blocked validated carrier must not serialize another independently admitted QUIC path",
    );
}

#[test]
fn measured_cross_tcp_quic_path_can_join_bulk_scheduling() {
    let quic_reference = instance(UnderlayProtocol::Udp, 0, 50);
    let tcp_candidate = instance(UnderlayProtocol::Tcp, 0, 51);
    let reference_path = observed_path(quic_reference, 100.0, 20_000_000.0);
    let candidate_path = observed_path(tcp_candidate, 5.0, 800_000_000.0);
    let observation = scheduling_observation([reference_path, candidate_path]);
    let flights = original_flights(quic_reference);
    let evidence = RequestEvidence::default()
        .prove_rate([quic_reference])
        .prove_tcp_capacity([tcp_candidate]);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(tcp_candidate),
        "transport family must not override receiver-proven capacity",
    );
}

#[test]
fn exact_quic_data_ack_progress_joins_normal_bulk_scheduling() {
    let reference = instance(UnderlayProtocol::Udp, 0, 54);
    let candidate = instance(UnderlayProtocol::Udp, 1, 55);
    let reference_path = observed_path(reference, 100.0, 20_000_000.0);
    let mut candidate_path = observed_path(candidate, 5.0, 800_000_000.0);
    candidate_path.has_bulk_model_evidence = false;
    let observation = scheduling_observation([reference_path, candidate_path]);
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default()
        .prove_rate([reference])
        .prove_quic_product_delivery(candidate);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(candidate),
        "exact QUIC product delivery must enter normal completion-based scheduling",
    );
}

#[test]
fn blocked_frontier_owner_remains_the_baseline_while_another_path_sends() {
    let reference = instance(UnderlayProtocol::Udp, 0, 52);
    let candidate = instance(UnderlayProtocol::Udp, 1, 53);
    let mut reference_path = observed_path(reference, 40.0, 200_000_000.0);
    reference_path.can_enqueue_frame = false;
    let candidate_path = observed_path(candidate, 20.0, 400_000_000.0);
    let observation = scheduling_observation([reference_path, candidate_path]);
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default().prove_rate([reference, candidate]);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(candidate),
        "a full congestion window on the ordered frontier must not idle another eligible path",
    );
}

#[test]
fn data_level_full_frontier_owner_remains_the_baseline_while_another_path_sends() {
    let reference = instance(UnderlayProtocol::Tcp, 0, 56);
    let candidate = instance(UnderlayProtocol::Udp, 1, 57);
    let mut reference_path = observed_path(reference, 40.0, 200_000_000.0);
    reference_path
        .shared_snapshot
        .as_mut()
        .expect("reference snapshot")
        .data_level_bytes_in_flight = MuxLimits::default().max_path_flight_bytes as u64;
    let candidate_path = observed_path(candidate, 20.0, 400_000_000.0);
    let observation = scheduling_observation([reference_path, candidate_path]);
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default().prove_rate([reference, candidate]);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(candidate),
        "a product-window-full DSN owner remains the ECF baseline instead of blocking every path",
    );
}

#[test]
fn validated_quic_extra_path_uses_ordinary_scheduling() {
    let reference = instance(UnderlayProtocol::Tcp, 0, 60);
    let candidate = instance(UnderlayProtocol::Udp, 0, 61);
    let reference_path = observed_path(reference, 40.0, 200_000_000.0);
    let mut candidate_path = observed_path(candidate, 1.0, 1_000_000_000.0);
    candidate_path.has_bulk_model_evidence = false;
    candidate_path.fresh_proof = None;
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default().prove_rate([reference]);
    let without_proof = scheduling_observation([reference_path, candidate_path]);

    assert_eq!(
        choose_bulk(&without_proof, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(reference),
    );

    candidate_path.fresh_proof = Some(RelayPathProofEpoch {
        proof_id: id_for_proof(candidate),
        proof_generation: 0,
        attached_at: Instant::now(),
    });
    let with_proof = scheduling_observation([reference_path, candidate_path]);
    assert_eq!(
        choose_bulk(&with_proof, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(candidate),
        "a validated QUIC path should carry ordinary bounded data without a calibration transaction",
    );
}

#[test]
fn validated_tcp_extra_path_uses_ordinary_scheduling() {
    let reference = instance(UnderlayProtocol::Tcp, 0, 62);
    let candidate = instance(UnderlayProtocol::Tcp, 1, 63);
    let reference_path = observed_path(reference, 40.0, 200_000_000.0);
    let mut candidate_path = observed_path(candidate, 1.0, 1_000_000_000.0);
    candidate_path.has_bulk_model_evidence = false;
    candidate_path.fresh_proof = None;
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default().prove_rate([reference]);
    let without_proof = scheduling_observation([reference_path, candidate_path]);

    assert_eq!(
        choose_bulk(&without_proof, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(reference),
    );

    candidate_path.fresh_proof = Some(RelayPathProofEpoch {
        proof_id: id_for_proof(candidate),
        proof_generation: 0,
        attached_at: Instant::now(),
    });
    let with_proof = scheduling_observation([reference_path, candidate_path]);
    assert_eq!(
        choose_bulk(&with_proof, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(candidate),
        "a validated TCP path should carry ordinary bounded data under native TCP send credit",
    );
}

#[test]
fn backup_quic_path_waits_until_ordinary_scheduling_can_admit_it() {
    let reference = instance(UnderlayProtocol::Udp, 0, 66);
    let candidate = instance(UnderlayProtocol::Udp, 1, 67);
    let reference_path = observed_path(reference, 40.0, 200_000_000.0);
    let mut candidate_path = observed_path(candidate, 10.0, 400_000_000.0);
    candidate_path.has_bulk_model_evidence = false;
    candidate_path
        .shared_snapshot
        .as_mut()
        .expect("candidate snapshot")
        .policy
        .backup = true;
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default().prove_rate([reference]);
    let backup = scheduling_observation([reference_path, candidate_path]);
    assert_eq!(
        choose_bulk(&backup, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(reference),
    );

    candidate_path
        .shared_snapshot
        .as_mut()
        .expect("candidate snapshot")
        .policy
        .backup = false;
    candidate_path.has_bulk_model_evidence = true;
    candidate_path.has_fresh_native_carrier_rate_evidence = true;
    let native = scheduling_observation([reference_path, candidate_path]);
    assert_eq!(
        choose_bulk(&native, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(candidate),
    );
}

#[test]
fn unavailable_quic_path_does_not_block_tcp_ack_clock_measurement() {
    let reference = instance(UnderlayProtocol::Udp, 0, 68);
    let quic_startup = instance(UnderlayProtocol::Udp, 1, 69);
    let tcp_candidate = instance(UnderlayProtocol::Tcp, 0, 70);
    let reference_path = observed_path(reference, 40.0, 200_000_000.0);
    let mut startup_path = observed_path(quic_startup, 40.0, 100_000_000.0);
    startup_path.has_bulk_model_evidence = false;
    startup_path.can_enqueue_frame = false;
    let candidate_path = observed_path(tcp_candidate, 40.0, 200_000_000.0);
    let observation = scheduling_observation([reference_path, startup_path, candidate_path]);
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default()
        .prove_rate([reference])
        .admit_ack_clock_candidate(tcp_candidate);
    assert!(matches!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::SelectedAckClockMeasurement {
            candidate,
            ..
        } if candidate == tcp_candidate
    ));
}

#[test]
fn tcp_product_ack_clock_is_only_a_native_capacity_fallback() {
    let reference = instance(UnderlayProtocol::Tcp, 0, 64);
    let candidate = instance(UnderlayProtocol::Tcp, 1, 65);
    let reference_path = observed_path(reference, 40.0, 200_000_000.0);
    let candidate_path = observed_path(candidate, 1.0, 1_000_000_000.0);
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default()
        .prove_rate([reference])
        .admit_ack_clock_candidate(candidate);

    let without_native_capacity = scheduling_observation([reference_path, candidate_path]);
    assert!(matches!(
        choose_bulk(&without_native_capacity, &flights, Some(&evidence)),
        BulkRelayPathChoice::SelectedAckClockMeasurement {
            candidate: selected,
            ..
        } if selected == candidate
    ));

    let mut native_candidate_path = candidate_path;
    native_candidate_path.has_fresh_native_carrier_rate_evidence = true;
    let with_native_capacity = scheduling_observation([reference_path, native_candidate_path]);
    assert_eq!(
        choose_bulk(&with_native_capacity, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(candidate),
        "native TCP delivery evidence must suppress redundant product calibration",
    );
}

#[test]
fn ack_clock_transaction_fences_exact_reference_and_candidate_instances() {
    let reference = instance(UnderlayProtocol::Udp, 0, 70);
    let candidate = instance(UnderlayProtocol::Tcp, 0, 71);
    let stale_reference = RelayPathInstance {
        attachment_id: reference.attachment_id + 100,
        ..reference
    };
    let mut reference_path = observed_path(reference, 20.0, 200_000_000.0);
    reference_path.has_bulk_model_evidence = true;
    let mut candidate_path = observed_path(candidate, 10.0, 300_000_000.0);
    candidate_path.can_enqueue_frame = false;
    let observation = scheduling_observation([reference_path, candidate_path]);
    let flights = original_flights(reference);
    let exact = RequestEvidence::default()
        .prove_rate([reference])
        .pending_measurement(reference, candidate);

    assert_eq!(
        observed_request_ack_clock_measurement_transaction(
            &observation.paths,
            reference.key,
            Some(exact.state()),
        ),
        Some(candidate),
    );
    assert_eq!(
        choose_bulk(&observation, &flights, Some(&exact)),
        BulkRelayPathChoice::SelectedAckClockMeasurementFence {
            reference,
            candidate,
        },
    );

    let stale = RequestEvidence::default()
        .prove_rate([reference])
        .pending_measurement(stale_reference, candidate);
    assert_eq!(
        observed_request_ack_clock_measurement_transaction(
            &observation.paths,
            reference.key,
            Some(stale.state()),
        ),
        None,
    );
    assert_eq!(
        choose_bulk(&observation, &flights, Some(&stale)),
        BulkRelayPathChoice::Selected(reference),
        "a transaction from an earlier attachment must not fence this attachment",
    );
}

#[test]
fn ack_clock_transaction_rejects_a_stale_candidate_instance() {
    let reference = instance(UnderlayProtocol::Udp, 0, 80);
    let candidate = instance(UnderlayProtocol::Tcp, 0, 81);
    let stale_candidate = RelayPathInstance {
        attachment_id: candidate.attachment_id + 100,
        ..candidate
    };
    let observation = scheduling_observation([
        observed_path(reference, 20.0, 200_000_000.0),
        observed_path(candidate, 10.0, 300_000_000.0),
    ]);
    let evidence = RequestEvidence::default()
        .prove_rate([reference])
        .pending_measurement(reference, stale_candidate);

    assert_eq!(
        observed_request_ack_clock_measurement_transaction(
            &observation.paths,
            reference.key,
            Some(evidence.state()),
        ),
        None,
    );
}
