use super::{
    BulkRelayPathChoice, BulkRelayPathRequest, ObservedBulkPathCandidate,
    ObservedOrdinaryPathChoice, RequestRelayPathObservation, RequestRelaySchedulingObservation,
    RequestSchedulingState, choose_bulk_relay_path_for_extent_avoiding,
    choose_observed_ordinary_data_path, choose_request_ack_clock_measurement_with_rates,
    observed_request_ack_clock_measurement_transaction, relay_path_snapshot_for_bulk_choice,
    request_snapshot_has_fresh_completion_rate,
};
use crate::model::ack_clock::reliable_request_ack_clock_measurement_target_bytes;
use crate::model::admission::{BulkPathCandidate, ReliableDataAckFrontierState};
use crate::model::capacity::{
    RELIABLE_INITIAL_WINDOW_PACKETS, reliable_bulk_carrier_feed_quantum_bytes,
    reliable_bulk_product_windows,
};
use crate::model::path::{
    CarrierPathInstanceId, RelayPathInstance, RelayPathKey, RelayPathProofEpoch,
};
use crate::model::request_evidence::RequestProductRateEpoch;
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
    snapshot.data_level_limit_bytes =
        reliable_bulk_product_windows(MuxLimits::default()).per_output_product_limit_bytes;
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
        carrier_pending_bytes: None,
        load_owned: true,
        shared_snapshot: Some(snapshot),
        startup_snapshot: Some(snapshot),
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
        native_authority_stamp: None,
        native_authority_unavailable: false,
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
        observed_at: Instant::now(),
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
            self.path_states
                .qualify_product_assignment_for_test(instance);
            let state = self.path_states.get_mut(instance);
            state.mark_product_path_use_proven();
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
            state.set_product_rate_epoch(RequestProductRateEpoch::for_test(800_000_000.0, 10));
        }
        self
    }

    fn prove_quic_product_delivery(mut self, instance: RelayPathInstance) -> Self {
        self.path_states
            .qualify_product_assignment_for_test(instance);
        let state = self.path_states.get_mut(instance);
        state.mark_product_path_use_proven();
        state.mark_capacity_admitted();
        self
    }

    fn prove_quic_product_progress_only(mut self, instance: RelayPathInstance) -> Self {
        self.path_states
            .get_mut(instance)
            .mark_product_path_use_proven();
        self
    }

    fn with_fresh_product_rate(
        mut self,
        instance: RelayPathInstance,
        rate_bps: f64,
        delivery_samples: u32,
    ) -> Self {
        let state = self.path_states.get_mut(instance);
        state.mark_capacity_admitted();
        state.mark_ack_clock_proven();
        state.set_product_rate_epoch(RequestProductRateEpoch::for_test(
            rate_bps,
            delivery_samples,
        ));
        self
    }

    fn with_product_rate_epoch(
        mut self,
        instance: RelayPathInstance,
        epoch: RequestProductRateEpoch,
    ) -> Self {
        let state = self.path_states.get_mut(instance);
        state.mark_capacity_admitted();
        state.mark_ack_clock_proven();
        state.set_product_rate_epoch(epoch);
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

    fn own_ack_clock_candidate(
        mut self,
        candidate: RelayPathInstance,
        target_bytes: u64,
        spent_bytes: u64,
    ) -> Self {
        self.operation = Some(RequestAckClockOperation::Owner {
            candidate,
            target_bytes,
        });
        let state = self.path_states.get_mut(candidate);
        state.mark_capacity_admitted();
        state.mark_ack_clock_first_window();
        state.set_ack_clock_measurement_target(target_bytes);
        state.set_ack_clock_measurement_bytes(spent_bytes);
        self
    }

    fn state(&self) -> RequestSchedulingState<'_> {
        RequestSchedulingState {
            operation: self.operation,
            path_states: &self.path_states,
            flights: None,
        }
    }
}

#[test]
fn product_assignment_qualification_is_durable_across_rate_expiry() {
    for (underlay, id) in [(UnderlayProtocol::Tcp, 91), (UnderlayProtocol::Udp, 92)] {
        let instance = instance(underlay, 0, id);
        let mut evidence = RequestEvidence::default();
        let authority_at = Instant::now();

        assert!(
            !evidence
                .state()
                .product_assignment_qualified(instance, authority_at),
            "an attachment without exact Product volume remains unqualified"
        );

        evidence
            .path_states
            .qualify_product_assignment_for_test(instance);
        assert!(
            evidence
                .state()
                .product_assignment_qualified(instance, authority_at),
            "exact Product-volume qualification does not require a native or numeric rate epoch"
        );

        evidence
            .path_states
            .get_mut(instance)
            .set_product_rate_epoch(
                RequestProductRateEpoch::new(
                    800_000_000.0,
                    10,
                    authority_at - std::time::Duration::from_secs(2),
                    std::time::Duration::from_secs(1),
                )
                .expect("expired Product rate epoch"),
            );
        assert!(
            evidence
                .state()
                .product_assignment_qualified(instance, authority_at),
            "numeric rate expiry cannot revoke exact Product-volume qualification for the same active incarnation"
        );
    }
}

fn choose_bulk(
    observation: &RequestRelaySchedulingObservation,
    flights: &RequestFlightLedger,
    evidence: Option<&RequestEvidence>,
) -> BulkRelayPathChoice {
    choose_bulk_at_frontier(
        observation,
        flights,
        evidence,
        ReliableDataAckFrontierState::Live,
    )
}

fn choose_bulk_at_frontier(
    observation: &RequestRelaySchedulingObservation,
    flights: &RequestFlightLedger,
    evidence: Option<&RequestEvidence>,
    frontier_state: ReliableDataAckFrontierState,
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
        frontier_state,
    })
}

fn choose_measurement_annotation(
    observation: &RequestRelaySchedulingObservation,
    flights: &RequestFlightLedger,
    evidence: &RequestEvidence,
) -> Option<BulkRelayPathChoice> {
    let reference = flights.oldest_lower_flight_owner_before_offset(PAYLOAD_BYTES as u64)?;
    choose_request_ack_clock_measurement_with_rates(
        observation,
        TrafficClass::Throughput,
        PAYLOAD_BYTES as u64,
        PAYLOAD_BYTES,
        0,
        Some(reference),
        Some(flights),
        Some(evidence.state()),
    )
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
                None,
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
fn request_acquisition_cannot_preempt_ordinary_first_owner_establishment() {
    let ordinary_slow = instance(UnderlayProtocol::Udp, 0, 12);
    let ordinary_fast = instance(UnderlayProtocol::Tcp, 0, 13);
    let observation = scheduling_observation([
        observed_path(ordinary_slow, 80.0, 20_000_000.0),
        observed_path(ordinary_fast, 5.0, 800_000_000.0),
    ]);
    let no_flights = RequestFlightLedger::default();
    let evidence = RequestEvidence::default()
        .admit_ack_clock_candidate(ordinary_fast)
        .admit_ack_clock_candidate(ordinary_slow);

    assert_eq!(
        choose_bulk(&observation, &no_flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(ordinary_fast),
        "with O=0 the ordinary completion policy establishes the first exact owner; acquisition state cannot run before that commit",
    );
}

#[test]
fn ordinary_data_scores_unowned_path_with_joining_flow_projection() {
    let owned = instance(UnderlayProtocol::Tcp, 0, 16);
    let joining = instance(UnderlayProtocol::Tcp, 1, 17);
    let mut owned_path = observed_path(owned, 20.0, 100_000_000.0);
    owned_path
        .shared_snapshot
        .as_mut()
        .expect("owned snapshot")
        .active_flows = 1;
    let mut joining_path = observed_path(joining, 10.0, 100_000_000.0);
    joining_path.load_owned = false;
    joining_path
        .shared_snapshot
        .as_mut()
        .expect("joining snapshot")
        .active_flows = 1;
    let observation = scheduling_observation([owned_path, joining_path]);

    assert_eq!(
        choose_observed_ordinary_data_path(
            &observation,
            TrafficClass::Throughput,
            1024 * 1024,
            0,
            &[],
            None,
        ),
        ObservedOrdinaryPathChoice::Selected(owned),
        "the unowned candidate must include the prospective joining flow in PathCapacity fair share",
    );
}

#[test]
fn t04b_request_contiguous_product_headroom_is_not_reclamped_by_inferred_bdp() {
    let owner = instance(UnderlayProtocol::Udp, 0, 201);
    let sibling = instance(UnderlayProtocol::Tcp, 0, 202);
    let mut favorable = observed_path(owner, 20.0, 500_000_000.0);
    favorable.has_fresh_native_carrier_rate_evidence = true;
    favorable
        .shared_snapshot
        .as_mut()
        .expect("owner snapshot")
        .carrier_delivery_rate_bps = Some(500_000_000.0);
    let mut adverse = favorable;
    let adverse_snapshot = adverse.shared_snapshot.as_mut().expect("owner snapshot");
    adverse_snapshot.delivery_rate_bps = 1_000_000.0;
    adverse_snapshot.pacing_rate_bps = 1_000_000.0;
    adverse_snapshot.carrier_delivery_rate_bps = Some(1_000_000.0);

    let mut blocked_sibling = observed_path(sibling, 20.0, 500_000_000.0);
    blocked_sibling.can_enqueue_frame = false;
    let favorable_snapshot = favorable.shared_snapshot.expect("owner snapshot");
    let adverse_snapshot = adverse.shared_snapshot.expect("owner snapshot");
    assert_eq!(
        (
            adverse.can_enqueue_frame,
            adverse.can_enqueue_stream_lane,
            adverse.load_owned,
            adverse.has_bulk_model_evidence,
            adverse.has_fresh_native_carrier_rate_evidence,
            adverse.fresh_proof,
            adverse_snapshot.state,
            adverse_snapshot.policy,
            adverse_snapshot.queue_bytes,
            adverse_snapshot.bytes_in_flight,
            adverse_snapshot.carrier_inflight_limit_bytes,
        ),
        (
            favorable.can_enqueue_frame,
            favorable.can_enqueue_stream_lane,
            favorable.load_owned,
            favorable.has_bulk_model_evidence,
            favorable.has_fresh_native_carrier_rate_evidence,
            favorable.fresh_proof,
            favorable_snapshot.state,
            favorable_snapshot.policy,
            favorable_snapshot.queue_bytes,
            favorable_snapshot.bytes_in_flight,
            favorable_snapshot.carrier_inflight_limit_bytes,
        ),
        "the variants share lifecycle, queue admission, evidence, and configured carrier limits",
    );
    assert_eq!(
        (
            adverse_snapshot.data_level_queue_bytes,
            adverse_snapshot.data_level_bytes_in_flight,
            adverse_snapshot.data_level_limit_bytes,
        ),
        (
            favorable_snapshot.data_level_queue_bytes,
            favorable_snapshot.data_level_bytes_in_flight,
            favorable_snapshot.data_level_limit_bytes,
        ),
        "the variants share Product P and exact debt",
    );

    let mut flights = RequestFlightLedger::default();
    flights.record_original_frame_instance(owner, &data_frame(0, PAYLOAD_BYTES));
    flights
        .record_original_frame_instance(sibling, &data_frame(PAYLOAD_BYTES as u64, PAYLOAD_BYTES));
    let evidence = RequestEvidence::default().prove_rate([owner, sibling]);
    let choose_owner = |owner_path| {
        let observation = scheduling_observation([owner_path, blocked_sibling]);
        choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
            observation: &observation,
            lane: TrafficClass::Throughput,
            offset: (2 * PAYLOAD_BYTES) as u64,
            payload_bytes: PAYLOAD_BYTES,
            cursor: 0,
            avoid_instances: &[],
            path_flights: Some(&flights),
            request_state: Some(evidence.state()),
            frontier_state: ReliableDataAckFrontierState::Live,
        })
    };

    assert_eq!(
        choose_owner(favorable),
        BulkRelayPathChoice::Selected(owner)
    );
    assert_eq!(
        choose_owner(adverse),
        BulkRelayPathChoice::Selected(owner),
        "an inferred low-rate BDP may rank the only enqueueable contiguous owner poorly, but cannot shrink unchanged Product/resource authority below its exact cross-path debt",
    );
}

#[test]
fn repair_selection_ignores_native_unused_credit_acquisition() {
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let fast_underlay = match underlay {
            UnderlayProtocol::Tcp => UnderlayProtocol::Udp,
            UnderlayProtocol::Udp => UnderlayProtocol::Tcp,
        };
        let fast = instance(fast_underlay, 0, 70);
        let underfed = instance(underlay, 1, 71);
        let avoided_unrelated = instance(UnderlayProtocol::Tcp, 9, 72);
        let fast_path = observed_path(fast, 10.0, 500_000_000.0);
        let mut underfed_path = observed_path(underfed, 100.0, 1_000_000.0);
        underfed_path
            .shared_snapshot
            .as_mut()
            .expect("underfed snapshot")
            .app_limited = true;
        let observation = scheduling_observation([fast_path, underfed_path]);

        assert_eq!(
            choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
                observation: &observation,
                lane: TrafficClass::Throughput,
                offset: PAYLOAD_BYTES as u64,
                payload_bytes: PAYLOAD_BYTES,
                cursor: 0,
                avoid_instances: &[avoided_unrelated],
                path_flights: None,
                request_state: None,
                frontier_state: ReliableDataAckFrontierState::Live,
            }),
            BulkRelayPathChoice::Selected(fast),
            "repair/reinjection retains ordinary completion ordering for an underfed {underlay:?} carrier",
        );
    }
}

#[test]
fn normal_request_bulk_can_feed_current_underfed_quic_credit() {
    let owner = instance(UnderlayProtocol::Tcp, 0, 73);
    let underfed = instance(UnderlayProtocol::Udp, 1, 74);
    let mut owner_path = observed_path(owner, 100.0, 500_000_000.0);
    let owner_snapshot = owner_path.shared_snapshot.as_mut().expect("owner snapshot");
    owner_snapshot.jitter_ms = 3.0;
    owner_snapshot.bytes_in_flight = PAYLOAD_BYTES as u64;
    owner_snapshot.data_level_bytes_in_flight = PAYLOAD_BYTES as u64;
    let mut underfed_path = observed_path(underfed, 101.0, 500_000_000.0);
    let underfed_snapshot = underfed_path
        .shared_snapshot
        .as_mut()
        .expect("underfed snapshot");
    underfed_snapshot.jitter_ms = 3.0;
    underfed_snapshot.app_limited = true;
    let observation = scheduling_observation([owner_path, underfed_path]);
    let flights = original_flights(owner);
    let evidence = RequestEvidence::default().prove_rate([owner, underfed]);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(underfed),
        "normal bulk placement may feed an already-admitted QUIC controller before owner hysteresis",
    );
}

#[test]
fn tcp_delivery_sample_app_limited_flag_does_not_preempt_request_owner() {
    let owner = instance(UnderlayProtocol::Udp, 0, 77);
    let sampled_tcp = instance(UnderlayProtocol::Tcp, 1, 78);
    let mut owner_path = observed_path(owner, 100.0, 500_000_000.0);
    let owner_snapshot = owner_path.shared_snapshot.as_mut().expect("owner snapshot");
    owner_snapshot.jitter_ms = 3.0;
    owner_snapshot.bytes_in_flight = PAYLOAD_BYTES as u64;
    owner_snapshot.data_level_bytes_in_flight = PAYLOAD_BYTES as u64;
    let mut sampled_path = observed_path(sampled_tcp, 101.0, 500_000_000.0);
    let sampled_snapshot = sampled_path
        .shared_snapshot
        .as_mut()
        .expect("sampled TCP snapshot");
    sampled_snapshot.jitter_ms = 3.0;
    sampled_snapshot.app_limited = true;
    let observation = scheduling_observation([owner_path, sampled_path]);
    let flights = original_flights(owner);
    let evidence = RequestEvidence::default().prove_rate([owner, sampled_tcp]);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(owner),
        "TCP_INFO delivery-sample classification cannot override lower-owner hysteresis",
    );
}

#[test]
fn materially_slower_underfed_request_credit_cannot_preempt_the_live_frontier() {
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let owner_underlay = match underlay {
            UnderlayProtocol::Tcp => UnderlayProtocol::Udp,
            UnderlayProtocol::Udp => UnderlayProtocol::Tcp,
        };
        let owner = instance(owner_underlay, 0, 75);
        let underfed = instance(underlay, 1, 76);
        let mut owner_path = observed_path(owner, 80.0, 555_000_000.0);
        let owner_snapshot = owner_path.shared_snapshot.as_mut().expect("owner snapshot");
        owner_snapshot.jitter_ms = 20.0;
        owner_snapshot.bytes_in_flight = PAYLOAD_BYTES as u64;
        owner_snapshot.data_level_bytes_in_flight = PAYLOAD_BYTES as u64;

        let mut underfed_path = observed_path(underfed, 100.0, 351_000.0);
        underfed_path.has_bulk_model_evidence = false;
        let underfed_snapshot = underfed_path
            .shared_snapshot
            .as_mut()
            .expect("underfed snapshot");
        underfed_snapshot.jitter_ms = 20.0;
        underfed_snapshot.app_limited = true;
        let owner_eta =
            scheduler::score_path(*owner_snapshot, TrafficClass::Throughput, PAYLOAD_BYTES)
                .expect("owner score")
                .eta_ms;
        let underfed_eta =
            scheduler::score_path(*underfed_snapshot, TrafficClass::Throughput, PAYLOAD_BYTES)
                .expect("underfed score")
                .eta_ms;
        assert!(
            underfed_eta > owner_eta + owner_snapshot.jitter_ms,
            "the fixture must reproduce a materially later underfed carrier: owner={owner_eta:.3} ms underfed={underfed_eta:.3} ms",
        );

        let observation = scheduling_observation([owner_path, underfed_path]);
        let flights = original_flights(owner);
        let evidence = RequestEvidence::default().prove_rate([owner, underfed]);
        assert_eq!(
            choose_bulk(&observation, &flights, Some(&evidence)),
            BulkRelayPathChoice::Selected(owner),
            "current native starvation is acquisition evidence, not authority to put a materially later {underlay:?} request range below the live frontier",
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
            None,
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
            None,
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
            None,
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
            None,
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
fn request_contiguous_owner_does_not_treat_sampled_native_shape_as_enqueue_credit() {
    let owner = instance(UnderlayProtocol::Tcp, 0, 34);
    let alternate = instance(UnderlayProtocol::Udp, 1, 35);
    let mut owner_path = observed_path(owner, 10.0, 500_000_000.0);
    let owner_snapshot = owner_path.shared_snapshot.as_mut().expect("snapshot");
    owner_snapshot.carrier_inflight_limit_bytes = PAYLOAD_BYTES as u64;
    owner_snapshot.queue_bytes = 8 * 1024 * 1024;
    owner_snapshot.bytes_in_flight = 8 * 1024 * 1024;
    owner_snapshot.data_level_limit_bytes = PAYLOAD_BYTES as u64;
    owner_snapshot.data_level_bytes_in_flight = 512 * 1024 - 1;
    let mut alternate_path = observed_path(alternate, 8.0, 500_000_000.0);
    alternate_path.can_enqueue_frame = false;
    let observation = scheduling_observation([owner_path, alternate_path]);
    let flights = original_flights(owner);
    let evidence = RequestEvidence::default().prove_rate([owner, alternate]);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(owner),
        "carrier-wide TCP queue samples do not consume the flow's Product window while exact Product debt still has headroom",
    );
}

#[test]
fn live_request_frontier_cannot_bypass_an_exhausted_product_window() {
    let owner = instance(UnderlayProtocol::Tcp, 0, 32);
    let alternate = instance(UnderlayProtocol::Udp, 1, 33);
    let mut owner_path = observed_path(owner, 10.0, 500_000_000.0);
    let owner_snapshot = owner_path.shared_snapshot.as_mut().expect("snapshot");
    owner_snapshot.data_level_bytes_in_flight = MuxLimits::default().max_path_flight_bytes as u64;
    let mut alternate_path = observed_path(alternate, 8.0, 500_000_000.0);
    alternate_path.can_enqueue_frame = false;
    let observation = scheduling_observation([owner_path, alternate_path]);
    let flights = original_flights(owner);
    let evidence = RequestEvidence::default().prove_rate([owner, alternate]);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Blocked,
        "a live contiguous owner cannot renew Product debt merely because TCP has native credit",
    );
    assert_eq!(
        choose_bulk_at_frontier(
            &observation,
            &flights,
            Some(&evidence),
            ReliableDataAckFrontierState::AuthoritativeGap,
        ),
        BulkRelayPathChoice::Blocked,
        "an authoritative gap remains blocked by the same exhausted Product authority",
    );
}

#[test]
fn tcp_product_lower_bound_cannot_downshift_baseline_without_fresh_native_capacity() {
    let path_instance = instance(UnderlayProtocol::Tcp, 0, 35);
    let mut path = observed_path(path_instance, 180.0, 200_000_000.0);
    path.has_fresh_native_carrier_rate_evidence = false;
    path.tcp
        .as_mut()
        .expect("TCP evidence")
        .startup_snapshot
        .delivery_rate_bps = 1_000_000.0;
    let observation = scheduling_observation([path]);
    let immature =
        RequestEvidence::default().with_fresh_product_rate(path_instance, 3_000_000.0, 1);

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
    assert_eq!(retained.product_progress_rate_bps, None);
    assert!(!retained.has_durable_product_progress);

    let boundary_minus_one = RequestEvidence::default().with_fresh_product_rate(
        path_instance,
        900_000_000.0,
        RELIABLE_INITIAL_WINDOW_PACKETS as u32 - 1,
    );
    let retained_high_point = relay_path_snapshot_for_bulk_choice(
        &observation,
        path_instance,
        Some(path_instance.key),
        Some(boundary_minus_one.state()),
        true,
    )
    .expect("sub-floor high Product point retains typed capacity prior");
    assert_eq!(retained_high_point.delivery_rate_bps, 200_000_000.0);
    assert_eq!(
        retained_high_point.rate_scope,
        crate::scheduler::PathRateScope::PathCapacity
    );
    assert_eq!(retained_high_point.product_progress_rate_bps, None);
    assert!(!retained_high_point.has_durable_product_progress);

    let mature = RequestEvidence::default().with_fresh_product_rate(
        path_instance,
        3_000_000.0,
        RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    );
    let measured = relay_path_snapshot_for_bulk_choice(
        &observation,
        path_instance,
        Some(path_instance.key),
        Some(mature.state()),
        true,
    )
    .expect("mature per-flow snapshot");
    assert_eq!(measured.delivery_rate_bps, 200_000_000.0);
    assert_eq!(
        measured.rate_scope,
        crate::scheduler::PathRateScope::PathCapacity,
        "a mature Product lower bound cannot downshift the configured path baseline when native rate is unavailable",
    );
    assert_eq!(measured.product_progress_rate_bps, Some(3_000_000.0));
    assert!(
        measured.has_durable_product_progress,
        "the mature exact Data ACK clock qualifies Product service without claiming physical-carrier capacity"
    );
}

#[test]
fn fresh_native_tcp_capacity_outweighs_mature_product_fallback() {
    let path_instance = instance(UnderlayProtocol::Tcp, 0, 37);
    let mut path = observed_path(path_instance, 100.0, 200_000_000.0);
    path.has_fresh_native_carrier_rate_evidence = true;
    path.shared_snapshot
        .as_mut()
        .expect("shared snapshot")
        .active_flows = 4;
    let observation = scheduling_observation([path]);
    let mature = RequestEvidence::default().with_fresh_product_rate(path_instance, 3_000_000.0, 10);

    let projected = relay_path_snapshot_for_bulk_choice(
        &observation,
        path_instance,
        Some(path_instance.key),
        Some(mature.state()),
        true,
    )
    .expect("fresh native capacity snapshot");

    assert_eq!(projected.delivery_rate_bps, 200_000_000.0);
    assert_eq!(
        projected.rate_scope,
        crate::scheduler::PathRateScope::PathCapacity,
        "a placement-limited Product sample must remain fallback to fresh native capacity",
    );
    assert_eq!(
        projected.data_level_limit_bytes,
        reliable_bulk_product_windows(MuxLimits::default()).per_output_product_limit_bytes,
        "native and Product rate provenance rank service but do not rewrite configured Product authority",
    );

    let projected_score = scheduler::score_path(projected, TrafficClass::Throughput, PAYLOAD_BYTES)
        .expect("projected score");
    let mut divided_capacity = projected;
    divided_capacity.delivery_rate_bps = 50_000_000.0;
    divided_capacity.active_flows = 1;
    let divided_score =
        scheduler::score_path(divided_capacity, TrafficClass::Throughput, PAYLOAD_BYTES)
            .expect("active-flow divided score");
    assert_eq!(
        projected_score.eta_ms, divided_score.eta_ms,
        "PathCapacity must retain the existing active-flow division",
    );

    let alternative = PathSnapshot::new(
        PathId(1),
        UnderlayProtocol::Tcp,
        projected.srtt_ms,
        40_000_000.0,
    );
    assert_eq!(
        scheduler::choose_path(
            &[projected, alternative],
            TrafficClass::Throughput,
            4 * 1024 * 1024,
        )
        .map(|score| score.path_id),
        Some(projected.id),
        "the 200 Mbps native carrier divided among four flows still outranks a 40 Mbps path",
    );
}

#[test]
fn quic_request_snapshot_publishes_configured_product_window() {
    let path_instance = instance(UnderlayProtocol::Udp, 0, 36);
    let mut path = observed_path(path_instance, 180.0, 2_000_000.0);
    let native_window = 2 * 1024 * 1024;
    let shared = path.shared_snapshot.as_mut().expect("shared snapshot");
    shared.carrier_inflight_limit_bytes = native_window;
    shared.app_limited = true;
    shared.has_durable_product_progress = true;
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
        reliable_bulk_product_windows(MuxLimits::default()).per_output_product_limit_bytes,
    );
    assert_eq!(snapshot.delivery_rate_bps, 2_000_000.0);
}

#[test]
fn expired_request_product_rate_and_shared_sibling_rate_cannot_become_local_authority() {
    let path_instance = instance(UnderlayProtocol::Udp, 0, 360);
    let mut path = observed_path(path_instance, 100.0, 500_000_000.0);
    let native_window = 1024 * 1024;
    let shared = path.shared_snapshot.as_mut().expect("shared snapshot");
    shared.carrier_inflight_limit_bytes = native_window;
    shared.carrier_delivery_rate_bps = None;
    shared.product_progress_rate_bps = Some(500_000_000.0);
    shared.has_durable_product_progress = true;
    shared.rate_scope = crate::scheduler::PathRateScope::PerFlowGoodput;
    let mut startup = shared.to_owned();
    startup.delivery_rate_bps = 5_000_000.0;
    startup.pacing_rate_bps = 5_000_000.0;
    startup.product_progress_rate_bps = None;
    startup.has_durable_product_progress = false;
    startup.rate_scope = crate::scheduler::PathRateScope::PathCapacity;
    path.startup_snapshot = Some(startup);
    let mut observation = scheduling_observation([path]);
    let authority_at = Instant::now();
    observation.observed_at = authority_at;
    let expired = RequestProductRateEpoch::new(
        500_000_000.0,
        10,
        authority_at - std::time::Duration::from_secs(2),
        std::time::Duration::from_secs(1),
    )
    .expect("expired diagnostic epoch");
    let evidence = RequestEvidence::default().with_product_rate_epoch(path_instance, expired);

    let projected = relay_path_snapshot_for_bulk_choice(
        &observation,
        path_instance,
        Some(path_instance.key),
        Some(evidence.state()),
        true,
    )
    .expect("request-local authority snapshot");
    assert!(!projected.has_durable_product_progress);
    assert_eq!(projected.product_progress_rate_bps, None);
    assert_eq!(projected.delivery_rate_bps, 5_000_000.0);
    assert_eq!(
        projected.rate_scope,
        crate::scheduler::PathRateScope::PathCapacity
    );
    assert_eq!(
        projected.data_level_limit_bytes,
        reliable_bulk_product_windows(MuxLimits::default()).per_output_product_limit_bytes,
        "configured Product authority is independent of expired local R and another stream's shared R",
    );
}

#[test]
fn immature_local_product_cannot_label_restored_startup_as_fresh_completion() {
    let path_instance = instance(UnderlayProtocol::Tcp, 0, 362);
    let mut path = observed_path(path_instance, 100.0, 900_000_000.0);
    path.has_bulk_model_evidence = true;
    path.has_fresh_native_carrier_rate_evidence = false;
    let shared = path.shared_snapshot.as_mut().expect("shared snapshot");
    shared.carrier_delivery_rate_bps = None;
    shared.product_progress_rate_bps = Some(900_000_000.0);
    shared.has_durable_product_progress = true;
    shared.rate_scope = crate::scheduler::PathRateScope::PerFlowGoodput;
    let mut startup = *shared;
    startup.delivery_rate_bps = 25_000_000.0;
    startup.pacing_rate_bps = 25_000_000.0;
    startup.product_progress_rate_bps = None;
    startup.has_durable_product_progress = false;
    startup.rate_scope = crate::scheduler::PathRateScope::PathCapacity;
    path.startup_snapshot = Some(startup);
    path.tcp.as_mut().expect("TCP evidence").startup_snapshot = startup;
    let observation = scheduling_observation([path]);
    let local = RequestEvidence::default().with_fresh_product_rate(
        path_instance,
        800_000_000.0,
        RELIABLE_INITIAL_WINDOW_PACKETS as u32 - 1,
    );

    let projected = relay_path_snapshot_for_bulk_choice(
        &observation,
        path_instance,
        Some(path_instance.key),
        Some(local.state()),
        true,
    )
    .expect("request-local authority snapshot");
    let path = observation
        .path_by_instance(path_instance)
        .expect("exact path observation");
    assert_eq!(projected.delivery_rate_bps, 25_000_000.0);
    assert_eq!(
        projected.rate_scope,
        crate::scheduler::PathRateScope::PathCapacity
    );
    assert!(!request_snapshot_has_fresh_completion_rate(
        path,
        projected,
        Some(local.state()),
        observation.observed_at,
    ));
}

#[test]
fn request_product_limit_is_independent_of_native_window_and_rate_evidence() {
    let path_instance = instance(UnderlayProtocol::Udp, 0, 361);
    let product_at = Instant::now();
    let mut path = observed_path(path_instance, 100.0, 500_000_000.0);
    let shared = path.shared_snapshot.as_mut().expect("shared snapshot");
    shared.carrier_inflight_limit_bytes = 1024 * 1024;
    shared.carrier_delivery_rate_bps = None;
    let mut observation = scheduling_observation([path]);
    observation.observed_at = product_at;
    let evidence = RequestEvidence::default().with_product_rate_epoch(
        path_instance,
        RequestProductRateEpoch::new(
            500_000_000.0,
            10,
            product_at,
            std::time::Duration::from_secs(1),
        )
        .expect("fresh Product epoch"),
    );

    let newer_product = relay_path_snapshot_for_bulk_choice(
        &observation,
        path_instance,
        Some(path_instance.key),
        Some(evidence.state()),
        true,
    )
    .expect("request-local Product authority");
    let product_limit =
        reliable_bulk_product_windows(MuxLimits::default()).per_output_product_limit_bytes;
    assert_eq!(
        newer_product.data_level_limit_bytes, product_limit,
        "native C cannot clamp configured Product authority",
    );

    path.shared_snapshot
        .as_mut()
        .expect("shared snapshot")
        .carrier_delivery_rate_bps = Some(500_000_000.0);
    let mut coherent_observation = scheduling_observation([path]);
    coherent_observation.observed_at = product_at;
    let coherent = relay_path_snapshot_for_bulk_choice(
        &coherent_observation,
        path_instance,
        Some(path_instance.key),
        Some(evidence.state()),
        true,
    )
    .expect("coherent request authority");
    assert_eq!(
        coherent.data_level_limit_bytes, product_limit,
        "adding native R does not rewrite configured Product authority",
    );
}

#[test]
fn tcp_request_snapshot_publishes_configured_product_window() {
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
        reliable_bulk_product_windows(MuxLimits::default()).per_output_product_limit_bytes,
        "TCP Product authority is configured independently of native socket backpressure",
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
fn tcp_acquisition_awaiting_one_outputs_ack_does_not_serialize_an_independent_output() {
    let reference = instance(UnderlayProtocol::Udp, 0, 43);
    let awaiting_ack = instance(UnderlayProtocol::Tcp, 0, 44);
    let independent = instance(UnderlayProtocol::Tcp, 1, 45);
    let reference_path = observed_path(reference, 100.0, 20_000_000.0);
    let mut awaiting_path = observed_path(awaiting_ack, 80.0, 200_000_000.0);
    awaiting_path.has_bulk_model_evidence = false;
    let mut independent_path = observed_path(independent, 5.0, 800_000_000.0);
    independent_path.has_bulk_model_evidence = false;
    let observation = scheduling_observation([reference_path, awaiting_path, independent_path]);
    let flights = original_flights(reference);
    let target = reliable_request_ack_clock_measurement_target_bytes(MuxLimits::default());
    let evidence = RequestEvidence::default()
        .prove_rate([reference])
        .own_ack_clock_candidate(awaiting_ack, target, target);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(independent),
        "A's fully committed Product generation may await its exact Data ACK, but it cannot own a direction-global acquisition token that blocks B's independent E-bounded Product acquisition",
    );
}

#[test]
fn pending_tcp_acquisition_skips_a_blocked_candidate_without_fencing_an_independent_regular_output()
{
    let reference = instance(UnderlayProtocol::Udp, 0, 46);
    let blocked = instance(UnderlayProtocol::Tcp, 0, 47);
    let independent = instance(UnderlayProtocol::Tcp, 1, 48);
    let reference_path = observed_path(reference, 100.0, 20_000_000.0);
    let mut blocked_path = observed_path(blocked, 80.0, 200_000_000.0);
    blocked_path.has_bulk_model_evidence = false;
    blocked_path.can_enqueue_frame = false;
    let mut independent_path = observed_path(independent, 5.0, 800_000_000.0);
    independent_path.has_bulk_model_evidence = false;
    let observation = scheduling_observation([reference_path, blocked_path, independent_path]);
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default()
        .prove_rate([reference])
        .pending_measurement(reference, blocked)
        .admit_ack_clock_candidate(independent);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(independent),
        "a transiently blocked advisory candidate is skipped for this acquisition round; its legacy Pending state cannot hold a direction-global fence over an independent regular output",
    );
}

#[test]
fn blocked_regular_membership_prevents_backup_product_acquisition() {
    let reference = instance(UnderlayProtocol::Udp, 0, 49);
    let blocked_regular = instance(UnderlayProtocol::Tcp, 0, 50);
    let backup = instance(UnderlayProtocol::Tcp, 1, 51);
    let reference_path = observed_path(reference, 20.0, 500_000_000.0);
    let mut blocked_regular_path = observed_path(blocked_regular, 10.0, 800_000_000.0);
    blocked_regular_path.can_enqueue_frame = false;
    let mut backup_path = observed_path(backup, 1.0, 1_000_000_000.0);
    backup_path
        .shared_snapshot
        .as_mut()
        .expect("backup shared snapshot")
        .policy
        .backup = true;
    backup_path
        .startup_snapshot
        .as_mut()
        .expect("backup startup snapshot")
        .policy
        .backup = true;
    let observation = scheduling_observation([reference_path, blocked_regular_path, backup_path]);
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default()
        .prove_rate([reference])
        .admit_ack_clock_candidate(blocked_regular)
        .admit_ack_clock_candidate(backup);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(reference),
        "transient writer blockage does not remove a regular output from the selected acquisition tier, so an unqualified backup cannot acquire while regular membership exists",
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
fn sub_sample_quic_product_progress_retains_bounded_acquisition_after_proof_expiry() {
    let reference = instance(UnderlayProtocol::Udp, 0, 154);
    let candidate = instance(UnderlayProtocol::Udp, 1, 155);
    let reference_path = observed_path(reference, 100.0, 20_000_000.0);
    let mut candidate_path = observed_path(candidate, 5.0, 800_000_000.0);
    candidate_path.has_bulk_model_evidence = false;
    candidate_path.fresh_proof = None;
    candidate_path
        .shared_snapshot
        .as_mut()
        .expect("candidate snapshot")
        .app_limited = true;
    let observation = scheduling_observation([reference_path, candidate_path]);
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default()
        .prove_rate([reference])
        .prove_quic_product_progress_only(candidate);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(candidate),
        "exact unique Product progress keeps an unqualified QUIC path eligible for the existing native-credit acquisition window after attachment proof expires",
    );
}

#[test]
fn quic_product_progress_without_a_further_ack_stops_at_native_credit() {
    let reference = instance(UnderlayProtocol::Udp, 0, 156);
    let candidate = instance(UnderlayProtocol::Udp, 1, 157);
    let reference_path = observed_path(reference, 100.0, 20_000_000.0);
    let mut candidate_path = observed_path(candidate, 5.0, 800_000_000.0);
    candidate_path.has_bulk_model_evidence = false;
    candidate_path.fresh_proof = None;
    let candidate_snapshot = candidate_path
        .shared_snapshot
        .as_mut()
        .expect("candidate snapshot");
    candidate_snapshot.app_limited = true;
    candidate_snapshot.data_level_bytes_in_flight = candidate_snapshot
        .carrier_inflight_limit_bytes
        .saturating_add(reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default()) as u64);
    let observation = scheduling_observation([reference_path, candidate_path]);
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default()
        .prove_rate([reference])
        .prove_quic_product_progress_only(candidate);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(reference),
        "one exact ACK is not unbounded rate authority: without another ACK, Product credit exhausts after the native window plus its one feed quantum",
    );
}

#[test]
fn slower_sub_sample_quic_progress_does_not_preempt_the_qualified_lead() {
    let reference = instance(UnderlayProtocol::Udp, 0, 158);
    let candidate = instance(UnderlayProtocol::Udp, 1, 159);
    let reference_path = observed_path(reference, 20.0, 500_000_000.0);
    let mut candidate_path = observed_path(candidate, 800.0, 351_000.0);
    candidate_path.has_bulk_model_evidence = false;
    candidate_path.fresh_proof = None;
    candidate_path
        .shared_snapshot
        .as_mut()
        .expect("candidate snapshot")
        .app_limited = true;
    let observation = scheduling_observation([reference_path, candidate_path]);
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default()
        .prove_rate([reference])
        .prove_quic_product_progress_only(candidate);

    assert_eq!(
        choose_bulk(&observation, &flights, Some(&evidence)),
        BulkRelayPathChoice::Selected(reference),
        "bounded acquisition eligibility must not change qualified completion ordering",
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
        "a product-window-full DSN owner remains the completion reference while another structurally admitted path proceeds",
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
        choose_measurement_annotation(&observation, &flights, &evidence),
        Some(BulkRelayPathChoice::SelectedAckClockMeasurement {
            candidate,
            ..
        }) if candidate == tcp_candidate
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
        choose_measurement_annotation(&without_native_capacity, &flights, &evidence),
        Some(BulkRelayPathChoice::SelectedAckClockMeasurement {
            candidate: selected,
            ..
        }) if selected == candidate
    ));

    let mut native_candidate_path = candidate_path;
    native_candidate_path.has_fresh_native_carrier_rate_evidence = true;
    let with_native_capacity = scheduling_observation([reference_path, native_candidate_path]);
    assert!(
        choose_measurement_annotation(&with_native_capacity, &flights, &evidence).is_none(),
        "native TCP delivery evidence must suppress redundant Product calibration annotation",
    );
}

#[test]
fn unmeasured_tcp_startup_prior_cannot_suppress_optional_measurement() {
    let reference = instance(UnderlayProtocol::Udp, 0, 164);
    let candidate = instance(UnderlayProtocol::Tcp, 0, 165);
    let reference_path = observed_path(reference, 20.0, 500_000_000.0);
    let candidate_path = observed_path(candidate, 800.0, 351_000.0);
    let observation = scheduling_observation([reference_path, candidate_path]);
    let flights = original_flights(reference);
    let target = reliable_request_ack_clock_measurement_target_bytes(MuxLimits::default());

    let entry = RequestEvidence::default()
        .prove_rate([reference])
        .with_fresh_product_rate(
            reference,
            500_000_000.0,
            RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        )
        .admit_ack_clock_candidate(candidate);
    let entry_choice = choose_measurement_annotation(&observation, &flights, &entry);
    let entry_preempts = matches!(
        entry_choice,
        Some(BulkRelayPathChoice::SelectedAckClockMeasurement {
            candidate: selected,
            target_bytes,
            ..
        }) if selected == candidate && target_bytes == target
    );

    // Prove that this is a cumulative transaction rather than a one-quantum
    // sample: even the final quantum remains forced onto the same slow TCP
    // owner after almost the complete target has already been committed.
    let nearly_spent = target.saturating_sub(PAYLOAD_BYTES as u64);
    let continuing = RequestEvidence::default()
        .prove_rate([reference])
        .with_fresh_product_rate(
            reference,
            500_000_000.0,
            RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        )
        .own_ack_clock_candidate(candidate, target, nearly_spent);
    let continuation_preempts = matches!(
        choose_measurement_annotation(&observation, &flights, &continuing),
        Some(BulkRelayPathChoice::SelectedAckClockMeasurement {
            candidate: selected,
            target_bytes,
            ..
        }) if selected == candidate && target_bytes == target
    );

    assert!(
        entry_preempts,
        "an unmeasured TCP startup prior is not achieved completion evidence and cannot suppress optional ACK-clock measurement",
    );
    assert!(
        continuation_preempts,
        "once admitted, the exact ACK-clock owner must remain stable through its final quantum",
    );
}

#[test]
fn begun_tcp_ack_clock_transaction_keeps_its_exact_owner_across_later_rate_change() {
    let reference = instance(UnderlayProtocol::Udp, 0, 166);
    let candidate = instance(UnderlayProtocol::Tcp, 0, 167);
    let reference_path = observed_path(reference, 20.0, 500_000_000.0);
    let candidate_path = observed_path(candidate, 800.0, 351_000.0);
    let observation = scheduling_observation([reference_path, candidate_path]);
    let flights = original_flights(reference);
    let target = reliable_request_ack_clock_measurement_target_bytes(MuxLimits::default());
    let begun = RequestEvidence::default()
        .prove_rate([reference])
        .own_ack_clock_candidate(candidate, target, PAYLOAD_BYTES as u64);

    assert!(matches!(
        choose_measurement_annotation(&observation, &flights, &begun),
        Some(BulkRelayPathChoice::SelectedAckClockMeasurement {
            candidate: selected,
            target_bytes,
            ..
        }) if selected == candidate && target_bytes == target
    ));
}

#[test]
fn tcp_ack_clock_acquisition_remains_fallback_when_reference_cannot_enqueue() {
    let reference = instance(UnderlayProtocol::Udp, 0, 168);
    let candidate = instance(UnderlayProtocol::Tcp, 0, 169);
    let mut reference_path = observed_path(reference, 20.0, 500_000_000.0);
    reference_path.can_enqueue_frame = false;
    let candidate_path = observed_path(candidate, 800.0, 351_000.0);
    let observation = scheduling_observation([reference_path, candidate_path]);
    let flights = original_flights(reference);
    let evidence = RequestEvidence::default()
        .prove_rate([reference])
        .admit_ack_clock_candidate(candidate);

    assert!(matches!(
        choose_measurement_annotation(&observation, &flights, &evidence),
        Some(BulkRelayPathChoice::SelectedAckClockMeasurement {
            candidate: selected,
            ..
        }) if selected == candidate
    ));
}

#[test]
fn equal_or_faster_tcp_capacity_can_start_ack_clock_acquisition() {
    for (ordinal, candidate_srtt_ms, candidate_rate_bps) in
        [(0_u64, 20.0, 500_000_000.0), (1, 10.0, 1_000_000_000.0)]
    {
        let reference = instance(UnderlayProtocol::Udp, 0, 170 + ordinal * 2);
        let candidate = instance(UnderlayProtocol::Tcp, 0, 171 + ordinal * 2);
        let reference_path = observed_path(reference, 20.0, 500_000_000.0);
        let candidate_path = observed_path(candidate, candidate_srtt_ms, candidate_rate_bps);
        let observation = scheduling_observation([reference_path, candidate_path]);
        let flights = original_flights(reference);
        let evidence = RequestEvidence::default()
            .prove_rate([reference])
            .admit_ack_clock_candidate(candidate);

        assert!(matches!(
            choose_measurement_annotation(&observation, &flights, &evidence),
            Some(BulkRelayPathChoice::SelectedAckClockMeasurement {
                candidate: selected,
                ..
            }) if selected == candidate
        ));
    }
}

#[test]
fn pending_ack_clock_metadata_has_no_acquisition_or_placement_authority() {
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
        None,
    );
    assert_eq!(
        choose_bulk(&observation, &flights, Some(&exact)),
        BulkRelayPathChoice::Selected(reference),
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
