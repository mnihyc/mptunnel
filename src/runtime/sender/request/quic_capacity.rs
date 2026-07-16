//! Request QUIC capacity controller.
//!
//! QUIC owns packet-ACK probe publication and its bounded product-admission lifetime.
//! Product capacity_admission remains an explicit event applied by the request owner.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::path::RelayPathInstance;
#[cfg(feature = "lab-diagnostics")]
use crate::model::request_capacity::request_quic_capacity_slow_start_rounds;
use crate::model::request_capacity::{
    request_capacity_stable_candidate_share_bytes, request_quic_capacity_measurement_geometry,
    request_quic_capacity_measurement_lease,
};
use crate::model::request_evidence::RequestPerFlowRateModel;
use crate::model::timing::quic_bulk_proof_freshness_horizon;
use crate::model::work::ReliableWorkClass;
use crate::protocol::{PathId, StreamId, UnderlayProtocol};
use crate::runtime::path::{
    CapacityProbeCommandTicket, ClientPathContext, QuicCapacityProbeCommand,
    RequestCapacityProbeCampaignBudget, RequestCapacityReconciliationView,
    RequestQuicCapacityProbeLease, RequestQuicCapacityProductAdmissionState,
    RequestQuicCapacityReconciliationQuery,
};
use crate::runtime::stream::ReliableRelayRemoteSet;
use crate::runtime::stream::request::RequestStreamState;
use crate::scheduler::TrafficClass;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub(super) struct RequestQuicCapacityMeasurement {
    pub(super) target: RelayPathInstance,
    pub(super) token: u64,
    pub(super) publication_expires_at: Instant,
    pub(super) capacity_admitted: bool,
    pub(super) ticket: CapacityProbeCommandTicket,
    pub(super) _lease: RequestQuicCapacityProbeLease,
}

impl Drop for RequestQuicCapacityMeasurement {
    fn drop(&mut self) {
        self.ticket.cancel();
    }
}

#[derive(Debug)]
pub(super) enum RequestQuicCapacityEvent {
    CarrierProofAccepted {
        target: RelayPathInstance,
        token: u64,
    },
    ProductAdmissionCommitted {
        target: RelayPathInstance,
        measurement: RequestQuicCapacityMeasurement,
    },
    ProductAdmissionExpired {
        target: RelayPathInstance,
        measurement: RequestQuicCapacityMeasurement,
    },
}

#[derive(Debug)]
pub(super) struct RequestQuicCapacityController {
    pub(super) active: Option<RequestQuicCapacityMeasurement>,
    pub(super) attempted_paths: HashSet<usize>,
    pub(super) campaign: Arc<RequestCapacityProbeCampaignBudget>,
}

impl Default for RequestQuicCapacityController {
    fn default() -> Self {
        Self {
            active: None,
            attempted_paths: HashSet::new(),
            campaign: Arc::new(RequestCapacityProbeCampaignBudget::default()),
        }
    }
}

impl RequestQuicCapacityController {
    pub(super) fn reconciliation_query(&self) -> Option<RequestQuicCapacityReconciliationQuery> {
        self.active
            .as_ref()
            .map(|measurement| RequestQuicCapacityReconciliationQuery {
                target: measurement.target,
                token: measurement.token,
            })
    }

    /// Native QUIC packet-ACK evidence is carrier admission evidence; the
    /// request owner applies the returned exact-instance capacity_admissions.
    pub(super) fn native_evidence_targets<'a>(
        &'a self,
        context: &'a ClientPathContext,
        remotes: &'a ReliableRelayRemoteSet,
        now: Instant,
    ) -> impl Iterator<Item = RelayPathInstance> + 'a {
        remotes.paths.iter().filter_map(move |path| {
            let instance = path.instance();
            let admissible = instance.key.underlay == UnderlayProtocol::Udp
                && path.path_proof_id.is_some_and(|proof_id| {
                    context.relay_path_has_fresh_proof_as_of(
                        instance.key.underlay,
                        instance.key.index,
                        proof_id,
                        path.attached_at,
                        now,
                    )
                })
                && context.relay_path_has_native_bulk_model_evidence_as_of(
                    instance.key.underlay,
                    instance.key.index,
                    path.attached_at,
                    now,
                );
            admissible.then_some(instance)
        })
    }

    pub(super) fn reconcile(
        &mut self,
        context: &ClientPathContext,
        view: &RequestCapacityReconciliationView,
        remotes: &ReliableRelayRemoteSet,
    ) -> Vec<RequestQuicCapacityEvent> {
        let now = view.observed_at();
        let mut events = Vec::new();
        if self
            .active
            .as_ref()
            .is_some_and(|measurement| !remotes.contains_path_instance(measurement.target))
        {
            let measurement = self
                .active
                .take()
                .expect("detached QUIC measurement was just observed");
            context.retire_request_quic_capacity_probe_token(measurement.token);
            drop(measurement);
            return events;
        }

        // Deadline-edge proof acceptance precedes publication expiry.
        let accepted = self
            .active
            .as_ref()
            .filter(|measurement| !measurement.capacity_admitted)
            .filter(|measurement| view.quic_carrier_proven(measurement.target, measurement.token))
            .map(|measurement| (measurement.target, measurement.token));
        if let Some((target, token)) = accepted {
            if let Some(measurement) = self.active.as_mut() {
                measurement.capacity_admitted = true;
            }
            events.push(RequestQuicCapacityEvent::CarrierProofAccepted { target, token });
        }

        if self.active.as_ref().is_some_and(|measurement| {
            !measurement.capacity_admitted && now >= measurement.publication_expires_at
        }) {
            let measurement = self
                .active
                .take()
                .expect("expired QUIC measurement was just observed");
            context.retire_request_quic_capacity_probe_token(measurement.token);
            drop(measurement);
            return events;
        }

        let product_admission = self
            .active
            .as_ref()
            .filter(|measurement| measurement.capacity_admitted)
            .map(|measurement| {
                (
                    measurement.target,
                    measurement.token,
                    view.quic_product_admission_state(measurement.target, measurement.token),
                )
            });
        let Some((target, token, state)) = product_admission else {
            return events;
        };
        match state {
            RequestQuicCapacityProductAdmissionState::Pending => events,
            RequestQuicCapacityProductAdmissionState::Complete => {
                context.retire_request_quic_capacity_probe_token(token);
                let measurement = self
                    .active
                    .take()
                    .expect("observed QUIC measurement remains serialized");
                debug_assert_eq!(measurement.token, token);
                events.push(RequestQuicCapacityEvent::ProductAdmissionCommitted {
                    target,
                    measurement,
                });
                events
            }
            RequestQuicCapacityProductAdmissionState::Absent => {
                context.retire_request_quic_capacity_probe_token(token);
                let measurement = self
                    .active
                    .take()
                    .expect("observed QUIC measurement remains serialized");
                debug_assert_eq!(measurement.token, token);
                events.push(RequestQuicCapacityEvent::ProductAdmissionExpired {
                    target,
                    measurement,
                });
                events
            }
        }
    }
}

impl RequestQuicCapacityController {
    pub(super) fn try_start(
        &mut self,
        stream_id: StreamId,
        request: &RequestStreamState,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: TrafficClass,
        reference: Option<(RelayPathInstance, RequestPerFlowRateModel)>,
    ) {
        if !lane.is_bulk() || self.active.is_some() {
            return;
        }
        let has_unattempted_udp_candidate = remotes.paths.iter().any(|path| {
            let instance = path.instance();
            instance.key.underlay == UnderlayProtocol::Udp
                && context.relay_path_allows_automatic_bulk_use(instance.key)
                && !self.attempted_paths.contains(&instance.key.index)
                && !request
                    .path_states
                    .get(instance)
                    .is_some_and(|state| state.capacity_admitted())
        });
        if !has_unattempted_udp_candidate || context.reliable_relay_has_latency_pressure() {
            // Bulk sends call this repeatedly. Topology can reject the common
            // single-path/completed case without touching session health.
            return;
        }
        let Some((reference, reference_model)) = reference else {
            return;
        };
        let Some(reference_snapshot) = context.reliable_path_snapshot(reference.key) else {
            return;
        };
        if reference_snapshot.active_latency_sensitive_flows > 0
            || reference_snapshot.session_active_latency_sensitive_flows > 0
        {
            return;
        }
        // QUIC keeps its native packet-ACK proof transaction, but shares TCP's
        // topology-stable budget policy. Attempt order cannot enlarge a train.
        let excluded_reference =
            (reference.key.underlay == UnderlayProtocol::Udp).then_some(reference.key.index);
        let eligible_candidates =
            context.automatic_bulk_path_count(UnderlayProtocol::Udp, excluded_reference);
        let proposed_candidate_share =
            request_capacity_stable_candidate_share_bytes(context.mux_limits, eligible_candidates);
        let stable_candidate_share =
            context.request_quic_capacity_probe_candidate_share_bytes(proposed_candidate_share);
        let session_remaining_bytes = context.request_quic_capacity_probe_remaining_bytes();
        let Some((path, snapshot, geometry, _train_envelope_bytes)) = remotes
            .paths
            .iter()
            .filter(|path| {
                let instance = path.instance();
                let snapshot = context.reliable_path_snapshot(instance.key);
                instance.key != reference.key
                    && instance.key.underlay == UnderlayProtocol::Udp
                    && context.relay_path_allows_automatic_bulk_use(instance.key)
                    && !self.attempted_paths.contains(&instance.key.index)
                    && !request
                        .path_states
                        .get(instance)
                        .is_some_and(|state| state.capacity_admitted())
                    && path.path_proof_id.is_some_and(|proof_id| {
                        context.relay_path_has_fresh_proof(
                            instance.key.underlay,
                            instance.key.index,
                            proof_id,
                            path.attached_at,
                        )
                    })
                    && !context.relay_path_has_native_bulk_model_evidence_since(
                        instance.key.underlay,
                        instance.key.index,
                        path.attached_at,
                    )
                    && snapshot.is_some_and(|snapshot| {
                        snapshot.bytes_in_flight == 0
                            && snapshot.queue_bytes == 0
                            && snapshot.data_level_bytes_in_flight == 0
                            && snapshot.data_level_queue_bytes == 0
                    })
                    && path
                        .stream
                        .can_enqueue_work_lane_now(ReliableWorkClass::Data, lane)
            })
            .filter_map(|path| {
                let snapshot = context.reliable_path_snapshot(path.key())?;
                let campaign_remaining_bytes =
                    self.campaign.remaining_bytes(stable_candidate_share);
                let train_envelope_bytes = session_remaining_bytes
                    .min(campaign_remaining_bytes)
                    .min(context.request_quic_capacity_probe_path_remaining_bytes(
                        path.key().index,
                        stable_candidate_share,
                    ));
                let geometry = request_quic_capacity_measurement_geometry(
                    snapshot,
                    reference_model.rate_bps,
                    context.mux_limits,
                    train_envelope_bytes,
                )?;
                Some((path, snapshot, geometry, train_envelope_bytes))
            })
            .min_by_key(|(path, _, _, _)| context.relay_path_config_ordinal(path.key()))
        else {
            return;
        };
        let train_payload_bytes = geometry.train_bytes;
        static NEXT_REQUEST_QUIC_CAPACITY_ID: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        let token =
            NEXT_REQUEST_QUIC_CAPACITY_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let ticket = CapacityProbeCommandTicket::new();
        let lease_duration = request_quic_capacity_measurement_lease(snapshot, train_payload_bytes);
        let Some(expires_at) = Instant::now().checked_add(lease_duration) else {
            return;
        };
        let proof_validity = quic_bulk_proof_freshness_horizon(
            Duration::from_secs_f64(snapshot.srtt_ms.max(1.0) / 1_000.0),
            Duration::from_secs_f64(snapshot.jitter_ms.max(1.0) / 1_000.0),
        );
        let Some(publication_expires_at) = expires_at.checked_add(proof_validity) else {
            return;
        };
        let instance = path.instance();
        let Some(mut lease) = context.try_reserve_request_quic_capacity_probe(
            stream_id,
            instance.key.index,
            instance,
            token,
            train_payload_bytes,
            stable_candidate_share,
            self.campaign.clone(),
            path.attached_at,
            expires_at,
            proof_validity,
            ticket.clone(),
        ) else {
            return;
        };
        let probe = QuicCapacityProbeCommand {
            stream_id,
            path_instance: instance,
            path_id: PathId(instance.key.index as u16),
            measurement_id: token,
            train_payload_bytes,
            sample_floor_bytes: geometry.sample_floor_bytes,
            warmup_carrier_bytes: geometry.warmup_carrier_bytes,
            required_timed_carrier_bytes: geometry.required_timed_carrier_bytes,
            proof_validity,
            expires_at,
            ticket: ticket.clone(),
            cancel_on_drop: true,
        };
        if path
            .stream
            .try_enqueue_request_quic_capacity_probe(probe)
            .is_err()
        {
            return;
        }
        lease.commit();
        self.attempted_paths.insert(instance.key.index);
        self.active = Some(RequestQuicCapacityMeasurement {
            target: instance,
            token,
            publication_expires_at,
            capacity_admitted: false,
            ticket,
            _lease: lease,
        });
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "request_quic_capacity_measurement",
            format_args!(
                "phase=started stream_id={} path_index={} instance_id={} measurement_id={} train_bytes={} stable_candidate_share_bytes={} train_envelope_bytes={} sample_floor_bytes={} accounting_slack_bytes={} timing_slack_bytes={} desired_warmup_bytes={} warmup_bytes={} required_proof_bytes={} candidate_carrier_flight_bytes={} reference_rate_bps={} reference_rate_scope={:?} slow_start_rounds={} lease_ms={}",
                stream_id.0,
                instance.key.index,
                instance.attachment_id,
                token,
                train_payload_bytes,
                stable_candidate_share,
                _train_envelope_bytes,
                geometry.sample_floor_bytes,
                geometry.accounting_slack_bytes,
                geometry.timing_slack_bytes,
                geometry.desired_warmup_carrier_bytes,
                geometry.warmup_carrier_bytes,
                geometry.required_timed_carrier_bytes,
                geometry.candidate_carrier_flight_bytes,
                geometry.reference_rate_bps,
                reference_snapshot.rate_scope,
                request_quic_capacity_slow_start_rounds(train_payload_bytes),
                lease_duration.as_millis(),
            ),
        );
    }
}

#[cfg(test)]
#[path = "quic_capacity_test.rs"]
mod tests;
