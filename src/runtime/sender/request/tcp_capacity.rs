//! Request TCP capacity controller.
//!
//! TCP owns receipt-probe leases and carrier-proof lifetime. It reports typed
//! outcomes upward because only the request product owner may graduate a
//! path_state or preserve a sealed ACK-clock transaction.

#[cfg(all(test, feature = "lab-diagnostics"))]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::TcpCapacityProofCandidate;
use crate::model::path::RelayPathInstance;
#[cfg(test)]
use crate::model::request_capacity::{
    request_capacity_stable_candidate_share_bytes,
    request_tcp_capacity_candidate_can_start_receipt, request_tcp_capacity_measurement_geometry,
    request_tcp_capacity_measurement_lease,
};
#[cfg(test)]
use crate::model::request_evidence::RequestProductRateEpoch;
#[cfg(test)]
use crate::model::timing::transport_pto_from_snapshot;
#[cfg(test)]
use crate::model::work::ReliableWorkClass;
#[cfg(test)]
use crate::protocol::{StreamId, UnderlayProtocol};
#[cfg(test)]
use crate::runtime::path::{
    CapacityProbeCommandTicket, ClientPathContext, RequestCapacityProbeCampaignBudget,
    RequestTcpCapacityProbeRequest,
};
use crate::runtime::path::{
    RequestCapacityReconciliationView, RequestTcpCapacityProbeLease, RequestTcpCapacityProofQuery,
};
use crate::runtime::stream::ReliableRelayRemoteSet;
#[cfg(test)]
use crate::runtime::stream::request::RequestStreamState;
#[cfg(test)]
use crate::scheduler::TrafficClass;
use std::collections::{HashMap, HashSet};
#[cfg(test)]
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
pub(super) struct RequestTcpCapacityMeasurement {
    pub(super) token: u64,
    pub(super) publication_expires_at: Instant,
    pub(super) proof_expires_at: Option<Instant>,
    pub(super) capacity_admitted: bool,
    pub(super) lease: RequestTcpCapacityProbeLease,
}

impl Drop for RequestTcpCapacityMeasurement {
    fn drop(&mut self) {
        self.lease.cancel();
    }
}

pub(super) fn request_tcp_carrier_authority_expired_naturally(
    published: bool,
    proof_expires_at: Option<Instant>,
    now: Instant,
) -> bool {
    published && proof_expires_at.is_some_and(|expires_at| now >= expires_at)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RequestTcpCapacityRetirement {
    Detached,
    PublicationExpired,
    AuthorityExpired,
    AuthorityLost,
}

#[derive(Debug)]
pub(super) enum RequestTcpCapacityEvent {
    CarrierProofAccepted {
        target: RelayPathInstance,
        token: u64,
        proof: TcpCapacityProofCandidate,
    },
    ProductAdmissionCommitted {
        target: RelayPathInstance,
        measurement: RequestTcpCapacityMeasurement,
    },
    CarrierAuthorityRetired {
        target: RelayPathInstance,
        measurement: RequestTcpCapacityMeasurement,
        cause: RequestTcpCapacityRetirement,
    },
}

#[derive(Debug)]
pub(super) struct RequestTcpCapacityController {
    pub(super) measurements: HashMap<RelayPathInstance, RequestTcpCapacityMeasurement>,
    #[cfg(test)]
    pub(super) attempted_paths: HashSet<usize>,
    #[cfg(test)]
    pub(super) campaign: Arc<RequestCapacityProbeCampaignBudget>,
}

impl Default for RequestTcpCapacityController {
    fn default() -> Self {
        Self {
            measurements: HashMap::new(),
            #[cfg(test)]
            attempted_paths: HashSet::new(),
            #[cfg(test)]
            campaign: Arc::new(RequestCapacityProbeCampaignBudget::default()),
        }
    }
}

impl RequestTcpCapacityController {
    pub(super) fn remove(&mut self, target: RelayPathInstance) {
        self.measurements.remove(&target);
    }

    pub(super) fn proof_queries(&self) -> impl Iterator<Item = RequestTcpCapacityProofQuery> + '_ {
        self.measurements
            .iter()
            .map(|(target, measurement)| RequestTcpCapacityProofQuery {
                target: *target,
                token: measurement.token,
            })
    }

    pub(super) fn reconcile(
        &mut self,
        view: &RequestCapacityReconciliationView,
        remotes: &ReliableRelayRemoteSet,
        committed_product_admissions: &HashSet<RelayPathInstance>,
    ) -> Vec<RequestTcpCapacityEvent> {
        let now = view.observed_at();
        let mut events = Vec::new();
        let detached = self
            .measurements
            .keys()
            .copied()
            .filter(|target| !remotes.contains_path_instance(*target))
            .collect::<Vec<_>>();
        for target in detached {
            let measurement = self
                .measurements
                .remove(&target)
                .expect("detached TCP measurement collected from the same map");
            events.push(RequestTcpCapacityEvent::CarrierAuthorityRetired {
                target,
                measurement,
                cause: RequestTcpCapacityRetirement::Detached,
            });
        }

        let observations = self
            .measurements
            .iter()
            .map(|(target, measurement)| {
                let proof = view.tcp_proof(*target);
                (
                    *target,
                    measurement.token,
                    measurement.capacity_admitted,
                    measurement.publication_expires_at,
                    measurement.proof_expires_at,
                    measurement.lease.is_current(),
                    measurement.lease.is_published(),
                    proof,
                )
            })
            .collect::<Vec<_>>();

        for (
            target,
            token,
            capacity_admitted,
            publication_expires_at,
            proof_expires_at,
            current,
            published,
            proof,
        ) in observations
        {
            if committed_product_admissions.contains(&target) {
                let measurement = self
                    .measurements
                    .remove(&target)
                    .expect("observed TCP measurement remains serialized");
                events.push(RequestTcpCapacityEvent::ProductAdmissionCommitted {
                    target,
                    measurement,
                });
                continue;
            }
            if !capacity_admitted {
                if let Some(proof) = proof {
                    if let Some(measurement) = self.measurements.get_mut(&target) {
                        measurement.capacity_admitted = true;
                        measurement.proof_expires_at = Some(proof.expires_at);
                    }
                    events.push(RequestTcpCapacityEvent::CarrierProofAccepted {
                        target,
                        token,
                        proof,
                    });
                } else if now >= publication_expires_at || !current {
                    let measurement = self
                        .measurements
                        .remove(&target)
                        .expect("observed TCP measurement remains serialized");
                    events.push(RequestTcpCapacityEvent::CarrierAuthorityRetired {
                        target,
                        measurement,
                        cause: RequestTcpCapacityRetirement::PublicationExpired,
                    });
                }
                continue;
            }

            let authority_expired =
                request_tcp_carrier_authority_expired_naturally(published, proof_expires_at, now);
            if authority_expired {
                let measurement = self
                    .measurements
                    .remove(&target)
                    .expect("observed TCP measurement remains serialized");
                events.push(RequestTcpCapacityEvent::CarrierAuthorityRetired {
                    target,
                    measurement,
                    cause: RequestTcpCapacityRetirement::AuthorityExpired,
                });
            } else if proof.is_none() || !published {
                let measurement = self
                    .measurements
                    .remove(&target)
                    .expect("observed TCP measurement remains serialized");
                events.push(RequestTcpCapacityEvent::CarrierAuthorityRetired {
                    target,
                    measurement,
                    cause: RequestTcpCapacityRetirement::AuthorityLost,
                });
            }
        }
        events
    }
}

impl RequestTcpCapacityController {
    #[cfg(test)]
    pub(super) fn try_start(
        &mut self,
        stream_id: StreamId,
        request: &RequestStreamState,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: TrafficClass,
        reference: Option<(RelayPathInstance, RequestProductRateEpoch)>,
    ) {
        if !lane.is_bulk() || context.reliable_relay_has_latency_pressure() {
            return;
        }
        let Some((reference, reference_model)) = reference else {
            return;
        };
        let Some(reference_snapshot) = context.reliable_path_snapshot_for_instance(reference)
        else {
            return;
        };
        if reference_snapshot.active_latency_sensitive_flows > 0
            || reference_snapshot.session_active_latency_sensitive_flows > 0
        {
            return;
        }
        // The reference rate prices only train geometry. The candidate's full
        // receiver-confirmed train remains the sole cold startup-rate authority.
        // Freeze one fair share per configured eligible candidate. A late
        // path must not inherit the unused campaign budget of earlier paths.
        let excluded_reference =
            (reference.key.underlay == UnderlayProtocol::Tcp).then_some(reference.key.index);
        let eligible_candidates =
            context.automatic_bulk_path_count(UnderlayProtocol::Tcp, excluded_reference);
        let proposed_candidate_share =
            request_capacity_stable_candidate_share_bytes(context.mux_limits, eligible_candidates);
        let stable_candidate_share =
            context.request_tcp_capacity_probe_candidate_share_bytes(proposed_candidate_share);
        let session_remaining_bytes = context.request_tcp_capacity_probe_remaining_bytes();
        let mut candidates = remotes
            .paths
            .iter()
            .filter(|path| {
                let instance = path.instance();
                let snapshot = context.reliable_path_snapshot_for_instance(instance);
                instance.key != reference.key
                    && instance.key.underlay == UnderlayProtocol::Tcp
                    && context.relay_path_allows_automatic_bulk_use(instance.key)
                    && !self.attempted_paths.contains(&instance.key.index)
                    && !request.path_states.get(instance).is_some_and(|state| {
                        state.capacity_admitted() || state.has_product_evidence()
                    })
                    && path.path_proof_id.is_some_and(|proof_id| {
                        context.relay_path_has_fresh_proof(
                            instance.key.underlay,
                            instance.key.index,
                            proof_id,
                            path.attached_at,
                        )
                    })
                    && !context.relay_path_instance_has_bulk_model_evidence(instance)
                    && snapshot.is_some_and(request_tcp_capacity_candidate_can_start_receipt)
                    && path
                        .stream
                        .can_enqueue_work_lane_now(ReliableWorkClass::Data, lane)
            })
            .filter_map(|path| {
                let candidate_snapshot =
                    context.reliable_path_snapshot_for_instance(path.instance())?;
                let campaign_remaining_bytes =
                    self.campaign.remaining_bytes(stable_candidate_share);
                let train_envelope_bytes = session_remaining_bytes
                    .min(campaign_remaining_bytes)
                    .min(context.request_tcp_capacity_probe_path_remaining_bytes(
                        path.key().index,
                        stable_candidate_share,
                    ));
                let geometry = request_tcp_capacity_measurement_geometry(
                    candidate_snapshot,
                    reference_model,
                    context.mux_limits,
                    train_envelope_bytes,
                )?;
                Some((path, candidate_snapshot, geometry, train_envelope_bytes))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| context.relay_path_key_order(left.0.key(), right.0.key()));
        if candidates.is_empty() {
            return;
        }
        static NEXT_REQUEST_TCP_CAPACITY_ID: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        for (path, candidate_snapshot, geometry, _train_envelope_bytes) in candidates {
            let instance = path.instance();
            let train_payload_bytes = geometry.train_bytes;
            let token =
                NEXT_REQUEST_TCP_CAPACITY_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let ticket = CapacityProbeCommandTicket::new();
            let now = Instant::now();
            let baseline_budget = transport_pto_from_snapshot(Some(candidate_snapshot));
            let lease_duration = request_tcp_capacity_measurement_lease(
                candidate_snapshot,
                train_payload_bytes,
                geometry.reference_rate_bps,
            );
            let Some(baseline_expires_at) = now.checked_add(baseline_budget) else {
                continue;
            };
            let Some(expires_at) = now.checked_add(lease_duration) else {
                continue;
            };
            let Some(write_expires_at) = expires_at.checked_sub(baseline_budget) else {
                continue;
            };
            let Some(lease) = context.try_reserve_request_tcp_capacity_probe(
                stream_id,
                instance.key.index,
                instance,
                token,
                train_payload_bytes,
                stable_candidate_share,
                self.campaign.clone(),
                geometry.required_timed_carrier_bytes,
                path.attached_at,
                expires_at,
                ticket,
            ) else {
                continue;
            };
            let request = RequestTcpCapacityProbeRequest {
                stream_id,
                path_instance: instance,
                path_id: candidate_snapshot.id,
                measurement_id: token,
                train_payload_bytes,
                sample_floor_bytes: geometry.sample_floor_bytes,
                warmup_carrier_bytes: geometry.warmup_carrier_bytes,
                timing_slack_bytes: geometry.timing_slack_bytes,
                required_timed_carrier_bytes: geometry.required_timed_carrier_bytes,
                baseline_expires_at,
                write_expires_at,
                expires_at,
            };
            if path
                .stream
                .try_enqueue_request_tcp_capacity_probe(request, lease.clone())
                .is_err()
            {
                continue;
            }
            self.attempted_paths.insert(instance.key.index);
            if !lease.commit() {
                // The exact carrier dequeued and rejected this one-shot attempt
                // before planner commit without putting any train byte on wire.
                continue;
            }
            let previous = self.measurements.insert(
                instance,
                RequestTcpCapacityMeasurement {
                    token,
                    publication_expires_at: expires_at,
                    proof_expires_at: None,
                    capacity_admitted: false,
                    lease,
                },
            );
            debug_assert!(previous.is_none());
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_tcp_capacity_measurement",
                format_args!(
                    "phase=started stream_id={} path_index={} instance_id={} measurement_id={} train_bytes={} train_envelope_bytes={} sample_floor_bytes={} accounting_slack_bytes={} timing_slack_bytes={} warmup_bytes={} required_timed_bytes={} candidate_carrier_flight_bytes={} candidate_srtt_ms={:.3} candidate_jitter_ms={:.3} reference_rate_mbps={:.3} reference_delivery_samples={} baseline_budget_ms={} write_deadline_after_ms={} final_budget_ms={} lease_ms={}",
                    stream_id.0,
                    instance.key.index,
                    instance.attachment_id,
                    token,
                    train_payload_bytes,
                    _train_envelope_bytes,
                    geometry.sample_floor_bytes,
                    geometry.accounting_slack_bytes,
                    geometry.timing_slack_bytes,
                    geometry.warmup_carrier_bytes,
                    geometry.required_timed_carrier_bytes,
                    geometry.candidate_carrier_flight_bytes,
                    candidate_snapshot.srtt_ms,
                    candidate_snapshot.jitter_ms,
                    geometry.reference_rate_bps as f64 / 1_000_000.0,
                    reference_model.delivery_samples,
                    baseline_budget.as_millis(),
                    lease_duration.saturating_sub(baseline_budget).as_millis(),
                    baseline_budget.as_millis(),
                    lease_duration.as_millis(),
                ),
            );
        }
    }
}

#[cfg(test)]
#[path = "tests_tcp_capacity.rs"]
mod tests;
