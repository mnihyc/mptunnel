//! Request TCP capacity controller.
//!
//! TCP owns receipt-probe leases and carrier-proof lifetime. It reports typed
//! outcomes upward because only the request product owner may graduate a
//! subflow or preserve a sealed ACK-clock transaction.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    TcpCapacityProofCandidate, product_delivery_samples_override_startup_prior,
};
use crate::model::path::RelayPathInstance;
use crate::model::path::RelayPathPlacement;
use crate::model::request::capacity::{
    request_capacity_stable_candidate_share_bytes, request_tcp_capacity_calibration_geometry,
    request_tcp_capacity_calibration_lease, request_tcp_capacity_candidate_can_start_receipt,
};
use crate::model::timing::transport_pto_from_snapshot;
use crate::model::work::ReliableWorkClass;
use crate::protocol::{PathId, StreamId, UnderlayProtocol};
use crate::runtime::path::{
    ClientPathContext, QuicCapacityProbeCommandTicket, RequestCapacityProbeCampaignBudget,
    RequestCapacityReconciliationView, RequestTcpCapacityProbeLease,
    RequestTcpCapacityProbeRequest, RequestTcpCapacityProofQuery,
};
#[cfg(feature = "lab-diagnostics")]
use crate::runtime::relay::ReliableRelayRemotePath;
use crate::runtime::relay::ReliableRelayRemoteSet;
use crate::runtime::stream::request::RequestStreamState;
use crate::scheduler::FlowLane;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
pub(super) struct RequestTcpCapacityCalibration {
    pub(super) token: u64,
    pub(super) publication_expires_at: Instant,
    pub(super) proof_expires_at: Option<Instant>,
    pub(super) graduated: bool,
    pub(super) lease: RequestTcpCapacityProbeLease,
}

impl Drop for RequestTcpCapacityCalibration {
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
    ProductHandoffComplete {
        target: RelayPathInstance,
        calibration: RequestTcpCapacityCalibration,
    },
    CarrierAuthorityRetired {
        target: RelayPathInstance,
        calibration: RequestTcpCapacityCalibration,
        cause: RequestTcpCapacityRetirement,
    },
}

#[derive(Debug)]
pub(super) struct RequestTcpCapacityController {
    pub(super) calibrations: HashMap<RelayPathInstance, RequestTcpCapacityCalibration>,
    pub(super) attempted_paths: HashSet<usize>,
    pub(super) campaign: Arc<RequestCapacityProbeCampaignBudget>,
    #[cfg(feature = "lab-diagnostics")]
    pub(super) last_gate: Option<(Option<RelayPathInstance>, &'static str)>,
}

impl Default for RequestTcpCapacityController {
    fn default() -> Self {
        Self {
            calibrations: HashMap::new(),
            attempted_paths: HashSet::new(),
            campaign: Arc::new(RequestCapacityProbeCampaignBudget::default()),
            #[cfg(feature = "lab-diagnostics")]
            last_gate: None,
        }
    }
}

impl RequestTcpCapacityController {
    pub(super) fn remove(&mut self, target: RelayPathInstance) {
        self.calibrations.remove(&target);
    }

    pub(super) fn proof_queries(&self) -> impl Iterator<Item = RequestTcpCapacityProofQuery> + '_ {
        self.calibrations
            .iter()
            .map(|(target, calibration)| RequestTcpCapacityProofQuery {
                target: *target,
                token: calibration.token,
            })
    }

    pub(super) fn reconcile(
        &mut self,
        view: &RequestCapacityReconciliationView,
        remotes: &ReliableRelayRemoteSet,
        completed_product_handoffs: &HashSet<RelayPathInstance>,
    ) -> Vec<RequestTcpCapacityEvent> {
        let now = view.observed_at();
        let mut events = Vec::new();
        let detached = self
            .calibrations
            .keys()
            .copied()
            .filter(|target| !remotes.contains_path_instance(*target))
            .collect::<Vec<_>>();
        for target in detached {
            let calibration = self
                .calibrations
                .remove(&target)
                .expect("detached TCP calibration collected from the same map");
            events.push(RequestTcpCapacityEvent::CarrierAuthorityRetired {
                target,
                calibration,
                cause: RequestTcpCapacityRetirement::Detached,
            });
        }

        let observations = self
            .calibrations
            .iter()
            .map(|(target, calibration)| {
                let proof = view.tcp_proof(*target);
                (
                    *target,
                    calibration.token,
                    calibration.graduated,
                    calibration.publication_expires_at,
                    calibration.proof_expires_at,
                    calibration.lease.is_current(),
                    calibration.lease.is_published(),
                    proof,
                )
            })
            .collect::<Vec<_>>();

        for (
            target,
            token,
            graduated,
            publication_expires_at,
            proof_expires_at,
            current,
            published,
            proof,
        ) in observations
        {
            if completed_product_handoffs.contains(&target) {
                let calibration = self
                    .calibrations
                    .remove(&target)
                    .expect("observed TCP calibration remains serialized");
                events.push(RequestTcpCapacityEvent::ProductHandoffComplete {
                    target,
                    calibration,
                });
                continue;
            }
            if !graduated {
                if let Some(proof) = proof {
                    if let Some(calibration) = self.calibrations.get_mut(&target) {
                        calibration.graduated = true;
                        calibration.proof_expires_at = Some(proof.expires_at);
                    }
                    events.push(RequestTcpCapacityEvent::CarrierProofAccepted {
                        target,
                        token,
                        proof,
                    });
                } else if now >= publication_expires_at || !current {
                    let calibration = self
                        .calibrations
                        .remove(&target)
                        .expect("observed TCP calibration remains serialized");
                    events.push(RequestTcpCapacityEvent::CarrierAuthorityRetired {
                        target,
                        calibration,
                        cause: RequestTcpCapacityRetirement::PublicationExpired,
                    });
                }
                continue;
            }

            let authority_expired =
                request_tcp_carrier_authority_expired_naturally(published, proof_expires_at, now);
            if authority_expired {
                let calibration = self
                    .calibrations
                    .remove(&target)
                    .expect("observed TCP calibration remains serialized");
                events.push(RequestTcpCapacityEvent::CarrierAuthorityRetired {
                    target,
                    calibration,
                    cause: RequestTcpCapacityRetirement::AuthorityExpired,
                });
            } else if proof.is_none() || !published {
                let calibration = self
                    .calibrations
                    .remove(&target)
                    .expect("observed TCP calibration remains serialized");
                events.push(RequestTcpCapacityEvent::CarrierAuthorityRetired {
                    target,
                    calibration,
                    cause: RequestTcpCapacityRetirement::AuthorityLost,
                });
            }
        }
        events
    }
}

impl RequestTcpCapacityController {
    #[cfg(feature = "lab-diagnostics")]
    fn diagnose_request_tcp_capacity_gate(
        &mut self,
        stream_id: StreamId,
        request: &RequestStreamState,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: FlowLane,
    ) {
        let active_tcp_service_flows = context.active_tcp_service_request_bulk_flows();
        let latency_pressure = context.reliable_relay_has_latency_pressure();
        let service_path = request.ordered_service.and_then(|service| {
            (service.key.underlay == UnderlayProtocol::Tcp)
                .then(|| {
                    remotes.paths.iter().find(|path| {
                        path.instance() == service && path.placement == RelayPathPlacement::Active
                    })
                })
                .flatten()
        });
        let service_instance = service_path.map(ReliableRelayRemotePath::instance);
        let service_snapshot =
            service_instance.and_then(|instance| context.reliable_path_snapshot(instance.key));
        let service_bulk_evidence = service_instance.is_some_and(|instance| {
            context.relay_path_has_bulk_model_evidence(instance.key.underlay, instance.key.index)
        });
        let service_model = service_instance.and_then(|instance| {
            request
                .subflows
                .get(instance)
                .and_then(|state| state.per_flow_rate())
        });
        let candidate_path = remotes
            .paths
            .iter()
            .filter(|path| {
                path.placement == RelayPathPlacement::Validation
                    && path.key().underlay == UnderlayProtocol::Tcp
                    && context.relay_path_allows_automatic_bulk_use(path.key())
                    && !self.attempted_paths.contains(&path.key().index)
            })
            .min_by_key(|path| context.relay_path_config_ordinal(path.key()));
        let candidate_instance = candidate_path.map(ReliableRelayRemotePath::instance);
        let candidate_snapshot =
            candidate_instance.and_then(|instance| context.reliable_path_snapshot(instance.key));
        let eligible_candidates = context.automatic_bulk_path_count(
            UnderlayProtocol::Tcp,
            service_instance.map(|instance| instance.key.index),
        );
        let proposed_candidate_share =
            request_capacity_stable_candidate_share_bytes(context.mux_limits, eligible_candidates);
        let stable_candidate_share =
            context.request_tcp_capacity_probe_candidate_share_bytes(proposed_candidate_share);
        let campaign_remaining_bytes = self.campaign.remaining_bytes(stable_candidate_share);
        let session_remaining_bytes = context.request_tcp_capacity_probe_remaining_bytes();
        let train_envelope_bytes = candidate_instance.map_or(0, |instance| {
            session_remaining_bytes.min(campaign_remaining_bytes).min(
                context.request_tcp_capacity_probe_path_remaining_bytes(
                    instance.key.index,
                    stable_candidate_share,
                ),
            )
        });
        let geometry = candidate_snapshot
            .zip(service_model)
            .and_then(|(candidate, service)| {
                request_tcp_capacity_calibration_geometry(
                    candidate,
                    service,
                    context.mux_limits,
                    train_envelope_bytes,
                )
            });
        let candidate_bulk_evidence = candidate_instance.is_some_and(|instance| {
            context.relay_path_has_bulk_model_evidence(instance.key.underlay, instance.key.index)
        });
        let path_proof_fresh = candidate_path.is_some_and(|path| {
            let instance = path.instance();
            path.path_proof_id.is_some_and(|proof_id| {
                context.relay_path_has_fresh_proof(
                    instance.key.underlay,
                    instance.key.index,
                    proof_id,
                    path.attached_at,
                )
            })
        });
        let can_enqueue = candidate_path.is_some_and(|path| {
            path.stream
                .can_enqueue_work_lane_now(ReliableWorkClass::Data, lane)
        });
        let gate = if !lane.is_bulk() {
            "non_bulk_lane"
        } else if active_tcp_service_flows != 1 {
            "tcp_service_flow_count"
        } else if latency_pressure {
            "session_latency_pressure"
        } else if service_path.is_none() {
            "active_tcp_service_missing"
        } else if service_snapshot.is_none() {
            "service_snapshot_missing"
        } else if !service_bulk_evidence {
            "service_bulk_evidence_missing"
        } else if service_snapshot.is_some_and(|snapshot| {
            snapshot.active_latency_sensitive_flows > 0
                || snapshot.session_active_latency_sensitive_flows > 0
        }) {
            "service_latency_pressure"
        } else if service_model.is_none() {
            "service_flow_model_missing"
        } else if service_model.is_some_and(|model| {
            !product_delivery_samples_override_startup_prior(model.delivery_samples)
        }) {
            "service_flow_model_immature"
        } else if candidate_path.is_none() {
            "validation_tcp_missing"
        } else if candidate_instance.is_some_and(|instance| {
            request
                .subflows
                .get(instance)
                .is_some_and(|state| state.graduated())
        }) {
            "candidate_already_graduated"
        } else if candidate_instance.is_some_and(|instance| {
            request
                .subflows
                .get(instance)
                .is_some_and(|state| state.has_product_evidence())
        }) {
            "candidate_product_evidence_present"
        } else if candidate_path.is_some_and(|path| path.path_proof_id.is_none()) {
            "candidate_path_proof_missing"
        } else if !path_proof_fresh {
            "candidate_path_proof_stale"
        } else if candidate_bulk_evidence {
            "candidate_bulk_evidence_present"
        } else if candidate_snapshot.is_none() {
            "candidate_snapshot_missing"
        } else if geometry.is_none() {
            "train_geometry_unavailable"
        } else if candidate_snapshot.is_some_and(|snapshot| snapshot.queue_bytes > 0) {
            "candidate_carrier_queue"
        } else if candidate_snapshot.is_some_and(|snapshot| snapshot.product_bytes_in_flight > 0) {
            "candidate_product_inflight"
        } else if candidate_snapshot.is_some_and(|snapshot| snapshot.product_queue_bytes > 0) {
            "candidate_product_queue"
        } else if candidate_snapshot.is_some_and(|snapshot| {
            snapshot.active_latency_sensitive_flows > 0
                || snapshot.session_active_latency_sensitive_flows > 0
        }) {
            "candidate_latency_pressure"
        } else if !can_enqueue {
            "candidate_queue_credit_missing"
        } else {
            "eligible"
        };
        let signature = (candidate_instance, gate);
        if self.last_gate == Some(signature) {
            return;
        }
        self.last_gate = Some(signature);
        lab_diagnostic(
            "request_tcp_capacity_gate",
            format_args!(
                "stream_id={} first_failed_gate={} lane={:?} active_tcp_service_flows={} latency_pressure={} service_path_index={} service_bulk_evidence={} service_carrier_bif={} service_product_bif={} service_rate_mbps={:.3} service_delivery_samples={} candidate_path_index={} candidate_proof_id={} candidate_proof_fresh={} candidate_bulk_evidence={} candidate_carrier_bif={} candidate_queue_bytes={} candidate_product_bif={} candidate_product_queue_bytes={} can_enqueue={} train_bytes={} stable_candidate_share_bytes={} campaign_remaining_bytes={} train_envelope_bytes={} session_remaining_bytes={}",
                stream_id.0,
                gate,
                lane,
                active_tcp_service_flows,
                latency_pressure,
                service_instance.map_or(-1, |instance| instance.key.index as i64),
                service_bulk_evidence,
                service_snapshot.map_or(0, |snapshot| snapshot.bytes_in_flight),
                service_snapshot.map_or(0, |snapshot| snapshot.product_bytes_in_flight),
                service_model.map_or(0.0, |model| model.rate_bps / 1_000_000.0),
                service_model.map_or(0, |model| model.delivery_samples),
                candidate_instance.map_or(-1, |instance| instance.key.index as i64),
                candidate_path
                    .and_then(|path| path.path_proof_id)
                    .unwrap_or(0),
                path_proof_fresh,
                candidate_bulk_evidence,
                candidate_snapshot.map_or(0, |snapshot| snapshot.bytes_in_flight),
                candidate_snapshot.map_or(0, |snapshot| snapshot.queue_bytes),
                candidate_snapshot.map_or(0, |snapshot| snapshot.product_bytes_in_flight),
                candidate_snapshot.map_or(0, |snapshot| snapshot.product_queue_bytes),
                can_enqueue,
                geometry.map_or(0, |geometry| geometry.train_bytes),
                stable_candidate_share,
                campaign_remaining_bytes,
                train_envelope_bytes,
                context.request_tcp_capacity_probe_remaining_bytes(),
            ),
        );
    }

    pub(super) fn try_start(
        &mut self,
        stream_id: StreamId,
        request: &RequestStreamState,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: FlowLane,
    ) {
        #[cfg(feature = "lab-diagnostics")]
        self.diagnose_request_tcp_capacity_gate(stream_id, request, context, remotes, lane);
        if !lane.is_bulk()
            || context.active_tcp_service_request_bulk_flows() != 1
            || context.reliable_relay_has_latency_pressure()
        {
            return;
        }
        let Some(service_path) = request.ordered_service.and_then(|service| {
            (service.key.underlay == UnderlayProtocol::Tcp).then(|| {
                remotes.paths.iter().find(|path| {
                    path.instance() == service && path.placement == RelayPathPlacement::Active
                })
            })?
        }) else {
            return;
        };
        let service = service_path.instance();
        let Some(service_snapshot) = context.reliable_path_snapshot(service.key) else {
            return;
        };
        if !context.relay_path_has_bulk_model_evidence(service.key.underlay, service.key.index)
            || service_snapshot.active_latency_sensitive_flows > 0
            || service_snapshot.session_active_latency_sensitive_flows > 0
        {
            return;
        }
        let Some(service_model) = request
            .subflows
            .get(service)
            .and_then(|state| state.per_flow_rate())
            .filter(|model| {
                product_delivery_samples_override_startup_prior(model.delivery_samples)
            })
        else {
            return;
        };
        // The Service model prices only train geometry. The candidate's full
        // receiver-confirmed train remains the sole cold startup-rate authority.
        // Freeze one fair share per configured eligible candidate. A late
        // path must not inherit the unused campaign budget of earlier paths.
        let eligible_candidates =
            context.automatic_bulk_path_count(UnderlayProtocol::Tcp, Some(service.key.index));
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
                let snapshot = context.reliable_path_snapshot(instance.key);
                path.placement == RelayPathPlacement::Validation
                    && instance.key.underlay == UnderlayProtocol::Tcp
                    && context.relay_path_allows_automatic_bulk_use(instance.key)
                    && !self.attempted_paths.contains(&instance.key.index)
                    && !request
                        .subflows
                        .get(instance)
                        .is_some_and(|state| state.graduated() || state.has_product_evidence())
                    && path.path_proof_id.is_some_and(|proof_id| {
                        context.relay_path_has_fresh_proof(
                            instance.key.underlay,
                            instance.key.index,
                            proof_id,
                            path.attached_at,
                        )
                    })
                    && !context.relay_path_has_bulk_model_evidence(
                        instance.key.underlay,
                        instance.key.index,
                    )
                    && snapshot.is_some_and(request_tcp_capacity_candidate_can_start_receipt)
                    && path
                        .stream
                        .can_enqueue_work_lane_now(ReliableWorkClass::Data, lane)
            })
            .filter_map(|path| {
                let candidate_snapshot = context.reliable_path_snapshot(path.key())?;
                let campaign_remaining_bytes =
                    self.campaign.remaining_bytes(stable_candidate_share);
                let train_envelope_bytes = session_remaining_bytes
                    .min(campaign_remaining_bytes)
                    .min(context.request_tcp_capacity_probe_path_remaining_bytes(
                        path.key().index,
                        stable_candidate_share,
                    ));
                let geometry = request_tcp_capacity_calibration_geometry(
                    candidate_snapshot,
                    service_model,
                    context.mux_limits,
                    train_envelope_bytes,
                )?;
                Some((path, candidate_snapshot, geometry, train_envelope_bytes))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(path, _, _, _)| context.relay_path_config_ordinal(path.key()));
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
            let ticket = QuicCapacityProbeCommandTicket::new();
            let now = Instant::now();
            let baseline_budget = transport_pto_from_snapshot(Some(candidate_snapshot));
            let lease_duration = request_tcp_capacity_calibration_lease(
                candidate_snapshot,
                train_payload_bytes,
                geometry.service_rate_bps,
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
                stream_id: stream_id,
                path_instance: instance,
                path_id: PathId(instance.key.index as u16),
                calibration_id: token,
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
            let previous = self.calibrations.insert(
                instance,
                RequestTcpCapacityCalibration {
                    token,
                    publication_expires_at: expires_at,
                    proof_expires_at: None,
                    graduated: false,
                    lease,
                },
            );
            debug_assert!(previous.is_none());
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_tcp_capacity_calibration",
                format_args!(
                    "phase=started stream_id={} path_index={} instance_id={} calibration_id={} train_bytes={} train_envelope_bytes={} sample_floor_bytes={} accounting_slack_bytes={} timing_slack_bytes={} warmup_bytes={} required_timed_bytes={} candidate_carrier_flight_bytes={} candidate_srtt_ms={:.3} candidate_jitter_ms={:.3} service_rate_mbps={:.3} service_delivery_samples={} baseline_budget_ms={} write_deadline_after_ms={} final_budget_ms={} lease_ms={}",
                    stream_id.0,
                    instance.key.index,
                    instance.id,
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
                    geometry.service_rate_bps as f64 / 1_000_000.0,
                    service_model.delivery_samples,
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
#[path = "tcp_capacity_test.rs"]
mod tests;
