//! Request QUIC capacity controller.
//!
//! QUIC owns packet-ACK probe publication and its bounded handoff lifetime.
//! Product graduation remains an explicit event applied by the request owner.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::reliable_subflow_startup_sample_limit_bytes;
use crate::model::path::RelayPathInstance;
use crate::model::path::RelayPathPlacement;
#[cfg(feature = "lab-diagnostics")]
use crate::model::request::capacity::request_quic_capacity_slow_start_rounds;
use crate::model::request::capacity::{
    request_capacity_stable_candidate_share_bytes, request_quic_capacity_calibration_geometry,
    request_quic_capacity_calibration_lease,
};
use crate::model::timing::quic_bulk_proof_freshness_horizon;
use crate::model::work::ReliableWorkClass;
use crate::protocol::{PathId, StreamId, UnderlayProtocol};
use crate::runtime::path::{
    CapacityProbeCommandTicket, ClientPathContext, QuicCapacityProbeCommand,
    QuicCapacityProbeOwner, RequestCapacityProbeCampaignBudget, RequestCapacityReconciliationView,
    RequestQuicCapacityProbeLease, RequestQuicCapacityProductHandoffState,
    RequestQuicCapacityReconciliationQuery,
};
use crate::runtime::relay::ReliableRelayRemoteSet;
use crate::runtime::stream::request::RequestStreamState;
use crate::scheduler::FlowLane;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub(super) struct RequestQuicCapacityCalibration {
    pub(super) target: RelayPathInstance,
    pub(super) token: u64,
    pub(super) publication_expires_at: Instant,
    pub(super) graduated: bool,
    pub(super) ticket: CapacityProbeCommandTicket,
    pub(super) _lease: RequestQuicCapacityProbeLease,
}

impl Drop for RequestQuicCapacityCalibration {
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
    ProductHandoffComplete {
        target: RelayPathInstance,
        calibration: RequestQuicCapacityCalibration,
    },
    ProductHandoffExpired {
        target: RelayPathInstance,
        calibration: RequestQuicCapacityCalibration,
    },
}

#[derive(Debug)]
pub(super) struct RequestQuicCapacityController {
    pub(super) active: Option<RequestQuicCapacityCalibration>,
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
            .map(|calibration| RequestQuicCapacityReconciliationQuery {
                target: calibration.target,
                token: calibration.token,
            })
    }

    /// Native QUIC packet-ACK evidence is carrier admission evidence; the
    /// request owner applies the returned exact-instance graduations.
    pub(super) fn native_evidence_targets<'a>(
        &'a self,
        context: &'a ClientPathContext,
        ordered_service: Option<RelayPathInstance>,
        remotes: &'a ReliableRelayRemoteSet,
        now: Instant,
    ) -> impl Iterator<Item = RelayPathInstance> + 'a {
        let service_available = ordered_service.is_some_and(|service| {
            service.key.underlay == UnderlayProtocol::Udp && remotes.contains_path_instance(service)
        });
        remotes.paths.iter().filter_map(move |path| {
            let instance = path.instance();
            let admissible = service_available
                && path.placement == RelayPathPlacement::Validation
                && instance.key.underlay == UnderlayProtocol::Udp
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
            .is_some_and(|calibration| !remotes.contains_path_instance(calibration.target))
        {
            let calibration = self
                .active
                .take()
                .expect("detached QUIC calibration was just observed");
            context.retire_request_quic_capacity_probe_token(calibration.token);
            drop(calibration);
            return events;
        }

        // Deadline-edge proof acceptance precedes publication expiry.
        let accepted = self
            .active
            .as_ref()
            .filter(|calibration| !calibration.graduated)
            .filter(|calibration| view.quic_carrier_proven(calibration.target, calibration.token))
            .map(|calibration| (calibration.target, calibration.token));
        if let Some((target, token)) = accepted {
            if let Some(calibration) = self.active.as_mut() {
                calibration.graduated = true;
            }
            events.push(RequestQuicCapacityEvent::CarrierProofAccepted { target, token });
        }

        if self.active.as_ref().is_some_and(|calibration| {
            !calibration.graduated && now >= calibration.publication_expires_at
        }) {
            let calibration = self
                .active
                .take()
                .expect("expired QUIC calibration was just observed");
            context.retire_request_quic_capacity_probe_token(calibration.token);
            drop(calibration);
            return events;
        }

        let handoff = self
            .active
            .as_ref()
            .filter(|calibration| calibration.graduated)
            .map(|calibration| {
                (
                    calibration.target,
                    calibration.token,
                    view.quic_handoff_state(calibration.target, calibration.token),
                )
            });
        let Some((target, token, state)) = handoff else {
            return events;
        };
        match state {
            RequestQuicCapacityProductHandoffState::Pending => events,
            RequestQuicCapacityProductHandoffState::Complete => {
                context.retire_request_quic_capacity_probe_token(token);
                let calibration = self
                    .active
                    .take()
                    .expect("observed QUIC calibration remains serialized");
                debug_assert_eq!(calibration.token, token);
                events.push(RequestQuicCapacityEvent::ProductHandoffComplete {
                    target,
                    calibration,
                });
                events
            }
            RequestQuicCapacityProductHandoffState::Absent => {
                context.retire_request_quic_capacity_probe_token(token);
                let calibration = self
                    .active
                    .take()
                    .expect("observed QUIC calibration remains serialized");
                debug_assert_eq!(calibration.token, token);
                events.push(RequestQuicCapacityEvent::ProductHandoffExpired {
                    target,
                    calibration,
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
        lane: FlowLane,
    ) {
        if !lane.is_bulk() || self.active.is_some() {
            return;
        }
        let has_unattempted_udp_candidate = remotes.paths.iter().any(|path| {
            let instance = path.instance();
            path.placement == RelayPathPlacement::Validation
                && instance.key.underlay == UnderlayProtocol::Udp
                && context.relay_path_allows_automatic_bulk_use(instance.key)
                && !self.attempted_paths.contains(&instance.key.index)
                && !request
                    .subflows
                    .get(instance)
                    .is_some_and(|state| state.graduated())
        });
        if !has_unattempted_udp_candidate || context.reliable_relay_has_latency_pressure() {
            // Bulk sends call this repeatedly. Topology can reject the common
            // single-path/completed case without touching session health.
            return;
        }
        let Some(service_path) = request.ordered_service.and_then(|service| {
            (service.key.underlay == UnderlayProtocol::Udp).then(|| {
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
        if service_snapshot.active_latency_sensitive_flows > 0
            || service_snapshot.session_active_latency_sensitive_flows > 0
        {
            return;
        }
        if service_snapshot.product_bytes_in_flight
            < reliable_subflow_startup_sample_limit_bytes(context.mux_limits)
        {
            return;
        }
        // QUIC keeps its native packet-ACK proof transaction, but shares TCP's
        // topology-stable budget policy. Attempt order cannot enlarge a train.
        let eligible_candidates =
            context.automatic_bulk_path_count(UnderlayProtocol::Udp, Some(service.key.index));
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
                path.placement == RelayPathPlacement::Validation
                    && instance.key.underlay == UnderlayProtocol::Udp
                    && context.relay_path_allows_automatic_bulk_use(instance.key)
                    && !self.attempted_paths.contains(&instance.key.index)
                    && !request
                        .subflows
                        .get(instance)
                        .is_some_and(|state| state.graduated())
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
                            && snapshot.product_bytes_in_flight == 0
                            && snapshot.product_queue_bytes == 0
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
                let geometry = request_quic_capacity_calibration_geometry(
                    snapshot,
                    service_snapshot.delivery_rate_bps,
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
        let lease_duration = request_quic_capacity_calibration_lease(snapshot, train_payload_bytes);
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
            owner: QuicCapacityProbeOwner::Request {
                stream_id: stream_id,
                path_instance: instance,
            },
            path_id: PathId(instance.key.index as u16),
            calibration_id: token,
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
        self.active = Some(RequestQuicCapacityCalibration {
            target: instance,
            token,
            publication_expires_at,
            graduated: false,
            ticket,
            _lease: lease,
        });
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "request_quic_capacity_calibration",
            format_args!(
                "phase=started stream_id={} path_index={} instance_id={} calibration_id={} train_bytes={} stable_candidate_share_bytes={} train_envelope_bytes={} sample_floor_bytes={} accounting_slack_bytes={} timing_slack_bytes={} desired_warmup_bytes={} warmup_bytes={} required_proof_bytes={} candidate_carrier_flight_bytes={} service_rate_bps={} service_rate_scope={:?} slow_start_rounds={} lease_ms={}",
                stream_id.0,
                instance.key.index,
                instance.id,
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
                geometry.service_rate_bps,
                service_snapshot.rate_scope,
                request_quic_capacity_slow_start_rounds(train_payload_bytes),
                lease_duration.as_millis(),
            ),
        );
    }
}

#[cfg(test)]
#[path = "quic_capacity_test.rs"]
mod tests;
