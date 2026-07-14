//! Response evidence gates and Subflow admission-epoch ownership.
//! One mutex owns epoch mutation; metrics, topology, and carrier recovery stay outside it.

use super::ResponseStreamBinding;
use super::response_ack_clock::ResponseAckClockCalibrationState;
use super::response_evidence::{
    ServerPathMetricsSource, server_output_local_path_metrics,
    server_path_metrics_has_bulk_rate_evidence, server_path_metrics_has_sender_evidence,
    server_udp_path_metrics_has_durable_rate_estimate,
};
use super::response_snapshot::server_bulk_output_snapshot;
use super::response_topology::{
    ResponseStreamOutputEntry, ResponseStreamOutputs, TcpResponseCapacityPrior,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::ack_clock::{
    reliable_ack_clock_calibration_ceiling_bytes,
    reliable_ack_clock_calibration_rate_coverage_floor_bytes,
    reliable_tcp_ack_clock_calibration_initial_limit_bytes,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, product_delivery_samples_override_startup_prior,
    reliable_subflow_startup_sample_limit_bytes,
};
use crate::model::multipath::{
    FlowSubflowSet, PathAdmission, PathAdmissionDecision, SubflowAdmissionInput,
};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{StreamOpenRole, UnderlayProtocol};
use std::time::{Duration, Instant};

#[derive(Default)]
pub(super) struct ResponseSubflowSetState {
    pub(super) planner_generation: u64,
    pub(super) epoch_generation: u64,
    pub(super) set: Option<FlowSubflowSet>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseSubflowAdmissionReservation {
    pub(in crate::runtime) admission: PathAdmission,
    pub(in crate::runtime) epoch_generation: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseSubflowAdmissionRequest {
    pub(in crate::runtime) expected_planner_generation: u64,
    pub(in crate::runtime) expected_lane_generation: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) startup_owner_credit_bytes: usize,
    pub(in crate::runtime) optional_overhead_budget_bytes: usize,
    pub(in crate::runtime) max_read_gap_budget: Duration,
    pub(in crate::runtime) input: SubflowAdmissionInput,
}

pub(in crate::runtime) fn server_output_has_sender_evidence(
    entry: &ResponseStreamOutputEntry,
) -> bool {
    entry.owner_data_acked_bytes > 0
        || entry.delivery_samples > 0
        || entry.delivery_rate_bps.is_some()
        || matches!(
            server_output_local_path_metrics(entry),
            Some(path_metrics) if server_path_metrics_has_sender_evidence(path_metrics)
        )
}

/// Endpoint-only TCP has no carrier hint worth preserving. After an exact
/// startup sample, it may temporarily inherit the proven Service opportunity
/// instead of running a second exclusive measurement transport.
pub(in crate::runtime) fn server_output_accepts_service_capacity_prior(
    entry: &ResponseStreamOutputEntry,
) -> bool {
    entry.key.underlay == UnderlayProtocol::Tcp
        && !product_delivery_samples_override_startup_prior(entry.delivery_samples)
        && !server_output_local_path_metrics(entry)
            .is_some_and(server_path_metrics_has_bulk_rate_evidence)
        && entry.peer_path_metrics.is_some_and(|metrics| {
            metrics.source == ServerPathMetricsSource::PeerHint
                && metrics.metrics.app_limited
                && !metrics.metrics.has_ack_derived_data_sample
        })
}

pub(in crate::runtime) fn server_output_has_durable_product_progress(
    entry: &ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) -> bool {
    entry.product_progress_rate_bps.is_some()
        && server_output_has_durable_product_ack_progress(entry, mux_limits)
}

pub(super) fn server_output_has_durable_product_ack_progress(
    entry: &ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) -> bool {
    // Exact ownership bytes may be durable even when fragmented callbacks do
    // not produce an individual point-rate sample.
    let sample_floor = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    let accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    entry
        .owner_data_acked_bytes
        .saturating_add(accounting_slack)
        >= sample_floor
}

#[cfg(test)]
pub(in crate::runtime) fn server_output_has_bulk_rate_evidence(
    entry: &ResponseStreamOutputEntry,
) -> bool {
    server_output_has_bulk_rate_evidence_with_limits(entry, MuxLimits::default())
}

pub(in crate::runtime) fn server_output_has_bulk_rate_evidence_with_limits(
    entry: &ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) -> bool {
    let has_local_carrier_bulk = matches!(
        server_output_local_path_metrics(entry),
        Some(path_metrics) if server_path_metrics_has_bulk_rate_evidence(path_metrics)
    );
    match entry.key.underlay {
        UnderlayProtocol::Udp => has_local_carrier_bulk,
        UnderlayProtocol::Tcp => {
            has_local_carrier_bulk || server_output_has_durable_product_progress(entry, mux_limits)
        }
    }
}

pub(in crate::runtime) fn server_output_has_service_feed_evidence_with_limits(
    entry: &ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) -> bool {
    match entry.key.underlay {
        UnderlayProtocol::Udp => {
            server_output_has_durable_product_progress(entry, mux_limits)
                || matches!(
                    server_output_local_path_metrics(entry),
                    Some(path_metrics) if server_udp_path_metrics_has_durable_rate_estimate(path_metrics)
                )
        }
        UnderlayProtocol::Tcp => {
            server_output_has_bulk_rate_evidence_with_limits(entry, mux_limits)
        }
    }
}

impl ResponseStreamBinding {
    fn subflow_set_for(
        current: Option<FlowSubflowSet>,
        epoch_generation: u64,
        service: CarrierPathKey,
        startup_owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
    ) -> FlowSubflowSet {
        current
            .filter(|epoch| {
                epoch.matches_envelope(
                    service,
                    startup_owner_credit_bytes,
                    optional_overhead_budget_bytes,
                    max_read_gap_budget,
                )
            })
            .unwrap_or_else(|| {
                FlowSubflowSet::new(
                    epoch_generation,
                    service,
                    startup_owner_credit_bytes,
                    optional_overhead_budget_bytes,
                    max_read_gap_budget,
                )
            })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn subflow_set_snapshot(&self) -> Option<FlowSubflowSet> {
        self.subflow_set
            .lock()
            .expect("server reliable stream subflow set lock")
            .set
            .clone()
    }

    pub(in crate::runtime) fn subflow_state_snapshot(&self) -> (u64, Option<FlowSubflowSet>) {
        let state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        (state.planner_generation, state.set.clone())
    }

    #[cfg(test)]
    pub(in crate::runtime) fn preview_subflow_owner_admission(
        &self,
        service: CarrierPathKey,
        startup_owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
        input: SubflowAdmissionInput,
    ) -> PathAdmission {
        let state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        let epoch_generation = state.epoch_generation;
        let current = state.set.clone();
        drop(state);
        let mut epoch = Self::subflow_set_for(
            current,
            epoch_generation,
            service,
            startup_owner_credit_bytes,
            optional_overhead_budget_bytes,
            max_read_gap_budget,
        );
        epoch.admit_subflow_owner(input)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn commit_subflow_owner_admission(
        &self,
        service: CarrierPathKey,
        startup_owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
        input: SubflowAdmissionInput,
    ) -> PathAdmission {
        let (generation, _) = self.subflow_state_snapshot();
        self.commit_subflow_owner_admission_for_planner_generation(
            generation,
            self.lane_generation(),
            service,
            startup_owner_credit_bytes,
            optional_overhead_budget_bytes,
            max_read_gap_budget,
            input,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn commit_subflow_owner_admission_for_planner_generation(
        &self,
        expected_planner_generation: u64,
        expected_lane_generation: u64,
        service: CarrierPathKey,
        startup_owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
        input: SubflowAdmissionInput,
    ) -> PathAdmission {
        self.reserve_subflow_owner_admission_for_planner_generation(
            expected_planner_generation,
            expected_lane_generation,
            service,
            startup_owner_credit_bytes,
            optional_overhead_budget_bytes,
            max_read_gap_budget,
            input,
        )
        .admission
    }

    #[cfg(test)]
    pub(in crate::runtime) fn reserve_subflow_owner_admission_for_planner_generation(
        &self,
        expected_planner_generation: u64,
        expected_lane_generation: u64,
        service: CarrierPathKey,
        startup_owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
        input: SubflowAdmissionInput,
    ) -> ResponseSubflowAdmissionReservation {
        let request = ResponseSubflowAdmissionRequest {
            expected_planner_generation,
            expected_lane_generation,
            service,
            startup_owner_credit_bytes,
            optional_overhead_budget_bytes,
            max_read_gap_budget,
            input,
        };
        let standby = || ResponseSubflowAdmissionReservation {
            admission: PathAdmission::standby(),
            epoch_generation: None,
        };
        self.lane_tracker
            .with_matching_generation(self.session_id, expected_lane_generation, || {
                self.reserve_subflow_owner_admission_for_request(request)
            })
            .unwrap_or_else(standby)
    }

    pub(super) fn reserve_subflow_owner_admission_for_request(
        &self,
        request: ResponseSubflowAdmissionRequest,
    ) -> ResponseSubflowAdmissionReservation {
        let standby = || ResponseSubflowAdmissionReservation {
            admission: PathAdmission::standby(),
            epoch_generation: None,
        };
        let mut state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        if state.planner_generation != request.expected_planner_generation {
            return standby();
        }
        let envelope_changed = state.set.as_ref().is_some_and(|epoch| {
            !epoch.matches_envelope(
                request.service,
                request.startup_owner_credit_bytes,
                request.optional_overhead_budget_bytes,
                request.max_read_gap_budget,
            )
        });
        if envelope_changed {
            state.planner_generation = state.planner_generation.wrapping_add(1);
            state.epoch_generation = state.epoch_generation.wrapping_add(1);
            state.set = None;
        }
        let current = state.set.take();
        let mut epoch = Self::subflow_set_for(
            current,
            state.epoch_generation,
            request.service,
            request.startup_owner_credit_bytes,
            request.optional_overhead_budget_bytes,
            request.max_read_gap_budget,
        );
        let admission = epoch.admit_subflow_owner(request.input);
        state.set = epoch.has_members().then_some(epoch);
        ResponseSubflowAdmissionReservation {
            epoch_generation: (admission.decision == PathAdmissionDecision::AdmitSubflow)
                .then_some(state.epoch_generation),
            admission,
        }
    }

    pub(in crate::runtime) fn rollback_subflow_owner_admission_for_epoch(
        &self,
        expected_epoch_generation: u64,
        input: SubflowAdmissionInput,
    ) {
        let mut state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        if state.epoch_generation == expected_epoch_generation
            && let Some(epoch) = state.set.as_mut()
        {
            epoch.rollback_subflow_owner(input);
        }
    }

    pub(super) fn graduate_completed_response_startup_owner(&self) -> bool {
        let startup = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock")
            .set
            .as_ref()
            .and_then(|epoch| {
                let owner = epoch.startup_owner_key()?;
                Some((owner, epoch.startup_owner_sealed_sample_bytes(owner)))
            });
        let Some((owner, sealed_sample_bytes)) = startup else {
            return false;
        };
        let lane = self.lane();

        // Owner enqueue holds the outputs lock from Subflow reservation through
        // flight recording. Keep it here so the no-flight proof and graduation
        // are one transition with respect to new response OwnerData.
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let owner_position = outputs.entries.iter().position(|entry| {
            if entry.key != owner
                || entry.role != StreamOpenRole::Validation
                || entry.commands.is_closed()
            {
                return false;
            }
            match entry.key.underlay {
                // Scheduler assignment is not a TCP send clock. Completing the
                // finite startup sample proves ownership/reachability and opens
                // prior selection or fallback measurement; only an exact ACK
                // clock may replace either temporary capacity value.
                UnderlayProtocol::Tcp => {
                    sealed_sample_bytes.is_some_and(|bytes| entry.owner_data_acked_bytes >= bytes)
                }
                // QUIC capacity is carrier-scoped and cannot be inferred from
                // product STREAM_ACK timing.
                UnderlayProtocol::Udp => {
                    server_output_has_bulk_rate_evidence_with_limits(entry, self.mux_limits)
                }
            }
        });
        let Some(owner_position) = owner_position else {
            return false;
        };
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        if flights
            .values()
            .flatten()
            .any(|flight| flight.key == owner && flight.kind.is_ordering_owner())
        {
            return false;
        }

        let mut state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        let service_key = state.set.as_ref().map(FlowSubflowSet::service_key);
        let graduated = state
            .set
            .as_mut()
            .is_some_and(|epoch| epoch.graduate_startup_owner(owner));
        if graduated {
            let owner_identity = (
                outputs.entries[owner_position].key,
                outputs.entries[owner_position].incarnation,
            );
            if owner_identity.0.underlay == UnderlayProtocol::Tcp {
                let service_capacity_prior_bps = if server_output_accepts_service_capacity_prior(
                    &outputs.entries[owner_position],
                ) {
                    service_key.and_then(|service_key| {
                        outputs
                            .entries
                            .iter()
                            .find(|entry| {
                                entry.key == service_key
                                    && entry.key.underlay == owner_identity.0.underlay
                                    && entry.role != StreamOpenRole::Repair
                                    && !entry.commands.is_closed()
                                    && server_output_has_bulk_rate_evidence_with_limits(
                                        entry,
                                        self.mux_limits,
                                    )
                            })
                            .map(|service| {
                                server_bulk_output_snapshot(
                                    service,
                                    self.session_id,
                                    lane,
                                    &self.lane_tracker,
                                    self.mux_limits,
                                    Instant::now(),
                                )
                                .delivery_rate_bps
                            })
                    })
                } else {
                    None
                };
                if let Some(rate_bps) = service_capacity_prior_bps {
                    // The exact startup sample already proved reachability and
                    // bounded ownership. Endpoint-only TCP has no independent
                    // carrier hint to preserve, so ordinary shared work can use
                    // the same typed Service opportunity that admitted a
                    // calibration seed. A fresh exact-ACK epoch replaces it.
                    let entry = &mut outputs.entries[owner_position];
                    entry.tcp_product_rate_evidence = None;
                    entry.tcp_ack_clock_rate_bps = None;
                    entry.tcp_capacity_prior = Some(TcpResponseCapacityPrior {
                        rate_bps,
                        ordinary_windows: 0,
                    });
                    entry.product_progress_rate_bps = Some(rate_bps);
                    entry.delivery_rate_bps = Some(rate_bps);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "response_tcp_capacity_prior",
                        format_args!(
                            "phase=service_opportunity session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} rate_bps={}",
                            self.session_id.0,
                            self.binding_instance_id,
                            owner_identity.0.underlay,
                            owner_identity.0.path_id.0,
                            owner_identity.1,
                            rate_bps,
                        ),
                    );
                } else {
                    let calibration_snapshot = server_bulk_output_snapshot(
                        &outputs.entries[owner_position],
                        self.session_id,
                        lane,
                        &self.lane_tracker,
                        self.mux_limits,
                        Instant::now(),
                    );
                    let initial_limit = reliable_tcp_ack_clock_calibration_initial_limit_bytes(
                        calibration_snapshot,
                        self.mux_limits,
                    );
                    let max_limit = reliable_ack_clock_calibration_ceiling_bytes(self.mux_limits);
                    if initial_limit > 0 && max_limit >= initial_limit {
                        let coverage_floor =
                            reliable_ack_clock_calibration_rate_coverage_floor_bytes(
                                self.mux_limits,
                            );
                        outputs
                            .ack_clock_calibrations
                            .entry(owner_identity)
                            .or_insert_with(|| {
                                ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
                                    initial_limit,
                                    max_limit,
                                    coverage_floor,
                                )
                            });
                    }
                }
            }
            // Preserve the epoch and its measured members, but invalidate any
            // planner snapshot that still treats this output as the exclusive
            // unproven startup owner.
            state.planner_generation = state.planner_generation.wrapping_add(1);
        }
        graduated
    }

    fn reset_subflow_set_state(&self) {
        let mut state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        state.planner_generation = state.planner_generation.wrapping_add(1);
        state.epoch_generation = state.epoch_generation.wrapping_add(1);
        state.set = None;
    }

    pub(super) fn reset_subflow_set_with_outputs(&self, outputs: &mut ResponseStreamOutputs) {
        let active_calibration_has_owner_flights = outputs
            .active_ack_clock_calibration
            .is_some_and(|(active_key, active_incarnation)| {
                outputs
                    .ack_clock_calibrations
                    .contains_key(&(active_key, active_incarnation))
                    && outputs.entries.iter().any(|entry| {
                        entry.key == active_key && entry.incarnation == active_incarnation
                    })
                    && self
                        .flights
                        .lock()
                        .expect("server reliable stream flight lock")
                        .values()
                        .flatten()
                        .any(|flight| {
                            flight.key == active_key
                                && flight.output_incarnation == active_incarnation
                                && flight.kind.is_ordering_owner()
                        })
            });
        for calibration in outputs.ack_clock_calibrations.values_mut() {
            if !calibration.proven {
                calibration.retire();
            }
        }
        if !active_calibration_has_owner_flights {
            outputs.active_ack_clock_calibration = None;
        }
        self.reset_subflow_set_state();
    }

    #[cfg(test)]
    pub(in crate::runtime) fn reset_subflow_set(&self) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        self.reset_subflow_set_with_outputs(&mut outputs);
    }

    pub(super) fn invalidate_subflow_plan(&self) {
        let mut state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        state.planner_generation = state.planner_generation.wrapping_add(1);
    }
}

#[cfg(test)]
#[path = "response_admission_test.rs"]
mod tests;
