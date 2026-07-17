//! Request path evidence and serialized stream state.
//!
//! Exact attachment instances fence measurement state across reconnects. The
//! client relay serializes this aggregate, so it remains lock-free.

use super::flight::RequestFlightLedger;
use crate::model::path::RelayPathInstance;
use crate::model::request_evidence::{RequestPathRateEvidence, RequestPerFlowRateModel};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum RequestAckClockOperation {
    Pending {
        reference: RelayPathInstance,
        candidate: RelayPathInstance,
    },
    Owner {
        candidate: RelayPathInstance,
        target_bytes: u64,
    },
}

impl RequestAckClockOperation {
    pub(in crate::runtime) fn candidate(self) -> RelayPathInstance {
        match self {
            Self::Pending { candidate, .. } | Self::Owner { candidate, .. } => candidate,
        }
    }
}

/// Evidence owned by one exact request-path attachment.
///
/// Exact instances, rather than configured path indexes, fence evidence across
/// reconnects. Keeping one record per instance also makes partial cleanup an
/// explicit state transition instead of a collection of unrelated map edits.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestPathState {
    rate_evidence: Option<RequestPathRateEvidence>,
    per_flow_rate: Option<RequestPerFlowRateModel>,
    product_delivery_proven: bool,
    ack_clock_first_window: bool,
    ack_clock_proven: bool,
    ack_clock_measurement_bytes: Option<u64>,
    ack_clock_measurement_target: Option<u64>,
    tcp_capacity_proven: bool,
    capacity_admitted: bool,
}

impl RequestPathState {
    pub(in crate::runtime) fn rate_evidence_mut(
        &mut self,
        observed_at: Instant,
    ) -> &mut RequestPathRateEvidence {
        self.rate_evidence
            .get_or_insert_with(|| RequestPathRateEvidence::new(observed_at))
    }

    pub(in crate::runtime) fn per_flow_rate(&self) -> Option<RequestPerFlowRateModel> {
        self.per_flow_rate
    }

    pub(in crate::runtime) fn set_per_flow_rate(&mut self, model: RequestPerFlowRateModel) {
        self.per_flow_rate = Some(model);
    }

    pub(in crate::runtime) fn product_delivery_proven(&self) -> bool {
        self.product_delivery_proven
    }

    pub(in crate::runtime) fn mark_product_delivery_proven(&mut self) -> bool {
        !std::mem::replace(&mut self.product_delivery_proven, true)
    }

    pub(in crate::runtime) fn ack_clock_first_window(&self) -> bool {
        self.ack_clock_first_window
    }

    pub(in crate::runtime) fn mark_ack_clock_first_window(&mut self) -> bool {
        !std::mem::replace(&mut self.ack_clock_first_window, true)
    }

    pub(in crate::runtime) fn ack_clock_proven(&self) -> bool {
        self.ack_clock_proven
    }

    pub(in crate::runtime) fn mark_ack_clock_proven(&mut self) -> bool {
        !std::mem::replace(&mut self.ack_clock_proven, true)
    }

    pub(in crate::runtime) fn ack_clock_measurement_bytes(&self) -> Option<u64> {
        self.ack_clock_measurement_bytes
    }

    pub(in crate::runtime) fn set_ack_clock_measurement_bytes(&mut self, bytes: u64) {
        self.ack_clock_measurement_bytes = Some(bytes);
    }

    pub(in crate::runtime) fn ack_clock_measurement_target(&self) -> Option<u64> {
        self.ack_clock_measurement_target
    }

    pub(in crate::runtime) fn set_ack_clock_measurement_target(&mut self, bytes: u64) {
        self.ack_clock_measurement_target = Some(bytes);
    }

    pub(in crate::runtime) fn tcp_capacity_proven(&self) -> bool {
        self.tcp_capacity_proven
    }

    pub(in crate::runtime) fn mark_tcp_capacity_proven(&mut self) {
        self.tcp_capacity_proven = true;
    }

    pub(in crate::runtime) fn clear_tcp_capacity_proven(&mut self) {
        self.tcp_capacity_proven = false;
    }

    pub(in crate::runtime) fn capacity_admitted(&self) -> bool {
        self.capacity_admitted
    }

    pub(in crate::runtime) fn mark_capacity_admitted(&mut self) {
        self.capacity_admitted = true;
    }

    pub(in crate::runtime) fn has_product_evidence(&self) -> bool {
        self.rate_evidence.is_some()
            || self.per_flow_rate.is_some()
            || self.product_delivery_proven
            || self.ack_clock_proven
    }

    /// Revoke TCP admission evidence while retaining a completed flow model.
    ///
    /// A flow model is receiver-proven product history; carrier proof expiry
    /// must not erase it. All incomplete measurement authority is discarded.
    pub(in crate::runtime) fn revoke_tcp_capacity(&mut self) {
        self.tcp_capacity_proven = false;
        self.capacity_admitted = false;
        self.product_delivery_proven = false;
        self.ack_clock_first_window = false;
        self.ack_clock_proven = false;
        self.rate_evidence = None;
        self.ack_clock_measurement_bytes = None;
        self.ack_clock_measurement_target = None;
    }
}

/// Exact-instance path_state records for one request stream.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestPathStates {
    entries: HashMap<RelayPathInstance, RequestPathState>,
}

impl RequestPathStates {
    pub(in crate::runtime) fn get(&self, instance: RelayPathInstance) -> Option<&RequestPathState> {
        self.entries.get(&instance)
    }

    pub(in crate::runtime) fn get_mut(
        &mut self,
        instance: RelayPathInstance,
    ) -> &mut RequestPathState {
        self.entries.entry(instance).or_default()
    }

    pub(in crate::runtime) fn get_existing_mut(
        &mut self,
        instance: RelayPathInstance,
    ) -> Option<&mut RequestPathState> {
        self.entries.get_mut(&instance)
    }

    pub(in crate::runtime) fn retain_live(&mut self, live: &HashSet<RelayPathInstance>) {
        self.entries.retain(|instance, _| live.contains(instance));
    }

    pub(in crate::runtime) fn iter(
        &self,
    ) -> impl Iterator<Item = (RelayPathInstance, &RequestPathState)> {
        self.entries
            .iter()
            .map(|(instance, state)| (*instance, state))
    }
}

/// Single-task request product state.
///
/// The client relay serializes this aggregate, so request offsets, evidence,
/// path evidence and reinjection history stay lock-free. Per-path evidence is
/// keyed once in `path_states`, preventing partial membership cleanup.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestStreamState {
    pub(in crate::runtime) flights: RequestFlightLedger,
    pub(in crate::runtime) path_states: RequestPathStates,
    pub(in crate::runtime) ack_clock_operation: Option<RequestAckClockOperation>,
    pub(in crate::runtime) membership_generation: Option<u64>,
    /// Exact paths with outstanding data but no Data ACK progress over the
    /// connection-level persistence interval. Native path recovery continues.
    pub(in crate::runtime) stale_paths: HashSet<RelayPathInstance>,
    pub(in crate::runtime) reinjection_attempts: HashMap<RelayPathInstance, Instant>,
}
