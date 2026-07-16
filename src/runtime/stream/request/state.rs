//! Request path evidence and serialized stream state.
//!
//! Exact attachment instances fence measurement state across reconnects. The
//! client relay serializes this aggregate, so it remains lock-free.

use super::flight::RequestFlightLedger;
use crate::model::capacity::reliable_path_startup_sample_limit_bytes;
use crate::model::path::RelayPathInstance;
use crate::model::request_evidence::{RequestPathRateEvidence, RequestPerFlowRateModel};
use crate::mux::MuxLimits;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// One bounded product-data sample used to measure a previously unmeasured path.
///
/// The exact flight ledger still owns product offsets. This state only limits
/// how much unique data may be exposed while TCP or QUIC feedback establishes
/// receiver-observed goodput for the path.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RequestPathSample {
    path: RelayPathInstance,
    sent_bytes: u64,
    limit_bytes: u64,
    sealed_bytes: Option<u64>,
}

impl RequestPathSample {
    pub(in crate::runtime) fn path(self) -> RelayPathInstance {
        self.path
    }

    pub(in crate::runtime) fn sealed_bytes(self) -> Option<u64> {
        self.sealed_bytes
    }

    pub(in crate::runtime) fn is_sealed(self) -> bool {
        self.sealed_bytes.is_some()
    }

    pub(in crate::runtime) fn can_extend(self, payload_bytes: usize) -> bool {
        self.sealed_bytes.is_none()
            && self.sent_bytes.saturating_add(payload_bytes as u64) <= self.limit_bytes
    }
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestPathSamplingState {
    sample: Option<RequestPathSample>,
    paths: HashMap<RelayPathInstance, RequestStartupPathEvidence>,
    pub(in crate::runtime) sampled_paths: HashSet<RelayPathInstance>,
}

#[derive(Debug, Default)]
struct RequestStartupPathEvidence {
    acked_bytes: u64,
    first_sent_at: Option<Instant>,
    // Transaction-local completion for this startup sample. Durable path
    // scheduling evidence lives in RequestPathState after capacity_admission.
    sample_rate_proven: bool,
    receipt_proof: Option<(u64, u64)>,
}

#[derive(Debug)]
pub(in crate::runtime) struct RequestPathSampleCommit {
    next_sample: RequestPathSample,
    candidate: RelayPathInstance,
}

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
    rate_proven: bool,
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

    pub(in crate::runtime) fn rate_proven(&self) -> bool {
        self.rate_proven
    }

    pub(in crate::runtime) fn mark_rate_proven(&mut self) -> bool {
        !std::mem::replace(&mut self.rate_proven, true)
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

    pub(in crate::runtime) fn clear_capacity_admitted(&mut self) {
        self.capacity_admitted = false;
    }

    pub(in crate::runtime) fn has_product_evidence(&self) -> bool {
        self.rate_evidence.is_some()
            || self.per_flow_rate.is_some()
            || self.rate_proven
            || self.ack_clock_proven
    }

    /// Revoke TCP admission evidence while retaining a completed flow model.
    ///
    /// A flow model is receiver-proven product history; carrier proof expiry
    /// must not erase it. All incomplete measurement authority is discarded.
    pub(in crate::runtime) fn revoke_tcp_capacity(&mut self) {
        self.tcp_capacity_proven = false;
        self.capacity_admitted = false;
        self.rate_proven = false;
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
    pub(in crate::runtime) path_sampling: RequestPathSamplingState,
    pub(in crate::runtime) path_states: RequestPathStates,
    pub(in crate::runtime) ack_clock_operation: Option<RequestAckClockOperation>,
    pub(in crate::runtime) membership_generation: Option<u64>,
    /// Exact paths with outstanding data but no Data ACK progress over the
    /// connection-level persistence interval. Native path recovery continues.
    pub(in crate::runtime) stale_paths: HashSet<RelayPathInstance>,
    pub(in crate::runtime) reinjection_attempts: HashMap<RelayPathInstance, Instant>,
}

impl RequestPathSamplingState {
    pub(in crate::runtime) fn sample(&self) -> Option<RequestPathSample> {
        self.sample
    }

    pub(in crate::runtime) fn record_first_sent_at(
        &mut self,
        instance: RelayPathInstance,
        sent_at: Instant,
    ) {
        let first_sent_at = &mut self.paths.entry(instance).or_default().first_sent_at;
        *first_sent_at = Some(first_sent_at.map_or(sent_at, |current| current.min(sent_at)));
    }

    pub(in crate::runtime) fn record_acked(
        &mut self,
        instance: RelayPathInstance,
        bytes: usize,
        sent_at: Instant,
    ) {
        self.record_first_sent_at(instance, sent_at);
        let evidence = self.paths.entry(instance).or_default();
        evidence.acked_bytes = evidence.acked_bytes.saturating_add(bytes as u64);
    }

    pub(in crate::runtime) fn acked_sample(
        &self,
        instance: RelayPathInstance,
    ) -> Option<(u64, Instant)> {
        let evidence = self.paths.get(&instance)?;
        Some((evidence.acked_bytes, evidence.first_sent_at?))
    }

    pub(in crate::runtime) fn first_sent_at(&self, instance: RelayPathInstance) -> Option<Instant> {
        self.paths
            .get(&instance)
            .and_then(|evidence| evidence.first_sent_at)
    }

    pub(in crate::runtime) fn sample_rate_proven(&self, instance: RelayPathInstance) -> bool {
        self.paths
            .get(&instance)
            .is_some_and(|evidence| evidence.sample_rate_proven)
    }

    pub(in crate::runtime) fn mark_sample_rate_proven(
        &mut self,
        instance: RelayPathInstance,
    ) -> bool {
        !std::mem::replace(
            &mut self.paths.entry(instance).or_default().sample_rate_proven,
            true,
        )
    }

    pub(in crate::runtime) fn receipt_proof(
        &self,
        instance: RelayPathInstance,
    ) -> Option<(u64, u64)> {
        self.paths
            .get(&instance)
            .and_then(|evidence| evidence.receipt_proof)
    }

    pub(in crate::runtime) fn set_receipt_proof(
        &mut self,
        instance: RelayPathInstance,
        proof: (u64, u64),
    ) {
        self.paths.entry(instance).or_default().receipt_proof = Some(proof);
    }

    pub(in crate::runtime) fn clear_receipt_proof(&mut self, instance: RelayPathInstance) {
        if let Some(evidence) = self.paths.get_mut(&instance) {
            evidence.receipt_proof = None;
        }
    }

    pub(in crate::runtime) fn clear_path(&mut self, instance: RelayPathInstance) {
        self.paths.remove(&instance);
    }

    pub(in crate::runtime) fn plan_sample(
        &self,
        mux_limits: MuxLimits,
        candidate: RelayPathInstance,
        payload_bytes: usize,
    ) -> Option<RequestPathSampleCommit> {
        let limit_bytes = usize::try_from(reliable_path_startup_sample_limit_bytes(mux_limits))
            .unwrap_or(usize::MAX);
        if payload_bytes > limit_bytes {
            return None;
        }
        let mut next_sample = match self.sample {
            Some(sample) if sample.path == candidate && sample.can_extend(payload_bytes) => sample,
            Some(_) => return None,
            None if self.sampled_paths.contains(&candidate) => return None,
            None => RequestPathSample {
                path: candidate,
                sent_bytes: 0,
                limit_bytes: limit_bytes as u64,
                sealed_bytes: None,
            },
        };
        next_sample.sent_bytes = next_sample.sent_bytes.saturating_add(payload_bytes as u64);
        if next_sample.sent_bytes >= next_sample.limit_bytes {
            next_sample.sealed_bytes = Some(next_sample.sent_bytes);
        }
        Some(RequestPathSampleCommit {
            next_sample,
            candidate,
        })
    }

    pub(in crate::runtime) fn commit_sample(&mut self, commit: RequestPathSampleCommit) {
        self.sample = Some(commit.next_sample);
        self.sampled_paths.insert(commit.candidate);
    }

    pub(in crate::runtime) fn seal_if_next_frame_exceeds_limit(
        &mut self,
        payload_bytes: usize,
    ) -> Option<RelayPathInstance> {
        let sample = self.sample.as_mut()?;
        if sample.is_sealed()
            || sample.sent_bytes < crate::model::capacity::MIN_RATE_SAMPLE_BYTES
            || sample.can_extend(payload_bytes)
        {
            return None;
        }
        sample.sealed_bytes = Some(sample.sent_bytes);
        Some(sample.path)
    }

    pub(in crate::runtime) fn cancel_sample(&mut self, path: RelayPathInstance) {
        if self.sample.is_some_and(|sample| sample.path == path) {
            self.sample = None;
        }
        self.clear_path(path);
    }

    pub(in crate::runtime) fn complete_sample(&mut self, path: RelayPathInstance) -> bool {
        if !self.sample.is_some_and(|sample| sample.path == path) {
            return false;
        }
        self.sample = None;
        true
    }

    pub(in crate::runtime) fn retain_live(&mut self, live: &HashSet<RelayPathInstance>) {
        if self
            .sample
            .is_some_and(|sample| !live.contains(&sample.path))
        {
            self.sample = None;
        }
        self.sampled_paths
            .retain(|instance| live.contains(instance));
        self.paths.retain(|instance, _| live.contains(instance));
    }
}

#[cfg(test)]
#[path = "state_test.rs"]
mod tests;
