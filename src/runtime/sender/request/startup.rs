//! Request startup-subflow evidence and admission transaction.
//!
//! Selection may plan against a cloned epoch, but membership is committed only
//! after the carrier accepts the first owner frame. Failed/stale enqueues leave
//! no attempted marker or partial startup state to roll back.

use super::*;

#[derive(Debug, Default)]
pub(super) struct RequestStartupState {
    pub(super) epoch: Option<FlowSubflowSet<RelayPathInstance>>,
    pub(super) acked_bytes: HashMap<RelayPathInstance, u64>,
    pub(super) first_sent_at: HashMap<RelayPathInstance, Instant>,
    pub(super) rate_evidence: HashSet<RelayPathInstance>,
    pub(super) receipt_proofs: HashMap<RelayPathInstance, (u64, u64)>,
    pub(super) attempted_subflows: HashSet<RelayPathInstance>,
}

#[derive(Debug)]
pub(super) struct RequestStartupAdmission {
    next_epoch: FlowSubflowSet<RelayPathInstance>,
    candidate: RelayPathInstance,
}

impl RequestStartupState {
    pub(super) fn plan_admission(
        &self,
        mux_limits: MuxLimits,
        service: RelayPathInstance,
        candidate: RelayPathInstance,
        payload_bytes: usize,
    ) -> Option<RequestStartupAdmission> {
        if service.key.underlay != UnderlayProtocol::Tcp
            || candidate.key.underlay != UnderlayProtocol::Tcp
        {
            return None;
        }
        let startup_credit =
            usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits))
                .unwrap_or(usize::MAX);
        let mut next_epoch = self
            .epoch
            .as_ref()
            .filter(|epoch| epoch.matches_envelope(service, startup_credit, 0, Duration::ZERO))
            .cloned()
            .unwrap_or_else(|| FlowSubflowSet::new(0, service, startup_credit, 0, Duration::ZERO));
        let input = SubflowAdmissionInput {
            key: candidate,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        };
        (next_epoch.admit_subflow_owner(input).decision == PathAdmissionDecision::AdmitSubflow)
            .then_some(RequestStartupAdmission {
                next_epoch,
                candidate,
            })
    }

    pub(super) fn commit_admission(&mut self, admission: RequestStartupAdmission) {
        self.epoch = Some(admission.next_epoch);
        self.attempted_subflows.insert(admission.candidate);
    }

    pub(super) fn reset_epoch(&mut self) {
        self.epoch = None;
        self.acked_bytes.clear();
        self.first_sent_at.clear();
        self.rate_evidence.clear();
        self.receipt_proofs.clear();
    }

    pub(super) fn retain_live(&mut self, live: &HashSet<RelayPathInstance>) {
        self.attempted_subflows
            .retain(|instance| live.contains(instance));
        self.acked_bytes
            .retain(|instance, _| live.contains(instance));
        self.first_sent_at
            .retain(|instance, _| live.contains(instance));
        self.rate_evidence
            .retain(|instance| live.contains(instance));
        self.receipt_proofs
            .retain(|instance, _| live.contains(instance));
    }
}

#[cfg(test)]
mod tests;
