//! QUIC datagram path selection, pacing, and failover.

use super::DatagramSessionEvent;
use super::UDP_PATH_HANDSHAKE_TIMEOUT;
use super::association::DatagramPathSend;
#[cfg(test)]
use super::association::runtime_error_is_datagram_response_timeout;
use super::policy::{DatagramPathSendError, datagram_remaining_ttl_ms};
use super::quic_session::{UdpDatagramClientSession, open_udp_datagram_session_on_path};
use crate::model::capacity::{QUIC_PERSISTENT_CONGESTION_THRESHOLD, QUIC_TIMER_GRANULARITY};
use crate::model::path::{CarrierPathInstanceId, RelayPathKey};
use crate::model::timing::default_transport_pto;
use crate::protocol::{DatagramFlowId, DatagramId, Frame, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::model::{
    UdpPathRuntimeModel, path_is_endpoint_only, udp_observation_has_datagram_feedback,
};
use crate::runtime::path::quic::client::{ClientUdpErrorDisposition, client_udp_error_disposition};
use crate::runtime::path::{ClientPathContext, RelayPathLoadLease, UdpPathCandidate};
use crate::scheduler::{PathSnapshot, path_within_adaptive_lead_hysteresis};
use crate::transport::RateHint;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

pub(in crate::runtime) struct UdpDatagramClientAssociation {
    pub(in crate::runtime) context: ClientPathContext,
    pub(in crate::runtime) paths: Vec<UdpDatagramAssociationPath>,
    pub(in crate::runtime) suppressed_paths: HashMap<usize, Instant>,
    pub(in crate::runtime) last_successful_path: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct UdpAssociationCandidateScore {
    path_index: usize,
    completion_ms: f64,
    eta_ms: f64,
    opens_new_session: bool,
    rank: usize,
    snapshot: PathSnapshot,
}

fn udp_association_candidate_order(
    left: &UdpAssociationCandidateScore,
    right: &UdpAssociationCandidateScore,
) -> std::cmp::Ordering {
    left.completion_ms
        .total_cmp(&right.completion_ms)
        .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
        .then_with(|| left.opens_new_session.cmp(&right.opens_new_session))
        .then_with(|| left.rank.cmp(&right.rank))
}

pub(in crate::runtime) struct UdpDatagramAssociationPath {
    pub(in crate::runtime) session: UdpDatagramClientSession,
    pub(in crate::runtime) pacer: UdpDatagramPacer,
    _load_lease: RelayPathLoadLease,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct UdpDatagramPacer {
    pub(in crate::runtime) next_send_at: Instant,
}

impl UdpDatagramPacer {
    fn new() -> Self {
        Self {
            next_send_at: Instant::now(),
        }
    }

    pub(in crate::runtime) fn ready_at(self) -> Instant {
        self.next_send_at
    }

    pub(in crate::runtime) async fn wait_for_send(
        &mut self,
        model: UdpPathRuntimeModel,
        payload_bytes: usize,
    ) {
        let now = Instant::now();
        if self.next_send_at > now {
            tokio::time::sleep(self.next_send_at.duration_since(now)).await;
        }
        self.next_send_at = Instant::now() + model.pacing_interval(payload_bytes);
    }
}

impl UdpDatagramClientAssociation {
    pub(in crate::runtime) fn new(context: ClientPathContext) -> Self {
        Self {
            context,
            paths: Vec::new(),
            suppressed_paths: HashMap::new(),
            last_successful_path: None,
        }
    }

    pub(super) async fn send_to_path_index(
        &mut self,
        path_index: usize,
        send: DatagramPathSend,
    ) -> Result<(), DatagramPathSendError> {
        let product_deadline = send.product_deadline;
        let result = self.send_to_path(path_index, send).await;
        match result {
            Ok(()) => {
                self.last_successful_path = Some(path_index);
                Ok(())
            }
            Err(DatagramPathSendError::Runtime(source)) => {
                let settlement_owner = self
                    .paths
                    .iter()
                    .find(|path| path.session.path_index == path_index)
                    .map(|path| path.session.error_settlement_owner());
                let disposition = client_udp_error_disposition(&source);
                if let Some((owner, path_instance_id)) = settlement_owner {
                    owner
                        .settle_established_disposition(path_instance_id, disposition)
                        .await;
                }
                // Every runtime send failure ends this Product request. The
                // typed disposition decides only whether the exact physical
                // owner may also be retired; dropping the association path
                // releases its logical lease while preserving a live carrier
                // for a later fresh request.
                let _ = self.remove_path(path_index);
                if disposition == ClientUdpErrorDisposition::CarrierLifetime {
                    self.suppress_path_after_carrier_failure(
                        path_index,
                        default_transport_pto(),
                        datagram_remaining_ttl_ms(product_deadline),
                    );
                }
                Err(DatagramPathSendError::runtime(source))
            }
            Err(DatagramPathSendError::UdpPathOpen(source)) => {
                Err(DatagramPathSendError::runtime(source))
            }
            Err(error) => Err(error),
        }
    }

    pub(in crate::runtime) fn feedback_timeout(&self, path_index: usize, ttl_ms: u32) -> Duration {
        self.context
            .udp_path_runtime_model(path_index, ttl_ms)
            .map(|model| model.response_timeout)
            .unwrap_or_else(|| Duration::from_millis(u64::from(ttl_ms)))
    }

    pub(in crate::runtime) fn ranked_path_candidates(
        &mut self,
        payload_bytes: usize,
        ttl_ms: u32,
    ) -> Vec<UdpPathCandidate> {
        self.prune_suppressed_paths();
        let candidates = self
            .context
            .ordered_udp_path_candidates_for_ttl(payload_bytes, ttl_ms);
        let mut attempted = HashSet::new();
        let mut ranked = Vec::with_capacity(candidates.len());
        while let Some(path_index) =
            self.select_path_candidate(&candidates, &attempted, payload_bytes, ttl_ms)
        {
            attempted.insert(path_index);
            if let Some(candidate) = candidates
                .iter()
                .find(|candidate| candidate.path_index == path_index)
            {
                ranked.push(*candidate);
            }
        }
        ranked
    }

    pub(in crate::runtime) async fn close(&mut self) -> Result<(), RuntimeError> {
        let mut close_error = None;
        while let Some(mut path) = self.paths.pop() {
            let close_result = path.session.close().await;
            if close_error.is_none()
                && let Err(err) = close_result
            {
                close_error = Some(err);
            }
        }
        match close_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub(in crate::runtime) fn has_open_path(&self) -> bool {
        !self.paths.is_empty()
    }

    pub(in crate::runtime) async fn next_frame(
        &mut self,
    ) -> Result<(usize, Result<Frame, RuntimeError>), RuntimeError> {
        if self.paths.is_empty() {
            return Err(RuntimeError::NoSchedulableUdpPath);
        }
        let waits = self
            .paths
            .iter_mut()
            .map(|path| {
                let path_index = path.session.path_index;
                Box::pin(async move { (path_index, path.session.next_frame().await) })
                    as Pin<
                        Box<dyn Future<Output = (usize, Result<Frame, RuntimeError>)> + Send + '_>,
                    >
            })
            .collect::<Vec<_>>();
        let (event, _, remaining) = futures::future::select_all(waits).await;
        drop(remaining);
        Ok(event)
    }

    pub(in crate::runtime) async fn handle_frame(
        &mut self,
        path_index: usize,
        frame: Frame,
    ) -> Result<DatagramSessionEvent, RuntimeError> {
        let Some(position) = self
            .paths
            .iter()
            .position(|path| path.session.path_index == path_index)
        else {
            return Ok(DatagramSessionEvent::Control);
        };
        let path_instance_id = self.paths[position].session.path_instance_id();
        let result = self.paths[position].session.handle_frame(frame).await;
        if let Some(observation) = self.paths[position].session.take_feedback_observation() {
            self.context.mark_udp_path_feedback_for_instance(
                path_index,
                path_instance_id,
                observation,
            );
        }
        if let Err(source) = &result {
            let disposition = client_udp_error_disposition(source);
            let (owner, owner_instance_id) = self.paths[position].session.error_settlement_owner();
            let _ = owner
                .settle_established_disposition(owner_instance_id, disposition)
                .await;
            let _ = self.remove_path(path_index);
        }
        result
    }

    pub(in crate::runtime) async fn acknowledge(
        &mut self,
        path_index: usize,
        flow_id: DatagramFlowId,
        datagram_id: DatagramId,
    ) -> Result<(), RuntimeError> {
        let path = self
            .paths
            .iter_mut()
            .find(|path| path.session.path_index == path_index)
            .ok_or(RuntimeError::NoSchedulableUdpPath)?;
        path.session.acknowledge(flow_id, datagram_id).await
    }

    pub(in crate::runtime) fn has_flow(&self, flow_id: DatagramFlowId) -> bool {
        self.paths.iter().any(|path| path.session.has_flow(flow_id))
    }

    pub(in crate::runtime) async fn handle_receive_error(
        &mut self,
        path_index: usize,
        disposition: ClientUdpErrorDisposition,
    ) -> ClientUdpErrorDisposition {
        let settlement_owner = self
            .paths
            .iter()
            .find(|path| path.session.path_index == path_index)
            .map(|path| path.session.error_settlement_owner());
        if let Some((owner, path_instance_id)) = settlement_owner {
            owner
                .settle_established_disposition(path_instance_id, disposition)
                .await;
        }
        let _ = self.remove_path(path_index);
        disposition
    }

    pub(in crate::runtime) fn select_path_candidate(
        &self,
        candidates: &[UdpPathCandidate],
        attempted: &HashSet<usize>,
        payload_bytes: usize,
        ttl_ms: u32,
    ) -> Option<usize> {
        let now = Instant::now();
        let freshness_budget_ms = f64::from(ttl_ms);
        let mut viable = candidates
            .iter()
            .enumerate()
            .filter(|(_, candidate)| !attempted.contains(&candidate.path_index))
            .filter_map(|(rank, candidate)| {
                let open_ready_at = self
                    .paths
                    .iter()
                    .find(|path| path.session.path_index == candidate.path_index)
                    .map(|path| path.pacer.ready_at());
                let has_open_session = open_ready_at.is_some();
                let eta_ms = self.context.udp_path_eta_for_ttl(
                    candidate.path_index,
                    payload_bytes,
                    ttl_ms,
                    has_open_session,
                )?;
                let model = self
                    .context
                    .udp_path_runtime_model(candidate.path_index, ttl_ms)?;
                if !model.accepts_payload(payload_bytes) {
                    return None;
                }
                let snapshot = self.context.udp_path_snapshot(candidate.path_index)?;
                let ready_at = open_ready_at.unwrap_or(now);
                let ready_delay_ms = ready_at.saturating_duration_since(now).as_secs_f64() * 1000.0;
                let completion_ms = eta_ms + ready_delay_ms;
                (completion_ms <= freshness_budget_ms).then_some(UdpAssociationCandidateScore {
                    path_index: candidate.path_index,
                    completion_ms,
                    eta_ms,
                    opens_new_session: !has_open_session,
                    rank,
                    snapshot,
                })
            })
            .collect::<Vec<_>>();
        let evidenced_paths = viable
            .iter()
            .filter_map(|candidate| {
                self.path_has_datagram_feedback_or_hint(candidate.path_index)
                    .then_some(candidate.path_index)
            })
            .collect::<Vec<_>>();
        if evidenced_paths
            .iter()
            .any(|path_index| !self.path_is_temporarily_suppressed(*path_index, now))
        {
            viable.retain(|candidate| evidenced_paths.contains(&candidate.path_index));
        }
        let best_candidate = viable
            .iter()
            .min_by(|left, right| udp_association_candidate_order(left, right))
            .copied();
        if let Some(path_index) = self.last_successful_path
            && let Some(best) = best_candidate
            && let Some(candidate) = viable.iter().find(|candidate| {
                candidate.path_index == path_index
                    && !self.path_is_temporarily_suppressed(candidate.path_index, now)
            })
            && path_within_adaptive_lead_hysteresis(
                candidate.completion_ms,
                candidate.snapshot,
                best.completion_ms,
                best.snapshot,
                payload_bytes,
            )
        {
            return Some(candidate.path_index);
        }
        if evidenced_paths.is_empty()
            && self.context.udp_paths.iter().all(path_is_endpoint_only)
            && let Some(candidate) = viable
                .iter()
                .filter(|candidate| !self.path_is_temporarily_suppressed(candidate.path_index, now))
                .min_by(|left, right| left.path_index.cmp(&right.path_index))
        {
            return Some(candidate.path_index);
        }
        let has_unsuppressed = viable
            .iter()
            .any(|candidate| !self.path_is_temporarily_suppressed(candidate.path_index, now));
        if has_unsuppressed {
            viable.retain(|candidate| {
                !self.path_is_temporarily_suppressed(candidate.path_index, now)
            });
        }
        viable
            .into_iter()
            .min_by(udp_association_candidate_order)
            .map(|candidate| candidate.path_index)
    }

    pub(in crate::runtime) fn suppress_path_after_carrier_failure(
        &mut self,
        path_index: usize,
        recovery_interval: Duration,
        ttl_ms: u32,
    ) {
        let ttl = Duration::from_millis(u64::from(ttl_ms));
        let duration = recovery_interval
            .max(QUIC_TIMER_GRANULARITY)
            .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD)
            .min(ttl);
        self.suppressed_paths
            .insert(path_index, Instant::now() + duration);
    }

    fn prune_suppressed_paths(&mut self) {
        let now = Instant::now();
        self.suppressed_paths
            .retain(|_, suppressed_until| *suppressed_until > now);
    }

    fn path_is_temporarily_suppressed(&self, path_index: usize, now: Instant) -> bool {
        self.suppressed_paths
            .get(&path_index)
            .is_some_and(|suppressed_until| *suppressed_until > now)
    }

    fn path_has_datagram_feedback_or_hint(&self, path_index: usize) -> bool {
        let Some(path) = self.context.udp_paths.get(path_index) else {
            return false;
        };
        let now = Instant::now();
        let Some(observation) = self
            .context
            .health()
            .lock()
            .expect("client path health lock")
            .udp
            .get(path_index)
            .map(|record| record.observation_at(now))
        else {
            return false;
        };
        udp_observation_has_datagram_feedback(&observation)
            || path.metadata.initial_srtt_ms.is_some()
            || path.metadata.initial_jitter_ms.is_some()
            || path.metadata.initial_rate != RateHint::Unknown
    }

    async fn send_to_path(
        &mut self,
        path_index: usize,
        send: DatagramPathSend,
    ) -> Result<(), DatagramPathSendError> {
        let DatagramPathSend {
            target,
            flow_id,
            datagram_id,
            payload,
            setup_deadline: fallback_deadline,
            product_deadline,
            has_unattempted_alternative,
        } = send;
        let ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        if ttl_ms == 0 {
            return Err(DatagramPathSendError::Timeout);
        }
        let model = self
            .context
            .udp_path_runtime_model(path_index, ttl_ms)
            .ok_or_else(|| DatagramPathSendError::runtime(RuntimeError::NoSchedulableUdpPath))?;
        if !model.accepts_payload(payload.len()) {
            return Err(DatagramPathSendError::PayloadLimitExceeded {
                limit: model.max_payload_bytes,
            });
        }
        let association_had_open_path = !self.paths.is_empty();
        let open_started_at = tokio::time::Instant::now();
        let handshake_timeout = udp_datagram_path_open_timeout(
            association_had_open_path,
            has_unattempted_alternative,
            model,
            ttl_ms,
        )
        .min(fallback_deadline.saturating_duration_since(open_started_at));
        let open_deadline = (open_started_at + handshake_timeout).min(fallback_deadline);
        let position = self
            .ensure_path_session(path_index, open_deadline)
            .await
            .map_err(DatagramPathSendError::UdpPathOpen)?;
        let (observation_path_index, observation_path_instance_id, observation, result) = {
            let path = self.paths.get_mut(position).ok_or_else(|| {
                DatagramPathSendError::runtime(RuntimeError::NoSchedulableUdpPath)
            })?;
            if tokio::time::timeout_at(
                fallback_deadline,
                path.pacer.wait_for_send(model, payload.len()),
            )
            .await
            .is_err()
            {
                return Err(DatagramPathSendError::Timeout);
            }
            let result = path
                .session
                .send_to(
                    target,
                    flow_id,
                    datagram_id,
                    payload,
                    fallback_deadline,
                    product_deadline,
                )
                .await;
            let observation = path.session.take_feedback_observation();
            (
                path.session.path_index,
                path.session.path_instance_id(),
                observation,
                result,
            )
        };
        if let Some(observation) = observation {
            self.context.mark_udp_path_feedback_for_instance(
                observation_path_index,
                observation_path_instance_id,
                observation,
            );
        }

        match result {
            Ok(()) => Ok(()),
            Err(DatagramPathSendError::Timeout) => Err(DatagramPathSendError::Timeout),
            Err(err) => Err(err),
        }
    }

    async fn ensure_path_session(
        &mut self,
        path_index: usize,
        open_deadline: tokio::time::Instant,
    ) -> Result<usize, RuntimeError> {
        if let Some(position) = self
            .paths
            .iter()
            .position(|path| path.session.path_index == path_index)
        {
            return Ok(position);
        }
        // Logical Product load is reserved before carrier I/O and owned by
        // this exact association-path value. Physical carrier replacement may
        // change telemetry identity, but cannot create or release this load.
        let load_lease = self
            .context
            .reserve_relay_path_load(
                RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index: path_index,
                },
                crate::scheduler::TrafficClass::RealtimeDatagram,
            )
            .ok_or(RuntimeError::NoSchedulableUdpPath)?;
        let session =
            open_udp_datagram_session_on_path(&self.context, path_index, open_deadline).await?;
        self.paths.push(UdpDatagramAssociationPath {
            session,
            pacer: UdpDatagramPacer::new(),
            _load_lease: load_lease,
        });
        Ok(self.paths.len() - 1)
    }

    fn remove_path(&mut self, path_index: usize) -> Option<CarrierPathInstanceId> {
        let position = self
            .paths
            .iter()
            .position(|path| path.session.path_index == path_index)?;
        let path = self.paths.swap_remove(position);
        let path_instance_id = path.session.path_instance_id();
        Some(path_instance_id)
    }
}

#[cfg(test)]
pub(in crate::runtime) fn udp_datagram_error_is_path_retryable(err: &RuntimeError) -> bool {
    if runtime_error_is_datagram_response_timeout(err) {
        return false;
    }
    client_udp_error_disposition(err) == ClientUdpErrorDisposition::CarrierLifetime
}

pub(in crate::runtime) fn udp_datagram_path_open_timeout(
    association_has_open_path: bool,
    has_unattempted_alternative: bool,
    model: UdpPathRuntimeModel,
    ttl_ms: u32,
) -> Duration {
    let ttl_timeout = Duration::from_millis(u64::from(ttl_ms));
    if !association_has_open_path && !has_unattempted_alternative {
        return UDP_PATH_HANDSHAKE_TIMEOUT.min(ttl_timeout);
    }
    model
        .response_timeout
        .max(QUIC_TIMER_GRANULARITY)
        .min(UDP_PATH_HANDSHAKE_TIMEOUT)
        .min(ttl_timeout)
}
