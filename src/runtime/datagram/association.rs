//! Cross-underlay product datagram selection and failover.

use super::policy::{
    DatagramPathSendError, datagram_feedback_retry_budget, datagram_remaining_ttl_ms,
};
use super::quic::UdpDatagramClientAssociation;
use super::tcp::{TcpDatagramClientAssociation, tcp_datagram_path_open_timeout};
use super::{DatagramSessionEvent, ReceivedDatagram};
use crate::model::datagram::{DatagramAdmission, DatagramPayloadIdentity, DatagramReceiveWindow};
use crate::model::path::RelayPathKey;
use crate::mux::datagram::DatagramError;
use crate::protocol::{DatagramFlowId, DatagramId, Frame, TargetAddr, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::quic::client::{ClientUdpErrorDisposition, client_udp_error_disposition};
use crate::runtime::path::tcp::client::ClientTcpDatagramInbound;
use crate::runtime::path::{ClientPathContext, ClientSessionProductFlowLease};
use crate::runtime::telemetry::{ProductFlowCounter, ProductFlowLease};
use crate::scheduler::{TrafficClass, path_is_backup};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::time::Duration;

const MAX_DATAGRAM_EMISSIONS: usize = 2;

pub(in crate::runtime) struct DatagramClientAssociation {
    context: ClientPathContext,
    udp: Option<Box<UdpDatagramClientAssociation>>,
    tcp: Option<Box<TcpDatagramClientAssociation>>,
    product_flows: Vec<DatagramClientProductFlow>,
    pending: BTreeMap<(u64, u64), PendingProductDatagram>,
    pending_bytes: usize,
}

/// Transport-only terminal owner produced before a Product datagram lifetime
/// releases admission or telemetry. Dropping this owner still drops QUIC
/// request streams and invokes the TCP attachment's carrier retirement lane.
pub(in crate::runtime) struct RetiringDatagramClientAssociation {
    association: DatagramClientAssociation,
}

struct DatagramClientProductFlow {
    target: TargetAddr,
    flow_id: DatagramFlowId,
    next_datagram_id: u64,
    received_responses: DatagramReceiveWindow,
    detached_since: Option<tokio::time::Instant>,
    _session_product_flow: ClientSessionProductFlowLease,
    telemetry_flow: ProductFlowLease,
    telemetry_counter: ProductFlowCounter,
}

struct PendingProductDatagram {
    target: TargetAddr,
    payload: Bytes,
    traffic_class: TrafficClass,
    expires_at: tokio::time::Instant,
    retry_at: tokio::time::Instant,
    attempted_paths: Vec<RelayPathKey>,
    emissions: usize,
}

#[derive(Clone)]
pub(super) struct DatagramPathSend {
    pub(super) target: TargetAddr,
    pub(super) flow_id: DatagramFlowId,
    pub(super) datagram_id: DatagramId,
    pub(super) payload: Bytes,
    pub(super) setup_deadline: tokio::time::Instant,
    pub(super) product_deadline: tokio::time::Instant,
    pub(super) has_unattempted_alternative: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct DatagramUnderlayCandidate {
    key: RelayPathKey,
    eta_ms: f64,
    underlay_rank: usize,
}

pub(in crate::runtime) enum DatagramClientCarrierFrame {
    Tcp {
        path_index: usize,
        frame: Result<ClientTcpDatagramInbound, RuntimeError>,
    },
    Udp {
        path_index: usize,
        frame: Result<Frame, RuntimeError>,
    },
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) enum DatagramClientReceipt {
    Tcp {
        path_index: usize,
        attachment_id: u64,
        flow_id: DatagramFlowId,
        datagram_id: DatagramId,
        write_deadline: tokio::time::Instant,
    },
    Udp {
        path_index: usize,
        flow_id: DatagramFlowId,
        datagram_id: DatagramId,
    },
}

pub(in crate::runtime) enum DatagramClientReceive {
    Control,
    Duplicate(DatagramClientReceipt),
    Deliver {
        target: TargetAddr,
        payload: Bytes,
        receipt: DatagramClientReceipt,
    },
}

impl DatagramClientAssociation {
    pub(in crate::runtime) async fn new(context: ClientPathContext) -> Result<Self, RuntimeError> {
        context.ensure_session_active()?;
        if context.udp_paths.is_empty() && context.tcp_paths.is_empty() {
            return Err(RuntimeError::NoDatagramPath);
        }
        Ok(Self {
            context,
            udp: None,
            tcp: None,
            product_flows: Vec::new(),
            pending: BTreeMap::new(),
            pending_bytes: 0,
        })
    }

    fn allocate_product_datagram(
        &mut self,
        target: TargetAddr,
    ) -> Result<(DatagramFlowId, DatagramId, ProductFlowCounter), RuntimeError> {
        let position = match self
            .product_flows
            .iter()
            .position(|flow| flow.target == target)
        {
            Some(position) => {
                self.renew_detached_flow_after_retention(position)?;
                position
            }
            None => {
                if self.product_flows.len() >= self.context.mux_limits.max_streams {
                    return Err(RuntimeError::Datagram(DatagramError::FlowLimitExceeded {
                        limit: self.context.mux_limits.max_streams,
                    }));
                }
                let flow_id = self.context.allocate_datagram_flow_id()?;
                let telemetry_flow = self.context.telemetry.open_datagram_flow(
                    Some(self.context.session_id),
                    flow_id,
                    target.clone(),
                );
                let telemetry_counter = telemetry_flow.counter();
                let session_product_flow = self.context.reserve_session_product_flow()?;
                self.product_flows.push(DatagramClientProductFlow {
                    target,
                    flow_id,
                    next_datagram_id: 0,
                    received_responses: DatagramReceiveWindow::new(
                        self.context.mux_limits.max_ack_ranges,
                    ),
                    detached_since: None,
                    _session_product_flow: session_product_flow,
                    telemetry_flow,
                    telemetry_counter,
                });
                self.product_flows.len() - 1
            }
        };
        let flow = self
            .product_flows
            .get_mut(position)
            .expect("datagram product flow position exists");
        let datagram_id = DatagramId(flow.next_datagram_id);
        flow.next_datagram_id = flow
            .next_datagram_id
            .checked_add(1)
            .ok_or(RuntimeError::Protocol("datagram ID overflow"))?;
        Ok((flow.flow_id, datagram_id, flow.telemetry_counter.clone()))
    }

    fn renew_detached_flow_after_retention(&mut self, position: usize) -> Result<(), RuntimeError> {
        let flow = self
            .product_flows
            .get(position)
            .expect("datagram product flow position exists");
        let expired = flow.detached_since.is_some_and(|detached_since| {
            detached_since.elapsed() >= self.context.session_retention_timeout
        });
        let has_pending = self
            .pending
            .keys()
            .any(|(flow_id, _)| *flow_id == flow.flow_id.0);
        if !expired || has_pending {
            return Ok(());
        }

        let flow_id = self.context.allocate_datagram_flow_id()?;
        let target = flow.target.clone();
        let telemetry_flow = self.context.telemetry.open_datagram_flow(
            Some(self.context.session_id),
            flow_id,
            target,
        );
        let telemetry_counter = telemetry_flow.counter();
        let flow = self
            .product_flows
            .get_mut(position)
            .expect("datagram product flow position exists");
        let previous_telemetry = std::mem::replace(&mut flow.telemetry_flow, telemetry_flow);
        previous_telemetry.complete();
        flow.flow_id = flow_id;
        flow.next_datagram_id = 0;
        flow.received_responses =
            DatagramReceiveWindow::new(self.context.mux_limits.max_ack_ranges);
        flow.detached_since = None;
        flow.telemetry_counter = telemetry_counter;
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::runtime) fn select_underlay(
        context: &ClientPathContext,
        payload_bytes: usize,
        ttl_ms: u32,
    ) -> Option<UnderlayProtocol> {
        datagram_underlay_candidates(
            context,
            payload_bytes,
            ttl_ms,
            TrafficClass::RealtimeDatagram,
        )
        .first()
        .map(|candidate| candidate.key.underlay)
    }

    #[cfg(test)]
    pub(in crate::runtime) async fn send_to_fresh_datagram_with_route_hint(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
        route_hint: Option<RelayPathKey>,
    ) -> Result<(), RuntimeError> {
        self.send_to_fresh_datagram_with_policy(
            target,
            payload,
            ttl_ms,
            route_hint,
            TrafficClass::RealtimeDatagram,
        )
        .await
    }

    pub(in crate::runtime) async fn send_to_fresh_datagram_with_policy(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
        route_hint: Option<RelayPathKey>,
        traffic_class: TrafficClass,
    ) -> Result<(), RuntimeError> {
        let context = self.context.clone();
        let result = context
            .complete_session_operation(self.send_to_fresh_datagram_with_policy_active(
                target,
                payload,
                ttl_ms,
                route_hint,
                traffic_class,
            ))
            .await;
        if matches!(&result, Err(RuntimeError::RemoteClosed(_))) {
            self.pending.clear();
            self.pending_bytes = 0;
        }
        result
    }

    async fn send_to_fresh_datagram_with_policy_active(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
        route_hint: Option<RelayPathKey>,
        traffic_class: TrafficClass,
    ) -> Result<(), RuntimeError> {
        self.prune_pending(tokio::time::Instant::now());
        let (flow_id, datagram_id, telemetry_counter) =
            self.allocate_product_datagram(target.clone())?;
        telemetry_counter.record_datagram_to_peer(payload.len() as u64);
        let expires_at = tokio::time::Instant::now() + Duration::from_millis(u64::from(ttl_ms));
        let mut candidates = self.ranked_underlay_candidates(payload.len(), ttl_ms, traffic_class);
        #[cfg(feature = "lab-diagnostics")]
        let requested_route_hint = route_hint;
        if let Some(route_hint) = route_hint
            && let Some(position) = candidates
                .iter()
                .position(|candidate| candidate.key == route_hint)
        {
            let hinted = candidates.remove(position);
            candidates.insert(0, hinted);
        }

        let mut attempted_paths = Vec::new();
        let mut last_error = None;
        for (position, candidate) in candidates.iter().copied().enumerate() {
            let remaining_ttl_ms = datagram_remaining_ttl_ms(expires_at);
            if remaining_ttl_ms == 0 {
                break;
            }
            let has_alternative = position + 1 < candidates.len();
            let setup_deadline =
                self.path_setup_deadline(candidate.key, has_alternative, expires_at);
            #[cfg(feature = "lab-diagnostics")]
            crate::lab_diagnostics::lab_diagnostic(
                "datagram_path_selected",
                format_args!(
                    "path_underlay={:?} path_index={} eta_ms={:.3} payload_bytes={} ttl_ms={} position={} candidate_count={} route_hint={:?}",
                    candidate.key.underlay,
                    candidate.key.index,
                    candidate.eta_ms,
                    payload.len(),
                    remaining_ttl_ms,
                    position,
                    candidates.len(),
                    requested_route_hint,
                ),
            );
            attempted_paths.push(candidate.key);
            #[cfg(test)]
            self.context.record_datagram_candidate_attempt_for_test();
            match self
                .send_on_path(
                    candidate.key,
                    DatagramPathSend {
                        target: target.clone(),
                        flow_id,
                        datagram_id,
                        payload: payload.clone(),
                        setup_deadline,
                        product_deadline: expires_at,
                        has_unattempted_alternative: has_alternative,
                    },
                )
                .await
            {
                Ok(()) => {
                    let feedback_timeout = self.feedback_timeout(candidate.key, remaining_ttl_ms);
                    let retry_budget =
                        datagram_feedback_retry_budget(feedback_timeout, remaining_ttl_ms, true);
                    self.retain_pending(
                        flow_id,
                        datagram_id,
                        PendingProductDatagram {
                            target,
                            payload,
                            traffic_class,
                            expires_at,
                            retry_at: (tokio::time::Instant::now() + retry_budget).min(expires_at),
                            attempted_paths,
                            emissions: 1,
                        },
                    );
                    self.mark_flow_attached(flow_id);
                    return Ok(());
                }
                Err(error) => {
                    self.refresh_detached_flows(tokio::time::Instant::now());
                    let source = datagram_path_send_error_into_runtime(error, payload.len());
                    if !datagram_underlay_error_is_retryable(&source)
                        && !matches!(
                            source,
                            RuntimeError::Datagram(DatagramError::PayloadTooLarge { .. })
                        )
                    {
                        return Err(source);
                    }
                    last_error = Some(source);
                }
            }
        }
        Err(last_error.unwrap_or(RuntimeError::NoDatagramPath))
    }

    async fn send_on_path(
        &mut self,
        key: RelayPathKey,
        send: DatagramPathSend,
    ) -> Result<(), DatagramPathSendError> {
        match key.underlay {
            UnderlayProtocol::Tcp => {
                if self.tcp.is_none() {
                    self.tcp = Some(Box::new(TcpDatagramClientAssociation::new(
                        self.context.clone(),
                    )));
                }
                self.tcp
                    .as_mut()
                    .expect("TCP datagram association initialized")
                    .send_to_path(key.index, send)
                    .await
            }
            UnderlayProtocol::Udp => {
                if self.udp.is_none() {
                    self.udp = Some(Box::new(UdpDatagramClientAssociation::new(
                        self.context.clone(),
                    )));
                }
                self.udp
                    .as_mut()
                    .expect("UDP datagram association initialized")
                    .send_to_path_index(key.index, send)
                    .await
            }
        }
    }

    fn path_setup_deadline(
        &self,
        key: RelayPathKey,
        has_unattempted_alternative: bool,
        product_deadline: tokio::time::Instant,
    ) -> tokio::time::Instant {
        let now = tokio::time::Instant::now();
        let remaining = product_deadline.saturating_duration_since(now);
        let budget = match key.underlay {
            UnderlayProtocol::Tcp => tcp_datagram_path_open_timeout(
                self.context.tcp_path_snapshot(key.index),
                has_unattempted_alternative,
                remaining,
            ),
            // QUIC applies its measured path-open budget after it knows whether
            // this association already owns a live connection.
            UnderlayProtocol::Udp if has_unattempted_alternative => remaining / 2,
            UnderlayProtocol::Udp => remaining,
        };
        (now + budget).min(product_deadline)
    }

    fn ranked_underlay_candidates(
        &mut self,
        payload_bytes: usize,
        ttl_ms: u32,
        traffic_class: TrafficClass,
    ) -> Vec<DatagramUnderlayCandidate> {
        let mut candidates =
            datagram_underlay_candidates(&self.context, payload_bytes, ttl_ms, traffic_class);
        if !candidates
            .iter()
            .any(|candidate| candidate.key.underlay == UnderlayProtocol::Udp)
        {
            return candidates;
        }
        if self.udp.is_none() {
            self.udp = Some(Box::new(UdpDatagramClientAssociation::new(
                self.context.clone(),
            )));
        }
        let ranked_udp = self
            .udp
            .as_mut()
            .expect("UDP datagram association initialized")
            .ranked_path_candidates(payload_bytes, ttl_ms);
        candidates.retain_mut(|candidate| {
            if candidate.key.underlay != UnderlayProtocol::Udp {
                return true;
            }
            let Some(rank) = ranked_udp
                .iter()
                .position(|ranked| ranked.path_index == candidate.key.index)
            else {
                return false;
            };
            candidate.underlay_rank = rank;
            true
        });
        sort_datagram_underlay_candidates(&self.context, &mut candidates);
        candidates
    }

    fn feedback_timeout(&self, key: RelayPathKey, ttl_ms: u32) -> Duration {
        match key.underlay {
            UnderlayProtocol::Tcp => self
                .tcp
                .as_ref()
                .map(|tcp| tcp.feedback_timeout(key.index, ttl_ms)),
            UnderlayProtocol::Udp => self
                .udp
                .as_ref()
                .map(|udp| udp.feedback_timeout(key.index, ttl_ms)),
        }
        .unwrap_or_else(|| Duration::from_millis(u64::from(ttl_ms)))
    }

    fn pending_entry_limit(&self) -> usize {
        self.context
            .mux_limits
            .max_streams
            .min(
                self.context
                    .mux_limits
                    .max_datagram_queue_bytes
                    .saturating_div(64),
            )
            .max(1)
    }

    fn retain_pending(
        &mut self,
        flow_id: DatagramFlowId,
        datagram_id: DatagramId,
        pending: PendingProductDatagram,
    ) {
        if pending.payload.len() > self.context.mux_limits.max_datagram_queue_bytes {
            return;
        }
        let key = (flow_id.0, datagram_id.0);
        if let Some(previous) = self.pending.insert(key, pending) {
            self.pending_bytes = self.pending_bytes.saturating_sub(previous.payload.len());
        }
        self.pending_bytes = self
            .pending_bytes
            .saturating_add(self.pending[&key].payload.len());
        while self.pending.len() > self.pending_entry_limit()
            || self.pending_bytes > self.context.mux_limits.max_datagram_queue_bytes
        {
            let Some(oldest_key) = self
                .pending
                .iter()
                .min_by_key(|(_, pending)| pending.expires_at)
                .map(|(key, _)| *key)
            else {
                break;
            };
            self.remove_pending(oldest_key);
        }
    }

    fn remove_pending(&mut self, key: (u64, u64)) -> Option<PendingProductDatagram> {
        let pending = self.pending.remove(&key)?;
        self.pending_bytes = self.pending_bytes.saturating_sub(pending.payload.len());
        Some(pending)
    }

    fn prune_pending(&mut self, now: tokio::time::Instant) {
        let expired = self
            .pending
            .iter()
            .filter_map(|(key, pending)| (pending.expires_at <= now).then_some(*key))
            .collect::<Vec<_>>();
        for key in expired {
            self.remove_pending(key);
        }
    }

    pub(in crate::runtime) fn next_retry_deadline(&self) -> Option<tokio::time::Instant> {
        self.pending
            .values()
            .map(|pending| pending.retry_at.min(pending.expires_at))
            .min()
    }

    pub(in crate::runtime) async fn retry_due_datagram(&mut self) -> Result<(), RuntimeError> {
        let now = tokio::time::Instant::now();
        self.prune_pending(now);
        let Some(key) = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.retry_at <= now)
            .min_by_key(|(_, pending)| pending.retry_at)
            .map(|(key, _)| *key)
        else {
            return Ok(());
        };
        let Some(mut pending) = self.remove_pending(key) else {
            return Ok(());
        };
        if pending.emissions >= MAX_DATAGRAM_EMISSIONS || pending.expires_at <= now {
            pending.retry_at = pending.expires_at;
            if pending.expires_at > now {
                self.retain_pending(DatagramFlowId(key.0), DatagramId(key.1), pending);
            }
            return Ok(());
        }

        let ttl_ms = datagram_remaining_ttl_ms(pending.expires_at);
        let candidates = self
            .ranked_underlay_candidates(pending.payload.len(), ttl_ms, pending.traffic_class)
            .into_iter()
            .filter(|candidate| !pending.attempted_paths.contains(&candidate.key))
            .collect::<Vec<_>>();
        for (position, candidate) in candidates.iter().copied().enumerate() {
            let has_alternative = position + 1 < candidates.len();
            pending.attempted_paths.push(candidate.key);
            let setup_deadline =
                self.path_setup_deadline(candidate.key, has_alternative, pending.expires_at);
            match self
                .send_on_path(
                    candidate.key,
                    DatagramPathSend {
                        target: pending.target.clone(),
                        flow_id: DatagramFlowId(key.0),
                        datagram_id: DatagramId(key.1),
                        payload: pending.payload.clone(),
                        setup_deadline,
                        product_deadline: pending.expires_at,
                        has_unattempted_alternative: has_alternative,
                    },
                )
                .await
            {
                Ok(()) => {
                    pending.emissions = pending.emissions.saturating_add(1);
                    let feedback_timeout = self.feedback_timeout(candidate.key, ttl_ms);
                    pending.retry_at = if pending.emissions < MAX_DATAGRAM_EMISSIONS {
                        (tokio::time::Instant::now() + feedback_timeout).min(pending.expires_at)
                    } else {
                        pending.expires_at
                    };
                    self.mark_flow_attached(DatagramFlowId(key.0));
                    self.retain_pending(DatagramFlowId(key.0), DatagramId(key.1), pending);
                    return Ok(());
                }
                Err(error) => {
                    self.refresh_detached_flows(tokio::time::Instant::now());
                    let source =
                        datagram_path_send_error_into_runtime(error, pending.payload.len());
                    if !datagram_underlay_error_is_retryable(&source)
                        && !matches!(
                            source,
                            RuntimeError::Datagram(DatagramError::PayloadTooLarge { .. })
                        )
                    {
                        pending.retry_at = pending.expires_at;
                        self.retain_pending(DatagramFlowId(key.0), DatagramId(key.1), pending);
                        return Err(source);
                    }
                }
            }
        }
        pending.retry_at = pending.expires_at;
        self.retain_pending(DatagramFlowId(key.0), DatagramId(key.1), pending);
        Ok(())
    }

    fn acknowledge_pending(
        &mut self,
        flow_id: DatagramFlowId,
        received: &[crate::protocol::OffsetRange],
    ) {
        let acknowledged = self
            .pending
            .keys()
            .filter(|(pending_flow_id, datagram_id)| {
                *pending_flow_id == flow_id.0
                    && received
                        .iter()
                        .any(|range| *datagram_id >= range.start && *datagram_id < range.end)
            })
            .copied()
            .collect::<Vec<_>>();
        for key in acknowledged {
            self.remove_pending(key);
        }
    }

    fn schedule_reinjection_after_path_failure(&mut self, key: RelayPathKey) {
        let now = tokio::time::Instant::now();
        for pending in self.pending.values_mut() {
            if pending.emissions < MAX_DATAGRAM_EMISSIONS && pending.attempted_paths.contains(&key)
            {
                pending.retry_at = now;
            }
        }
        self.refresh_detached_flows(now);
    }

    fn mark_flow_attached(&mut self, flow_id: DatagramFlowId) {
        if let Some(flow) = self
            .product_flows
            .iter_mut()
            .find(|flow| flow.flow_id == flow_id)
        {
            flow.detached_since = None;
        }
    }

    fn refresh_detached_flows(&mut self, now: tokio::time::Instant) {
        let udp = self.udp.as_deref();
        let tcp = self.tcp.as_deref();
        for flow in &mut self.product_flows {
            let attached = udp.is_some_and(|udp| udp.has_flow(flow.flow_id))
                || tcp.is_some_and(|tcp| tcp.has_flow(flow.flow_id));
            if attached {
                flow.detached_since = None;
            } else {
                flow.detached_since.get_or_insert(now);
            }
        }
    }

    pub(in crate::runtime) fn can_receive(&self) -> bool {
        self.udp
            .as_ref()
            .is_some_and(|association| association.has_open_path())
            || self
                .tcp
                .as_ref()
                .is_some_and(|association| association.has_open_path())
    }

    pub(in crate::runtime) async fn next_carrier_frame(
        &mut self,
    ) -> Result<DatagramClientCarrierFrame, RuntimeError> {
        let udp_open = self
            .udp
            .as_ref()
            .is_some_and(|association| association.has_open_path());
        let tcp_open = self
            .tcp
            .as_ref()
            .is_some_and(|association| association.has_open_path());
        match (udp_open, tcp_open) {
            (true, true) => {
                let udp = self.udp.as_mut().expect("open UDP datagram association");
                let tcp = self.tcp.as_mut().expect("open TCP datagram association");
                tokio::select! {
                    event = udp.next_frame() => {
                        let (path_index, frame) = event?;
                        Ok(DatagramClientCarrierFrame::Udp { path_index, frame })
                    }
                    event = tcp.next_frame() => {
                        let (path_index, frame) = event?;
                        Ok(DatagramClientCarrierFrame::Tcp { path_index, frame })
                    },
                }
            }
            (true, false) => {
                let (path_index, frame) = self
                    .udp
                    .as_mut()
                    .expect("open UDP datagram association")
                    .next_frame()
                    .await?;
                Ok(DatagramClientCarrierFrame::Udp { path_index, frame })
            }
            (false, true) => {
                let (path_index, frame) = self
                    .tcp
                    .as_mut()
                    .expect("open TCP datagram association")
                    .next_frame()
                    .await?;
                Ok(DatagramClientCarrierFrame::Tcp { path_index, frame })
            }
            (false, false) => Err(RuntimeError::NoDatagramPath),
        }
    }

    pub(in crate::runtime) async fn handle_carrier_frame(
        &mut self,
        event: DatagramClientCarrierFrame,
    ) -> Result<DatagramClientReceive, RuntimeError> {
        let (session_event, receipt_path) = match event {
            DatagramClientCarrierFrame::Tcp {
                path_index,
                frame: Ok(frame),
            } => {
                let result = self
                    .tcp
                    .as_mut()
                    .ok_or(RuntimeError::NoSchedulableTcpPath)?
                    .handle_frame(path_index, frame)
                    .await;
                let session_event = match result {
                    Ok(event) => event,
                    Err(error) => {
                        self.schedule_reinjection_after_path_failure(RelayPathKey {
                            underlay: UnderlayProtocol::Tcp,
                            index: path_index,
                        });
                        return Err(error);
                    }
                };
                (
                    session_event,
                    RelayPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        index: path_index,
                    },
                )
            }
            DatagramClientCarrierFrame::Tcp {
                path_index,
                frame: Err(err),
            } => {
                if let Some(tcp) = self.tcp.as_mut() {
                    tcp.handle_receive_error(path_index);
                }
                self.schedule_reinjection_after_path_failure(RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: path_index,
                });
                return Err(err);
            }
            DatagramClientCarrierFrame::Udp {
                path_index,
                frame: Ok(frame),
            } => {
                let result = self
                    .udp
                    .as_mut()
                    .ok_or(RuntimeError::NoSchedulableUdpPath)?
                    .handle_frame(path_index, frame)
                    .await;
                let session_event = match result {
                    Ok(event) => event,
                    Err(error) => {
                        if client_udp_error_disposition(&error)
                            != ClientUdpErrorDisposition::Session
                        {
                            self.schedule_reinjection_after_path_failure(RelayPathKey {
                                underlay: UnderlayProtocol::Udp,
                                index: path_index,
                            });
                        }
                        return Err(error);
                    }
                };
                (
                    session_event,
                    RelayPathKey {
                        underlay: UnderlayProtocol::Udp,
                        index: path_index,
                    },
                )
            }
            DatagramClientCarrierFrame::Udp {
                path_index,
                frame: Err(err),
            } => {
                let disposition = client_udp_error_disposition(&err);
                let disposition = if let Some(udp) = self.udp.as_mut() {
                    udp.handle_receive_error(path_index, disposition).await
                } else {
                    disposition
                };
                if disposition != ClientUdpErrorDisposition::Session {
                    self.schedule_reinjection_after_path_failure(RelayPathKey {
                        underlay: UnderlayProtocol::Udp,
                        index: path_index,
                    });
                }
                return Err(err);
            }
        };
        match session_event {
            DatagramSessionEvent::Control => Ok(DatagramClientReceive::Control),
            DatagramSessionEvent::Feedback { flow_id, received } => {
                self.acknowledge_pending(flow_id, &received);
                Ok(DatagramClientReceive::Control)
            }
            DatagramSessionEvent::Received(received) => {
                self.admit_received_datagram(received, receipt_path)
            }
        }
    }

    fn admit_received_datagram(
        &mut self,
        received: ReceivedDatagram,
        path: RelayPathKey,
    ) -> Result<DatagramClientReceive, RuntimeError> {
        let tcp_attachment_id = if path.underlay == UnderlayProtocol::Tcp {
            Some(
                self.tcp
                    .as_ref()
                    .and_then(|tcp| tcp.attachment_id(path.index))
                    .ok_or(RuntimeError::NoSchedulableTcpPath)?,
            )
        } else {
            None
        };
        let flow = self
            .product_flows
            .iter_mut()
            .find(|flow| flow.flow_id == received.flow_id)
            .ok_or(RuntimeError::Protocol("response for unknown datagram flow"))?;
        let receipt = match path.underlay {
            UnderlayProtocol::Udp => DatagramClientReceipt::Udp {
                path_index: path.index,
                flow_id: received.flow_id,
                datagram_id: received.datagram_id,
            },
            UnderlayProtocol::Tcp => DatagramClientReceipt::Tcp {
                path_index: path.index,
                attachment_id: tcp_attachment_id.expect("TCP receipt attachment resolved"),
                flow_id: received.flow_id,
                datagram_id: received.datagram_id,
                write_deadline: received.expires_at,
            },
        };
        match flow.received_responses.admit(
            received.datagram_id.0,
            DatagramPayloadIdentity::new(&received.payload),
        ) {
            Ok(DatagramAdmission::Duplicate) => {
                return Ok(DatagramClientReceive::Duplicate(receipt));
            }
            Ok(DatagramAdmission::Fresh) => {}
            Err(()) => {
                return Err(RuntimeError::Protocol(
                    "response datagram ID reused with a different payload",
                ));
            }
        }
        flow.telemetry_counter
            .record_datagram_from_peer(received.payload.len() as u64);
        Ok(DatagramClientReceive::Deliver {
            target: flow.target.clone(),
            payload: received.payload,
            receipt,
        })
    }

    pub(in crate::runtime) async fn acknowledge_received(
        &mut self,
        receipt: DatagramClientReceipt,
    ) -> Result<(), RuntimeError> {
        match receipt {
            DatagramClientReceipt::Tcp {
                path_index,
                attachment_id,
                flow_id,
                datagram_id,
                write_deadline,
            } => {
                self.tcp
                    .as_mut()
                    .ok_or(RuntimeError::NoSchedulableTcpPath)?
                    .acknowledge(
                        path_index,
                        attachment_id,
                        flow_id,
                        datagram_id,
                        write_deadline,
                    )
                    .await
            }
            DatagramClientReceipt::Udp {
                path_index,
                flow_id,
                datagram_id,
            } => {
                self.udp
                    .as_mut()
                    .ok_or(RuntimeError::NoSchedulableUdpPath)?
                    .acknowledge(path_index, flow_id, datagram_id)
                    .await
            }
        }
    }

    /// Retire the logical Product lifetime independently of transport close
    /// publication.
    ///
    /// Idle expiry is authoritative even when a carrier cannot accept its
    /// best-effort `DGRAM_CLOSE` publication. Pending reinjection state and
    /// session ownership therefore leave with the Product flow, while the
    /// underlay-local flow identities remain available to `close()` long
    /// enough to publish the close when capacity permits.
    pub(in crate::runtime) fn complete_product_lifetime(&mut self) {
        self.pending.clear();
        self.pending_bytes = 0;
        for flow in self.product_flows.drain(..) {
            flow.telemetry_flow.complete();
        }
    }

    /// Converts the live association into one typed terminal intent. No
    /// Product owner remains inside the returned value, while every exact
    /// transport attachment remains owned until close or Drop settles it.
    pub(in crate::runtime) fn begin_product_retirement(
        mut self,
    ) -> RetiringDatagramClientAssociation {
        self.complete_product_lifetime();
        RetiringDatagramClientAssociation { association: self }
    }

    /// One carrier-liveness horizon bounds best-effort close publication after
    /// authoritative Product idle expiry. Ordinary (non-idle) close keeps its
    /// existing transport-specific settlement and error semantics.
    pub(in crate::runtime) fn idle_close_publication_timeout(&self) -> Option<Duration> {
        let tcp = self
            .tcp
            .as_ref()
            .is_some_and(|tcp| tcp.has_open_path())
            .then_some(self.context.mux_limits.tcp_path_heartbeat_timeout);
        let udp = self
            .udp
            .as_ref()
            .is_some_and(|udp| udp.has_open_path())
            .then_some(self.context.mux_limits.quic_path_idle_timeout);
        match (tcp, udp) {
            (Some(tcp), Some(udp)) => Some(tcp.max(udp)),
            (Some(timeout), None) | (None, Some(timeout)) => Some(timeout),
            (None, None) => None,
        }
    }

    pub(in crate::runtime) async fn close(&mut self) -> Result<(), RuntimeError> {
        let udp_result = if let Some(udp) = &mut self.udp {
            udp.close().await
        } else {
            Ok(())
        };
        let tcp_result = if let Some(tcp) = &mut self.tcp {
            tcp.close().await
        } else {
            Ok(())
        };
        let result = udp_result.and(tcp_result);
        if result.is_ok() {
            self.complete_product_lifetime();
        }
        result
    }
}

impl RetiringDatagramClientAssociation {
    pub(in crate::runtime) fn publication_timeout(&self) -> Option<Duration> {
        self.association.idle_close_publication_timeout()
    }

    pub(in crate::runtime) async fn close(&mut self) -> Result<(), RuntimeError> {
        self.association.close().await
    }
}

fn datagram_underlay_candidates(
    context: &ClientPathContext,
    payload_bytes: usize,
    ttl_ms: u32,
    traffic_class: TrafficClass,
) -> Vec<DatagramUnderlayCandidate> {
    if ttl_ms == 0 {
        return Vec::new();
    }
    let freshness_budget_ms = f64::from(ttl_ms);
    let mut candidates = Vec::new();

    for (underlay_rank, path_index) in context
        .ordered_tcp_path_indices(traffic_class, payload_bytes)
        .into_iter()
        .enumerate()
    {
        let key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: path_index,
        };
        if let Some(eta_ms) = context.reliable_relay_path_eta_ms(key, traffic_class, payload_bytes)
            && eta_ms <= freshness_budget_ms
        {
            candidates.push(DatagramUnderlayCandidate {
                key,
                eta_ms,
                underlay_rank,
            });
        }
    }

    for (underlay_rank, candidate) in context
        .ordered_udp_path_candidates_for_ttl(payload_bytes, ttl_ms)
        .into_iter()
        .enumerate()
    {
        candidates.push(DatagramUnderlayCandidate {
            key: RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: candidate.path_index,
            },
            eta_ms: candidate.eta_ms,
            underlay_rank,
        });
    }

    sort_datagram_underlay_candidates(context, &mut candidates);
    candidates
}

fn sort_datagram_underlay_candidates(
    context: &ClientPathContext,
    candidates: &mut [DatagramUnderlayCandidate],
) {
    candidates.sort_by(|left, right| {
        let left_backup = context
            .reliable_path_snapshot(left.key)
            .is_some_and(path_is_backup);
        let right_backup = context
            .reliable_path_snapshot(right.key)
            .is_some_and(path_is_backup);
        left_backup
            .cmp(&right_backup)
            .then_with(|| {
                if left.key.underlay == right.key.underlay {
                    left.underlay_rank.cmp(&right.underlay_rank)
                } else {
                    left.eta_ms.total_cmp(&right.eta_ms)
                }
            })
            .then_with(|| context.relay_path_key_order(left.key, right.key))
    });
}

fn datagram_path_send_error_into_runtime(
    error: DatagramPathSendError,
    payload_bytes: usize,
) -> RuntimeError {
    match error {
        DatagramPathSendError::PayloadLimitExceeded { limit } => {
            RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                actual: payload_bytes,
                limit,
            })
        }
        DatagramPathSendError::Timeout => RuntimeError::PathOpenTimedOut,
        DatagramPathSendError::UdpPathOpen(source) => source,
        DatagramPathSendError::Runtime(source) => source,
    }
}

pub(in crate::runtime) fn datagram_underlay_error_is_retryable(err: &RuntimeError) -> bool {
    if runtime_error_is_datagram_response_timeout(err) {
        return false;
    }
    if client_udp_error_disposition(err) == ClientUdpErrorDisposition::Session {
        return false;
    }
    matches!(
        err,
        RuntimeError::NoTcpPath
            | RuntimeError::NoSchedulableTcpPath
            | RuntimeError::NoSchedulableUdpPath
            | RuntimeError::Io(_)
            | RuntimeError::Tcp(_)
            | RuntimeError::Udp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::QuicCarrier(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemotePathClosed(_)
            | RuntimeError::Protocol(_)
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::PathHeartbeatTimeout
            | RuntimeError::ReliablePathSessionClosed
            | RuntimeError::ReliablePathRetired
    )
}

pub(in crate::runtime) fn runtime_error_is_datagram_response_timeout(err: &RuntimeError) -> bool {
    matches!(err, RuntimeError::DatagramResponseTimedOut)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClientSecurityConfig, SharedSecret};
    use crate::performance::ResourceLimits;

    #[tokio::test]
    async fn product_completion_is_authoritative_before_transport_close() {
        let security = ClientSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
                .expect("association test secret"),
        );
        let context = ClientPathContext::new(
            vec![
                "quic://127.0.0.1:16191"
                    .parse()
                    .expect("association test path"),
            ],
            security,
            ResourceLimits::default(),
        )
        .expect("association test context");
        let telemetry = context.telemetry.clone();
        let mut association = DatagramClientAssociation::new(context)
            .await
            .expect("association");
        association
            .allocate_product_datagram(TargetAddr::Ip(
                "203.0.113.19:443".parse().expect("association target"),
            ))
            .expect("logical datagram flow");

        let active = telemetry.snapshot();
        assert_eq!(active.datagram.flows.opened, 1);
        assert_eq!(active.datagram.flows.active, 1);
        assert_eq!(association.product_flows.len(), 1);

        let retirement = association.begin_product_retirement();

        let retired = telemetry.snapshot();
        assert_eq!(retired.datagram.flows.active, 0);
        assert_eq!(retired.datagram.flows.completed, 1);
        assert_eq!(retired.datagram.flows.failed, 0);
        assert!(retirement.association.product_flows.is_empty());
        assert!(retirement.association.pending.is_empty());
        assert_eq!(retirement.association.pending_bytes, 0);
    }
}
