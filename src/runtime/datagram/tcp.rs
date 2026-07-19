//! TCP carrier attachments for one product datagram association.

use super::DatagramSessionEvent;
use super::association::{DatagramPathSend, runtime_error_is_datagram_response_timeout};
use super::policy::DatagramPathSendError;
use super::tcp_session::TcpDatagramClientSession;
use crate::model::capacity::{DATAGRAM_FEEDBACK_DELAY_BUDGET, TRANSPORT_TIMER_GRANULARITY};
use crate::model::timing::{
    path_open_pto, path_open_pto_multiplier, path_open_serialized_exchanges,
    transport_pto_from_snapshot,
};
use crate::protocol::{DatagramFlowId, DatagramId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::tcp::client::ClientTcpDatagramInbound;
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::time::{Duration, Instant};

pub(in crate::runtime) struct TcpDatagramClientAssociation {
    context: ClientPathContext,
    paths: Vec<TcpDatagramClientSession>,
}

impl TcpDatagramClientAssociation {
    pub(in crate::runtime) fn new(context: ClientPathContext) -> Self {
        Self {
            context,
            paths: Vec::new(),
        }
    }

    pub(super) async fn send_to_path(
        &mut self,
        path_index: usize,
        send: DatagramPathSend,
    ) -> Result<(), DatagramPathSendError> {
        let DatagramPathSend {
            target,
            flow_id,
            datagram_id,
            payload,
            setup_deadline,
            product_deadline,
            ..
        } = send;
        let position = self
            .ensure_path(path_index, payload.len(), setup_deadline)
            .await
            .map_err(DatagramPathSendError::runtime)?;
        let result = self.paths[position]
            .send_to(
                target,
                flow_id,
                datagram_id,
                payload,
                setup_deadline,
                product_deadline,
            )
            .await;
        if result.is_err() && !self.paths[position].connection_usable {
            self.remove_path(path_index, false);
        }
        result
    }

    async fn ensure_path(
        &mut self,
        path_index: usize,
        payload_bytes: usize,
        setup_deadline: tokio::time::Instant,
    ) -> Result<usize, RuntimeError> {
        if let Some(position) = self
            .paths
            .iter()
            .position(|session| session.path_index == path_index)
        {
            return Ok(position);
        }
        if self.context.tcp_paths.get(path_index).is_none() {
            return Err(RuntimeError::NoSchedulableTcpPath);
        }
        let key = crate::model::path::RelayPathKey {
            underlay: crate::protocol::UnderlayProtocol::Tcp,
            index: path_index,
        };
        let remaining = setup_deadline.saturating_duration_since(tokio::time::Instant::now());
        let eta_ms = self
            .context
            .reliable_relay_path_eta_ms(key, TrafficClass::RealtimeDatagram, payload_bytes)
            .unwrap_or(f64::INFINITY);
        if remaining.is_zero() || eta_ms > remaining.as_secs_f64() * 1000.0 {
            return Err(RuntimeError::PathOpenTimedOut);
        }
        let started_at = Instant::now();
        match TcpDatagramClientSession::open(&self.context, path_index, setup_deadline).await {
            Ok(session) => {
                self.context.mark_tcp_path_open_success(
                    path_index,
                    started_at.elapsed(),
                    TrafficClass::RealtimeDatagram,
                );
                self.paths.push(session);
                Ok(self.paths.len() - 1)
            }
            Err(error) => {
                if tcp_datagram_error_is_path_retryable(&error) {
                    self.context.mark_tcp_path_failure(path_index);
                }
                Err(error)
            }
        }
    }

    pub(in crate::runtime) fn feedback_timeout(&self, path_index: usize, ttl_ms: u32) -> Duration {
        self.paths
            .iter()
            .find(|session| session.path_index == path_index)
            .map(|session| session.response_timeout(ttl_ms))
            .or_else(|| {
                self.context
                    .tcp_path_snapshot(path_index)
                    .map(|snapshot| tcp_datagram_response_timeout(snapshot, None, None, ttl_ms))
            })
            .unwrap_or_else(|| Duration::from_millis(u64::from(ttl_ms)))
    }

    pub(in crate::runtime) async fn close(&mut self) -> Result<(), RuntimeError> {
        let mut close_error = None;
        while let Some(mut session) = self.paths.pop() {
            let path_index = session.path_index;
            let result = session.close().await;
            self.context.mark_tcp_path_delivery_for_instance(
                path_index,
                session.path_instance_id(),
                session.delivery_stats(),
            );
            self.context
                .release_tcp_path_load(path_index, TrafficClass::RealtimeDatagram);
            if close_error.is_none() {
                close_error = result.err();
            }
        }
        close_error.map_or(Ok(()), Err)
    }

    pub(in crate::runtime) fn has_open_path(&self) -> bool {
        !self.paths.is_empty()
    }

    pub(in crate::runtime) async fn next_frame(
        &mut self,
    ) -> Result<(usize, Result<ClientTcpDatagramInbound, RuntimeError>), RuntimeError> {
        if self.paths.is_empty() {
            return Err(RuntimeError::NoSchedulableTcpPath);
        }
        let reads = self.paths.iter_mut().map(|session| {
            let path_index = session.path_index;
            Box::pin(async move { (path_index, session.next_frame().await) })
        });
        let ((path_index, frame), _, _) = futures::future::select_all(reads).await;
        Ok((path_index, frame))
    }

    pub(in crate::runtime) async fn handle_frame(
        &mut self,
        path_index: usize,
        frame: ClientTcpDatagramInbound,
    ) -> Result<DatagramSessionEvent, RuntimeError> {
        let position = self
            .paths
            .iter()
            .position(|session| session.path_index == path_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        let result = self.paths[position].handle_frame(frame).await;
        if result.is_err() {
            self.remove_path(path_index, false);
        }
        result
    }

    pub(in crate::runtime) async fn acknowledge(
        &mut self,
        path_index: usize,
        attachment_id: u64,
        flow_id: DatagramFlowId,
        datagram_id: DatagramId,
        write_deadline: tokio::time::Instant,
    ) -> Result<(), RuntimeError> {
        let session = self
            .paths
            .iter_mut()
            .find(|session| {
                session.path_index == path_index && session.attachment_id() == attachment_id
            })
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        session
            .acknowledge(flow_id, datagram_id, write_deadline)
            .await
    }

    pub(in crate::runtime) fn attachment_id(&self, path_index: usize) -> Option<u64> {
        self.paths
            .iter()
            .find(|session| session.path_index == path_index)
            .map(TcpDatagramClientSession::attachment_id)
    }

    pub(in crate::runtime) fn has_flow(&self, flow_id: DatagramFlowId) -> bool {
        self.paths.iter().any(|session| session.has_flow(flow_id))
    }

    pub(in crate::runtime) fn handle_receive_error(&mut self, path_index: usize) {
        self.remove_path(path_index, false);
    }

    fn remove_path(&mut self, path_index: usize, failed: bool) {
        let Some(position) = self
            .paths
            .iter()
            .position(|session| session.path_index == path_index)
        else {
            return;
        };
        let session = self.paths.swap_remove(position);
        self.context.mark_tcp_path_delivery_for_instance(
            path_index,
            session.path_instance_id(),
            session.delivery_stats(),
        );
        self.context
            .release_tcp_path_load(path_index, TrafficClass::RealtimeDatagram);
        if failed {
            self.context.mark_tcp_path_failure(path_index);
        }
    }
}

pub(in crate::runtime) fn tcp_datagram_response_timeout(
    snapshot: PathSnapshot,
    response_srtt: Option<Duration>,
    response_rttvar: Option<Duration>,
    ttl_ms: u32,
) -> Duration {
    let ttl = Duration::from_millis(u64::from(ttl_ms));
    if ttl.is_zero() {
        return ttl;
    }
    let initial_response_pto = transport_pto_from_snapshot(Some(snapshot));
    let srtt = response_srtt.unwrap_or(initial_response_pto);
    let rttvar = response_rttvar.unwrap_or_else(|| {
        Duration::from_secs_f64((snapshot.jitter_ms.max(snapshot.srtt_ms.max(1.0) / 8.0)) / 1000.0)
    });
    let loss_gain = 1.0 + snapshot.loss_rate.clamp(0.0, 1.0);
    (srtt + rttvar.mul_f64(4.0) + DATAGRAM_FEEDBACK_DELAY_BUDGET)
        .mul_f64(loss_gain)
        .max(TRANSPORT_TIMER_GRANULARITY.min(ttl))
        .min(ttl)
}

pub(in crate::runtime) fn tcp_datagram_path_open_timeout(
    snapshot: Option<PathSnapshot>,
    has_unattempted_alternative: bool,
    remaining_ttl: Duration,
) -> Duration {
    // A new TCP carrier needs its own conservative retransmission budget;
    // a prior probe RTT cannot prove that this connection has opened.
    let fresh_carrier_pto = path_open_pto(snapshot, false);
    if has_unattempted_alternative {
        fresh_carrier_pto
            .saturating_mul(path_open_serialized_exchanges(snapshot))
            .min(remaining_ttl / 2)
    } else {
        fresh_carrier_pto
            .saturating_mul(path_open_pto_multiplier(snapshot))
            .min(remaining_ttl)
    }
}

pub(in crate::runtime) fn tcp_datagram_error_is_path_retryable(err: &RuntimeError) -> bool {
    if runtime_error_is_datagram_response_timeout(err) {
        return false;
    }
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Tcp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::PathHeartbeatTimeout
            | RuntimeError::ReliablePathSessionClosed
    )
}
