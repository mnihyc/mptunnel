//! Product datagram orchestration across TCP and QUIC carrier paths.

use super::RuntimeError;
use crate::config::ClientSecurityConfig;
use crate::mux::MuxLimits;
use crate::mux::datagram::DatagramError;
use crate::performance::ResourceLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::frame::datagram_feedback_range as protocol_datagram_feedback_range;
use crate::protocol::{DatagramFlowId, DatagramId, OffsetRange, TargetAddr};
use crate::transport::encrypted::TcpClientTlsConfig;
use crate::transport::{CarrierNetworkProvider, PathSpec, SystemCarrierNetworkProvider};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod association;
mod edge;
mod policy;
mod quic;
mod quic_session;
mod server;
mod tcp;
mod tcp_session;

pub(super) use association::DatagramClientAssociation;
#[cfg(test)]
pub(super) use association::DatagramClientReceive;
pub(super) use edge::{
    UdpEdgeCompletion, UdpEdgeLane, UdpEdgeRequest, close_udp_edge_lanes,
    dispatch_udp_edge_request_with_idle_timeout, finish_udp_edge_completion,
    reap_finished_udp_edge_lane_instance, remove_udp_edge_lane, udp_edge_completion_queue,
    udp_edge_queue_slots,
};
#[cfg(test)]
pub(super) use quic_session::UdpDatagramClientSession;
pub(in crate::runtime) use server::{ServerDatagramService, ServerDatagramServiceConfig};

#[cfg(test)]
pub(super) use association::{
    datagram_underlay_error_is_retryable, runtime_error_is_datagram_response_timeout,
};
#[cfg(test)]
pub(super) use policy::datagram_feedback_retry_budget;
#[cfg(test)]
pub(super) use quic::{
    UdpDatagramClientAssociation, udp_datagram_error_is_path_retryable,
    udp_datagram_path_open_timeout,
};
#[cfg(test)]
pub(super) use tcp::{tcp_datagram_path_open_timeout, tcp_datagram_response_timeout};

pub(in crate::runtime) const UDP_PATH_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);

/// Associates one product destination with its framed datagram flow.
pub(super) struct DatagramClientFlow {
    pub(super) target: TargetAddr,
    pub(super) flow_id: DatagramFlowId,
}

/// One peer-originated datagram, independent of any locally sent datagram.
pub(super) struct ReceivedDatagram {
    pub(super) flow_id: DatagramFlowId,
    pub(super) datagram_id: DatagramId,
    pub(super) expires_at: tokio::time::Instant,
    pub(super) payload: Bytes,
}

pub(super) enum DatagramSessionEvent {
    Control,
    Feedback {
        flow_id: DatagramFlowId,
        received: Vec<OffsetRange>,
    },
    Received(ReceivedDatagram),
}

/// Retains only the delivery evidence shared by TCP and QUIC sessions.
#[derive(Debug, Clone, Copy)]
pub(super) struct SentDatagram {
    pub(super) sent_at: Instant,
    pub(super) bytes: usize,
    pub(super) ttl: Duration,
}

pub(super) struct SentDatagramEvidence {
    entries: HashMap<(DatagramFlowId, DatagramId), SentDatagram>,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl SentDatagramEvidence {
    pub(super) fn new(limits: MuxLimits) -> Self {
        Self {
            entries: HashMap::new(),
            bytes: 0,
            max_entries: limits
                .max_streams
                .min(limits.max_datagram_queue_bytes.saturating_div(64))
                .max(limits.max_ack_ranges)
                .max(1),
            max_bytes: limits.max_datagram_queue_bytes,
        }
    }

    pub(super) fn insert(&mut self, key: (DatagramFlowId, DatagramId), sent: SentDatagram) {
        if sent.bytes > self.max_bytes {
            return;
        }
        if let Some(previous) = self.entries.insert(key, sent) {
            self.bytes = self.bytes.saturating_sub(previous.bytes);
        }
        self.bytes = self.bytes.saturating_add(sent.bytes);
        while self.entries.len() > self.max_entries || self.bytes > self.max_bytes {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, sent)| sent.sent_at)
                .map(|(key, _)| *key)
            else {
                break;
            };
            self.remove(&oldest);
        }
    }

    pub(super) fn remove(&mut self, key: &(DatagramFlowId, DatagramId)) -> Option<SentDatagram> {
        let sent = self.entries.remove(key)?;
        self.bytes = self.bytes.saturating_sub(sent.bytes);
        Some(sent)
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &(DatagramFlowId, DatagramId)> {
        self.entries.keys()
    }

    pub(super) fn expire(&mut self, now: Instant) -> u64 {
        let expired = self
            .entries
            .iter()
            .filter_map(|(key, sent)| {
                (now.duration_since(sent.sent_at) >= sent.ttl).then_some(*key)
            })
            .collect::<Vec<_>>();
        let lost = expired.len() as u64;
        for key in expired {
            self.remove(&key);
        }
        lost
    }
}

pub(in crate::runtime) fn datagram_feedback_range(
    datagram_id: DatagramId,
) -> Result<OffsetRange, RuntimeError> {
    protocol_datagram_feedback_range(datagram_id)
        .ok_or(RuntimeError::Protocol("datagram feedback range overflow"))
}

pub(super) fn datagram_id_is_in_ranges(datagram_id: DatagramId, ranges: &[OffsetRange]) -> bool {
    ranges
        .iter()
        .any(|range| datagram_id.0 >= range.start && datagram_id.0 < range.end)
}

pub async fn client_udp_datagram_round_trip(
    path: &PathSpec,
    security: ClientSecurityConfig,
    tls: TcpClientTlsConfig,
    resources: ResourceLimits,
    target: TargetAddr,
    payload: Bytes,
    ttl_ms: u32,
) -> Result<Bytes, RuntimeError> {
    client_udp_datagram_round_trip_with_provider(
        path,
        security,
        tls,
        resources,
        target,
        payload,
        ttl_ms,
        Arc::new(SystemCarrierNetworkProvider),
    )
    .await
}

/// Runs a standalone QUIC datagram flow through a host-provided carrier network.
#[allow(
    clippy::too_many_arguments,
    reason = "the diagnostic API mirrors one complete datagram operation without hidden global state"
)]
pub async fn client_udp_datagram_round_trip_with_provider(
    path: &PathSpec,
    security: ClientSecurityConfig,
    tls: TcpClientTlsConfig,
    resources: ResourceLimits,
    target: TargetAddr,
    payload: Bytes,
    ttl_ms: u32,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
) -> Result<Bytes, RuntimeError> {
    client_udp_datagram_round_trip_with_limits(
        path,
        security,
        tls,
        resources.into(),
        resources.into(),
        target,
        payload,
        ttl_ms,
        carrier_network,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn client_udp_datagram_round_trip_with_limits(
    path: &PathSpec,
    security: ClientSecurityConfig,
    tls: TcpClientTlsConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    target: TargetAddr,
    payload: Bytes,
    ttl_ms: u32,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
) -> Result<Bytes, RuntimeError> {
    let payload_len = payload.len();
    let setup_started_at = tokio::time::Instant::now();
    let product_deadline = setup_started_at + Duration::from_millis(u64::from(ttl_ms));
    let open_deadline = (setup_started_at + UDP_PATH_HANDSHAKE_TIMEOUT).min(product_deadline);
    if open_deadline <= tokio::time::Instant::now() {
        return Err(RuntimeError::DatagramResponseTimedOut);
    }
    let open = quic_session::UdpDatagramClientSession::open_with_provider(
        path,
        0,
        security,
        tls,
        codec_limits,
        mux_limits,
        open_deadline,
        carrier_network,
    )
    .await;
    let mut session = match open {
        Err(RuntimeError::PathOpenTimedOut) if open_deadline == product_deadline => {
            return Err(RuntimeError::DatagramResponseTimedOut);
        }
        result => result?,
    };
    if tokio::time::Instant::now() >= product_deadline {
        return Err(RuntimeError::DatagramResponseTimedOut);
    }
    session
        .send_to(
            target,
            DatagramFlowId(0),
            DatagramId(0),
            payload,
            product_deadline,
            product_deadline,
        )
        .await
        .map_err(|err| match err {
            policy::DatagramPathSendError::Runtime(source) => source,
            policy::DatagramPathSendError::UdpPathOpen(source) => source,
            policy::DatagramPathSendError::PayloadLimitExceeded { limit } => {
                RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                    actual: payload_len,
                    limit,
                })
            }
            policy::DatagramPathSendError::Timeout => RuntimeError::DatagramResponseTimedOut,
        })?;
    let response = loop {
        let frame = tokio::time::timeout_at(product_deadline, session.next_frame())
            .await
            .map_err(|_| RuntimeError::DatagramResponseTimedOut)??;
        if let DatagramSessionEvent::Received(response) = session.handle_frame(frame).await? {
            session
                .acknowledge(response.flow_id, response.datagram_id)
                .await?;
            break response.payload;
        }
    };
    session.close().await?;
    Ok(response)
}
