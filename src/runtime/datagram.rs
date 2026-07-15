//! Product datagram orchestration across TCP and QUIC carrier paths.

use super::RuntimeError;
use crate::config::{ResourceLimits, SecurityConfig};
use crate::model::timing::default_transport_pto;
use crate::mux::MuxLimits;
use crate::mux::datagram::{DatagramError, DatagramFlow};
use crate::protocol::codec::CodecLimits;
use crate::protocol::frame::datagram_ack_range as protocol_datagram_ack_range;
use crate::protocol::{DatagramFlowId, DatagramId, OffsetRange, TargetAddr};
use crate::transport::{CarrierNetworkProvider, PathSpec, SystemCarrierNetworkProvider};
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod association;
mod policy;
mod quic;
mod quic_session;
mod server;
mod tcp;
mod tcp_session;

pub(super) use association::{DatagramClientAssociation, datagram_underlay_candidate_keys};
pub(super) use quic_session::UdpDatagramClientSession;
pub(in crate::runtime) use server::ServerDatagramService;

#[cfg(test)]
pub(super) use association::{
    datagram_underlay_error_is_retryable, runtime_error_is_datagram_response_timeout,
};
#[cfg(test)]
pub(super) use policy::{
    DatagramTimeoutAction, datagram_response_deadline_budget, datagram_timeout_action,
};
#[cfg(test)]
pub(super) use quic::{
    UdpDatagramClientAssociation, udp_datagram_error_is_path_retryable,
    udp_datagram_first_response_timeout, udp_datagram_path_open_timeout,
};
#[cfg(test)]
pub(super) use tcp::{
    tcp_datagram_error_is_path_retryable, tcp_datagram_path_open_timeout,
    tcp_datagram_response_timeout,
};

pub(in crate::runtime) const UDP_PATH_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);

/// Associates one product destination with its framed datagram flow.
pub(super) struct DatagramClientFlow {
    pub(super) target: TargetAddr,
    pub(super) flow: DatagramFlow,
    pub(super) flow_id: DatagramFlowId,
}

/// Retains only the delivery evidence shared by TCP and QUIC sessions.
#[derive(Debug, Clone, Copy)]
pub(super) struct SentDatagram {
    pub(super) sent_at: Instant,
    pub(super) bytes: usize,
    pub(super) ttl: Duration,
}

pub(in crate::runtime) fn datagram_ack_range(
    datagram_id: DatagramId,
) -> Result<OffsetRange, RuntimeError> {
    protocol_datagram_ack_range(datagram_id)
        .ok_or(RuntimeError::Protocol("datagram ACK range overflow"))
}

pub(super) fn datagram_id_is_in_ranges(datagram_id: DatagramId, ranges: &[OffsetRange]) -> bool {
    ranges
        .iter()
        .any(|range| datagram_id.0 >= range.start && datagram_id.0 < range.end)
}

pub async fn client_udp_datagram_round_trip(
    path: &PathSpec,
    security: SecurityConfig,
    resources: ResourceLimits,
    target: TargetAddr,
    payload: Bytes,
    ttl_ms: u32,
) -> Result<Bytes, RuntimeError> {
    client_udp_datagram_round_trip_with_provider(
        path,
        security,
        resources,
        target,
        payload,
        ttl_ms,
        Arc::new(SystemCarrierNetworkProvider),
    )
    .await
}

/// Runs a standalone QUIC datagram flow through a host-provided carrier network.
pub async fn client_udp_datagram_round_trip_with_provider(
    path: &PathSpec,
    security: SecurityConfig,
    resources: ResourceLimits,
    target: TargetAddr,
    payload: Bytes,
    ttl_ms: u32,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
) -> Result<Bytes, RuntimeError> {
    client_udp_datagram_round_trip_with_limits(
        path,
        security,
        resources.into(),
        resources.into(),
        target,
        payload,
        ttl_ms,
        carrier_network,
    )
    .await
}

async fn client_udp_datagram_round_trip_with_limits(
    path: &PathSpec,
    security: SecurityConfig,
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
    let response = session
        .send_to(
            target,
            payload,
            product_deadline,
            product_deadline,
            default_transport_pto().min(Duration::from_millis(u64::from(ttl_ms))),
        )
        .await
        .map_err(|err| match err {
            policy::DatagramPathSendError::Runtime { source, .. } => source,
            policy::DatagramPathSendError::PayloadLimitExceeded { limit } => {
                RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                    actual: payload_len,
                    limit,
                })
            }
            policy::DatagramPathSendError::Timeout { .. } => RuntimeError::DatagramResponseTimedOut,
        })?;
    session.close().await?;
    Ok(response)
}
