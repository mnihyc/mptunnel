//! QUIC endpoint, connection, authentication, and transport configuration.

use super::native_datagram::NativeDatagramHub;
use super::presentation::H3Presentation;
use super::socket::{
    RemotePortMigrationReceipt, endpoint_from_udp_socket, remote_port_mapped_udp_socket,
};
#[cfg(windows)]
use super::socket::{bind_client_udp_socket, bind_server_udp_socket};
use super::stream::{DatagramFlowRegistry, IpTunnelRegistry};
use super::{
    CongestionMetrics, InstrumentedBbrConfig, InstrumentedController, QuicCandidateSelector,
    QuicCandidateVerifier, QuicCarrierError, QuicCarrierTelemetry, RecvStream, SendStream,
};
use crate::mux::MuxLimits;
use crate::transport::CarrierSocket;
use crate::transport::encrypted::{TcpClientTlsConfig, TcpServerTlsConfig};
use quinn::{ClientConfig, Endpoint as QuinnEndpoint, ServerConfig, TransportConfig, VarInt};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub struct Endpoint {
    endpoint: QuinnEndpoint,
    role: EndpointRole,
    mux_limits: MuxLimits,
}

#[derive(Clone)]
enum EndpointRole {
    Client {
        server_name: String,
        candidate_selector: QuicCandidateSelector,
    },
    Server {
        candidate_verifier: Arc<dyn QuicCandidateVerifier>,
    },
}

impl std::fmt::Debug for EndpointRole {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Client { server_name, .. } => formatter
                .debug_struct("EndpointRole::Client")
                .field("server_name", server_name)
                .finish_non_exhaustive(),
            Self::Server { .. } => formatter
                .debug_struct("EndpointRole::Server")
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Connection {
    connection: quinn::Connection,
    presentation: H3Presentation,
    native_datagrams: NativeDatagramHub,
    max_deferred_native_bytes: usize,
    max_datagram_flows: usize,
    write_backlog: Arc<AtomicU64>,
    telemetry: Arc<QuicCarrierTelemetry>,
}

impl Endpoint {
    pub async fn bind_server(
        addr: SocketAddr,
        tls: &TcpServerTlsConfig,
        candidate_verifier: Arc<dyn QuicCandidateVerifier>,
        mux_limits: MuxLimits,
    ) -> Result<Self, QuicCarrierError> {
        #[cfg(not(windows))]
        let endpoint = QuinnEndpoint::server(server_config(tls, mux_limits)?, addr)?;
        #[cfg(windows)]
        let endpoint = {
            let socket = bind_server_udp_socket(addr)?;
            endpoint_from_udp_socket(socket, Some(server_config(tls, mux_limits)?))?
        };
        Ok(Self {
            endpoint,
            role: EndpointRole::Server { candidate_verifier },
            mux_limits,
        })
    }

    pub async fn bind_client(
        addr: SocketAddr,
        tls: &TcpClientTlsConfig,
        candidate_selector: QuicCandidateSelector,
        mux_limits: MuxLimits,
    ) -> Result<Self, QuicCarrierError> {
        let server_name = tls
            .quic_server_name_text()
            .ok_or(QuicCarrierError::H3AuthorityRequiresDnsName)?;
        #[cfg(not(windows))]
        let mut endpoint = QuinnEndpoint::client(addr)?;
        #[cfg(windows)]
        let mut endpoint = {
            let socket = bind_client_udp_socket(addr)?;
            endpoint_from_udp_socket(socket, None)?
        };
        endpoint.set_default_client_config(client_config(tls, mux_limits)?);
        Ok(Self {
            endpoint,
            role: EndpointRole::Client {
                server_name,
                candidate_selector,
            },
            mux_limits,
        })
    }

    /// Builds Quinn on a socket already prepared by the host network adapter.
    pub async fn bind_client_socket(
        socket: CarrierSocket,
        tls: &TcpClientTlsConfig,
        candidate_selector: QuicCandidateSelector,
        mux_limits: MuxLimits,
    ) -> Result<Self, QuicCarrierError> {
        let server_name = tls
            .quic_server_name_text()
            .ok_or(QuicCarrierError::H3AuthorityRequiresDnsName)?;
        let mut endpoint = endpoint_from_udp_socket(socket.into_udp_socket()?, None)?;
        endpoint.set_default_client_config(client_config(tls, mux_limits)?);
        Ok(Self {
            endpoint,
            role: EndpointRole::Client {
                server_name,
                candidate_selector,
            },
            mux_limits,
        })
    }

    pub async fn connect(&self, remote: SocketAddr) -> Result<Connection, QuicCarrierError> {
        let EndpointRole::Client { server_name, .. } = &self.role else {
            panic!("only client QUIC endpoints initiate connections");
        };
        let connecting = self
            .endpoint
            .connect(remote, server_name)
            .map_err(QuicCarrierError::Connect)?;
        Connection::from_quinn(connecting.await?, self.role.clone(), self.mux_limits).await
    }

    /// Rebinds an established client endpoint through another externally
    /// forwarded destination port while retaining Quinn's peer locator.
    pub(crate) fn rebind_client_socket(
        &self,
        socket: CarrierSocket,
        canonical_remote: SocketAddr,
        selected_remote: SocketAddr,
    ) -> Result<RemotePortMigrationReceipt, QuicCarrierError> {
        if !matches!(self.role, EndpointRole::Client { .. }) {
            return Err(QuicCarrierError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "only client QUIC endpoints can migrate destination ports",
            )));
        }
        let (socket, receipt) = remote_port_mapped_udp_socket(
            socket.into_udp_socket()?,
            canonical_remote,
            selected_remote,
        )?;
        self.endpoint.rebind_abstract(socket)?;
        Ok(receipt)
    }

    pub async fn accept(&self) -> Option<Connection> {
        loop {
            let incoming = self.endpoint.accept().await?;
            match incoming.await {
                Ok(connection) => {
                    match Connection::from_quinn(connection, self.role.clone(), self.mux_limits)
                        .await
                    {
                        Ok(connection) => return Some(connection),
                        Err(_) => continue,
                    }
                }
                // Pre-authentication failures are expected on a public UDP
                // socket and are attacker-controlled. They must not allocate,
                // amplify logs, or appear as authenticated carrier faults.
                Err(_) => continue,
            }
        }
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.endpoint.local_addr()
    }
}

impl Connection {
    async fn from_quinn(
        connection: quinn::Connection,
        role: EndpointRole,
        mux_limits: MuxLimits,
    ) -> Result<Self, QuicCarrierError> {
        let telemetry = connection
            .congestion_state()
            .into_any()
            .downcast::<InstrumentedController>()
            .expect("QUIC carrier must use the instrumented congestion controller")
            .telemetry
            .clone();
        let concurrent_carrier_streams = mux_limits
            .max_quic_concurrent_bidi_streams
            .max(1)
            .min(mux_limits.max_streams.max(1));
        let native_route_queue = (mux_limits.max_datagram_queue_bytes / 1200).clamp(8, 256);
        let native_datagrams = NativeDatagramHub::new(
            connection.clone(),
            mux_limits.max_datagram_queue_bytes,
            concurrent_carrier_streams,
            native_route_queue,
        );
        let presentation = match role {
            EndpointRole::Client {
                server_name,
                candidate_selector,
            } => {
                H3Presentation::client(connection.clone(), server_name, candidate_selector).await?
            }
            EndpointRole::Server { candidate_verifier } => {
                H3Presentation::server(
                    connection.clone(),
                    concurrent_carrier_streams,
                    candidate_verifier,
                )
                .await?
            }
        };
        Ok(Self {
            connection,
            presentation,
            native_datagrams,
            max_deferred_native_bytes: mux_limits.max_datagram_queue_bytes,
            max_datagram_flows: mux_limits.max_streams,
            write_backlog: Arc::new(AtomicU64::new(0)),
            telemetry,
        })
    }

    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), QuicCarrierError> {
        let stream = self.presentation.open().await?;
        let known_datagram_flows = Arc::new(std::sync::Mutex::new(DatagramFlowRegistry::new(
            self.max_datagram_flows,
        )));
        let known_ip_tunnel = Arc::new(std::sync::Mutex::new(IpTunnelRegistry::new()));
        let native_send = self.native_datagrams.sender(stream.request_stream_id);
        let native_recv = self.native_datagrams.register(stream.request_stream_id)?;
        Ok((
            SendStream {
                stream: stream.send,
                native: native_send,
                write_backlog: self.write_backlog.clone(),
                telemetry: self.telemetry.clone(),
                known_datagram_flows: known_datagram_flows.clone(),
                known_ip_tunnel: known_ip_tunnel.clone(),
                priority: 0,
            },
            RecvStream::new(
                stream.recv,
                native_recv,
                known_datagram_flows,
                known_ip_tunnel,
                self.max_deferred_native_bytes,
            ),
        ))
    }

    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), QuicCarrierError> {
        let stream = self.presentation.accept().await?;
        let known_datagram_flows = Arc::new(std::sync::Mutex::new(DatagramFlowRegistry::new(
            self.max_datagram_flows,
        )));
        let known_ip_tunnel = Arc::new(std::sync::Mutex::new(IpTunnelRegistry::new()));
        let native_send = self.native_datagrams.sender(stream.request_stream_id);
        let native_recv = self.native_datagrams.register(stream.request_stream_id)?;
        Ok((
            SendStream {
                stream: stream.send,
                native: native_send,
                write_backlog: self.write_backlog.clone(),
                telemetry: self.telemetry.clone(),
                known_datagram_flows: known_datagram_flows.clone(),
                known_ip_tunnel: known_ip_tunnel.clone(),
                priority: 0,
            },
            RecvStream::new(
                stream.recv,
                native_recv,
                known_datagram_flows,
                known_ip_tunnel,
                self.max_deferred_native_bytes,
            ),
        ))
    }

    pub fn close(&self) {
        self.connection.close(
            VarInt::from_u32(h3::error::Code::H3_NO_ERROR.value() as u32),
            b"closed",
        );
    }

    #[cfg(test)]
    pub(super) fn native_datagram_routing_counts(&self) -> (usize, usize, u64) {
        self.native_datagrams.routing_counts()
    }

    pub fn is_closed(&self) -> bool {
        self.connection.close_reason().is_some()
    }

    pub async fn wait_closed(&self) {
        let _ = self.connection.closed().await;
    }

    pub fn is_locally_closed(&self) -> bool {
        matches!(
            self.connection.close_reason(),
            Some(quinn::ConnectionError::LocallyClosed)
        )
    }

    #[cfg(feature = "lab-diagnostics")]
    pub fn close_reason(&self) -> Option<String> {
        self.connection
            .close_reason()
            .map(|reason| reason.to_string())
    }

    pub fn stats(&self) -> quinn::ConnectionStats {
        self.connection.stats()
    }

    pub fn rtt(&self) -> std::time::Duration {
        self.connection.rtt()
    }

    pub fn congestion_metrics(&self) -> CongestionMetrics {
        let controller = self.connection.congestion_state();
        let metrics = controller.metrics();
        let instrumented = controller
            .into_any()
            .downcast::<InstrumentedController>()
            .expect("QUIC carrier must use the instrumented congestion controller");
        debug_assert!(
            Arc::ptr_eq(&instrumented.telemetry, &self.telemetry),
            "fresh QUIC paths must preserve the carrier telemetry owner"
        );
        let snapshot = instrumented.snapshot();
        CongestionMetrics {
            path_epoch: snapshot.path_epoch,
            congestion_window: metrics.congestion_window,
            bytes_in_flight: snapshot.bytes_in_flight,
            pending_bytes: self.write_backlog.load(Ordering::Relaxed),
            pacing_rate_bps: metrics.pacing_rate,
            loss_ppm: snapshot.loss_ppm,
            lost_bytes: snapshot.lost_bytes,
            ecn_ppm: None,
            newly_acked_bytes: snapshot.newly_acked_bytes,
            non_app_limited_acked_bytes: snapshot.non_app_limited_acked_bytes,
            timed_non_app_limited_acked_bytes: snapshot.timed_non_app_limited_acked_bytes,
            non_app_limited_ack_elapsed: snapshot.non_app_limited_ack_elapsed,
            delivery_evidence_written_bytes: self.telemetry.delivery_evidence_written_bytes(),
            delivery_evidence_cancelled_bytes: self.telemetry.delivery_evidence_cancelled_bytes(),
            delivery_evidence_pending_ack_bytes: self
                .telemetry
                .delivery_evidence_pending_ack_bytes(),
            delivery_evidence_newly_acked_bytes: snapshot.delivery_evidence_newly_acked_bytes,
            delivery_sample_count: snapshot.delivery_sample_count,
            non_app_limited_delivery_sample_count: snapshot.non_app_limited_delivery_sample_count,
            timed_non_app_limited_delivery_sample_count: snapshot
                .timed_non_app_limited_delivery_sample_count,
            app_limited: snapshot.app_limited,
        }
    }

    pub fn delivery_activity_notify(&self) -> Arc<Notify> {
        self.telemetry.delivery_activity_notify()
    }

    #[cfg(test)]
    pub fn negotiated_protocol(&self) -> Option<Vec<u8>> {
        self.connection
            .handshake_data()
            .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
            .and_then(|data| data.protocol)
    }
}

fn server_config(
    tls: &TcpServerTlsConfig,
    mux_limits: MuxLimits,
) -> Result<ServerConfig, QuicCarrierError> {
    let mut crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls.rustls_config())?;
    if let Some(secret) = tls.quic_initial_secret() {
        crypto.initial_packet_secret(secret);
    }
    let mut config = ServerConfig::with_crypto(Arc::new(crypto));
    config.transport = Arc::new(quic_transport_config(mux_limits, false)?);
    Ok(config)
}

fn client_config(
    tls: &TcpClientTlsConfig,
    mux_limits: MuxLimits,
) -> Result<ClientConfig, QuicCarrierError> {
    let mut crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls.rustls_config())?;
    if let Some(secret) = tls.quic_initial_secret() {
        crypto.initial_packet_secret(secret);
    }
    let mut config = ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(quic_transport_config(mux_limits, true)?));
    Ok(config)
}

fn quic_transport_config(
    mux_limits: MuxLimits,
    client_keep_alive: bool,
) -> Result<TransportConfig, QuicCarrierError> {
    let stream_receive_window = mux_limits.max_stream_window_bytes.max(1);
    let connection_receive_window = stream_receive_window
        .saturating_add(mux_limits.max_repair_bytes as u64)
        .saturating_add(mux_limits.max_reorder_bytes as u64)
        .saturating_add(mux_limits.max_datagram_queue_bytes as u64)
        .saturating_add(mux_limits.max_path_flight_bytes as u64);
    let send_window = (mux_limits.max_path_flight_bytes as u64)
        .max(mux_limits.max_reliable_relay_chunk_bytes as u64)
        .max(1);
    let concurrent_streams = mux_limits
        .max_quic_concurrent_bidi_streams
        .max(1)
        .min(mux_limits.max_streams.max(1)) as u64;

    let mut transport = TransportConfig::default();
    transport
        .stream_receive_window(varint_saturating(stream_receive_window))
        .receive_window(varint_saturating(connection_receive_window))
        .send_window(send_window)
        .max_concurrent_bidi_streams(varint_saturating(concurrent_streams))
        // RFC 9114 requires a control stream and both QPACK streams in each
        // direction. One additional short-lived reserved stream permits
        // standards-compliant H3 greasing without unbounded unidirectional
        // stream admission.
        .max_concurrent_uni_streams(4_u8.into())
        .datagram_receive_buffer_size(Some(mux_limits.max_datagram_queue_bytes))
        .datagram_send_buffer_size(mux_limits.max_datagram_queue_bytes)
        .max_idle_timeout(Some(mux_limits.quic_path_idle_timeout.try_into()?))
        .congestion_controller_factory(Arc::new(InstrumentedBbrConfig));
    if client_keep_alive {
        let maximum = mux_limits.quic_path_keep_alive_interval;
        transport.keep_alive_interval_range(maximum - maximum / 5, maximum);
    }
    Ok(transport)
}

fn varint_saturating(value: u64) -> VarInt {
    VarInt::from_u64(value.min(VarInt::MAX.into_inner()))
        .expect("bounded to QUIC variable integer range")
}

#[cfg(test)]
#[path = "tests_endpoint.rs"]
mod tests;
