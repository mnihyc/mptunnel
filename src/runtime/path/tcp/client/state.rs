//! Shared state for the reliable TCP client actor.
//!
//! Algorithms may mutate these bounded state machines, but reconnect, stream,
//! receive, capacity, and writer modules do not own each other's lifetimes.

use super::super::capacity::RequestTcpCapacityProbeLease;
use super::super::group::{ClientTcpCarrierGroups, ClientTcpEndpointPolicy};
use super::connection::ClientTcpCarrierConnection;
use crate::config::ClientSecurityConfig;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::reliable_capacity_measurement_session_limit_bytes;
use crate::model::path::CarrierPathInstanceId;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::path_capacity::{CapacityReceiveTracker, PathCapacityReceiveError};
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, SessionId};
use crate::runtime::path::commands::TcpCapacityProbeCommand;
use crate::runtime::path::model::{path_metrics_from_snapshot, path_snapshot_with_id};
use crate::runtime::path::proof::PathProofTracker;
use crate::runtime::path::state::ClientPathState;
use crate::runtime::path::{AuthenticatedCarrierInventory, AuthenticatedCarrierRegistration};
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusCarrier, PeerStatusSnapshotSource};
use crate::scheduler::PathSnapshot;
use crate::transport::PathSpec;
use crate::transport::encrypted::TcpClientTlsConfig;
use crate::transport::{CarrierNetworkProvider, CarrierPathIdentity};
use std::sync::Arc;
use std::time::Instant;

pub(in crate::runtime::path::tcp) struct ClientTcpPathConnection {
    pub(in crate::runtime::path::tcp) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime::path::tcp) startup_snapshot: PathSnapshot,
    pub(in crate::runtime::path::tcp) startup_metrics: PathMetrics,
    pub(in crate::runtime::path::tcp) carrier: ClientTcpCarrierConnection,
    pub(in crate::runtime::path::tcp) peer_status: PeerStatusCarrier,
    authenticated_carrier: Option<AuthenticatedCarrierRegistration>,
    pub(in crate::runtime::path::tcp) path_proofs: PathProofTracker,
    pub(in crate::runtime::path::tcp) capacity: ClientTcpCapacityState,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::path::tcp) struct ClientTcpAttachmentEvidence {
    pub(in crate::runtime::path::tcp) snapshot: PathSnapshot,
    pub(in crate::runtime::path::tcp) metrics: PathMetrics,
}

impl ClientTcpPathConnection {
    pub(in crate::runtime::path::tcp) fn new(
        path_instance_id: CarrierPathInstanceId,
        startup_snapshot: PathSnapshot,
        startup_metrics: PathMetrics,
        carrier: ClientTcpCarrierConnection,
        peer_status: PeerStatusCarrier,
        mux_limits: MuxLimits,
    ) -> Self {
        Self {
            path_instance_id,
            startup_snapshot,
            startup_metrics,
            carrier,
            peer_status,
            authenticated_carrier: None,
            path_proofs: PathProofTracker::from_limits(mux_limits),
            capacity: ClientTcpCapacityState::new(mux_limits),
        }
    }

    pub(in crate::runtime::path::tcp) fn record_outbound_activity(&mut self) {
        self.carrier.refresh_liveness();
    }

    pub(in crate::runtime::path::tcp) fn retain_authenticated_carrier(
        &mut self,
        registration: AuthenticatedCarrierRegistration,
    ) {
        assert!(
            self.authenticated_carrier.is_none(),
            "TCP connection authenticated carrier published more than once"
        );
        self.authenticated_carrier = Some(registration);
    }
}

#[derive(Clone)]
pub(in crate::runtime) struct ClientTcpPathSessionRuntime {
    pub(in crate::runtime) paths: Arc<Vec<PathSpec>>,
    pub(in crate::runtime) config_index: usize,
    pub(in crate::runtime) path_index: usize,
    /// Wire identity exists only while one exact physical actor owns a group
    /// reservation.
    pub(in crate::runtime) path_id: Option<PathId>,
    pub(in crate::runtime) remote_port: Option<u16>,
    pub(in crate::runtime) carrier_identity: CarrierPathIdentity,
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) security: Arc<Vec<ClientSecurityConfig>>,
    pub(in crate::runtime) tls: Arc<Vec<TcpClientTlsConfig>>,
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) command_queue: usize,
    pub(in crate::runtime) stream_frame_queue: usize,
    pub(in crate::runtime) closed_stream_cache_capacity: usize,
    pub(in crate::runtime) session_retention_timeout: std::time::Duration,
    pub(in crate::runtime) state: Arc<ClientPathState>,
    pub(in crate::runtime) carrier_network: Arc<dyn CarrierNetworkProvider>,
    pub(in crate::runtime) peer_status: PeerStatusBroker,
    pub(in crate::runtime) peer_status_snapshot: PeerStatusSnapshotSource,
    pub(in crate::runtime) authenticated_carriers: AuthenticatedCarrierInventory,
    pub(in crate::runtime) endpoint_policy: Arc<ClientTcpEndpointPolicy>,
    pub(in crate::runtime) carrier_groups: Arc<ClientTcpCarrierGroups>,
}

impl ClientTcpPathSessionRuntime {
    pub(in crate::runtime) fn for_carrier(
        &self,
        path_id: PathId,
        remote_port: Option<u16>,
    ) -> Self {
        let mut runtime = self.clone();
        runtime.path_id = Some(path_id);
        runtime.remote_port = remote_port;
        runtime
    }

    pub(in crate::runtime) fn path_id(&self) -> PathId {
        self.path_id
            .expect("physical TCP actor owns one reserved wire PathId")
    }

    pub(in crate::runtime) fn path(&self) -> &PathSpec {
        self.paths
            .get(self.config_index)
            .expect("TCP session path inventory matches its index")
    }

    /// Captures one attachment's carrier evidence while exact physical-instance
    /// ownership can still be verified. If a replacement has already taken the
    /// stable member record, retain this actor's exact startup evidence instead
    /// of borrowing the replacement's measurements.
    pub(in crate::runtime::path::tcp) fn attachment_evidence(
        &self,
        connection: &ClientTcpPathConnection,
    ) -> ClientTcpAttachmentEvidence {
        let Some(observation) = self
            .state
            .tcp_path_observation_for_instance(self.path_index, connection.path_instance_id)
        else {
            return ClientTcpAttachmentEvidence {
                snapshot: connection.startup_snapshot,
                metrics: connection.startup_metrics,
            };
        };
        let snapshot = path_snapshot_with_id(self.path(), self.path_id(), observation);
        ClientTcpAttachmentEvidence {
            snapshot,
            metrics: path_metrics_from_snapshot(
                snapshot,
                observation,
                PathMetricDirection::ClientToServer,
            ),
        }
    }

    pub(in crate::runtime) fn security(&self) -> &ClientSecurityConfig {
        self.security
            .get(self.config_index)
            .expect("TCP session security inventory matches its index")
    }

    pub(in crate::runtime) fn tls(&self) -> &TcpClientTlsConfig {
        self.tls
            .get(self.config_index)
            .expect("TCP session TLS inventory matches its index")
    }

    pub(in crate::runtime::path::tcp) fn observe_sender_transport_state(
        &self,
        connection: &mut ClientTcpPathConnection,
        force: bool,
    ) {
        let Some(observation) = connection
            .carrier
            .tcp_metrics
            .as_mut()
            .and_then(|publisher| {
                publisher.maybe_observe(self.path_id(), PathMetricDirection::ClientToServer, force)
            })
        else {
            return;
        };
        #[cfg(feature = "lab-diagnostics")]
        {
            let bytes_in_flight = observation.bytes_in_flight().unwrap_or(0);
            let inflight_limit_bytes = observation.inflight_limit_bytes().unwrap_or(0);
            if observation.retransmission_advanced() == Some(true) {
                lab_diagnostic(
                    "tcp_native_retransmission",
                    format_args!(
                        "session_id={} path_index={} path_instance_id={} direction={:?}",
                        self.session_id.0,
                        self.path_index,
                        connection.path_instance_id.as_u64(),
                        observation.direction(),
                    ),
                );
            }
            lab_diagnostic(
                "tcp_sender_metrics",
                format_args!(
                    "path_index={} path_instance_id={} newly_acked_bytes={} acked_bytes_since_epoch={} retransmission_advanced={:?} delivery_rate_mbps={:.3} pacing_rate_mbps={:.3} bytes_in_flight={} inflight_limit={} queue_bytes={} app_limited={}",
                    self.path_index,
                    connection.path_instance_id.as_u64(),
                    observation.newly_acked_bytes().unwrap_or(0),
                    observation.acked_bytes_since_epoch().unwrap_or(0),
                    observation.retransmission_advanced(),
                    observation.delivery_rate_bps().unwrap_or(0) as f64 / 1_000_000.0,
                    observation.pacing_rate_bps().unwrap_or(0) as f64 / 1_000_000.0,
                    bytes_in_flight,
                    inflight_limit_bytes,
                    observation.queue_bytes().unwrap_or(0),
                    observation.app_limited().unwrap_or(true),
                ),
            );
        }
        let _ = self.state.mutate_path_eligibility(
            crate::model::path::RelayPathKey {
                underlay: crate::protocol::UnderlayProtocol::Tcp,
                index: self.path_index,
            },
            |record| {
                record.mark_tcp_transport_state(connection.path_instance_id, observation);
            },
        );
    }
}

/// Reliable-only measurement state must not leak into datagram carrier users.
pub(in crate::runtime::path::tcp) struct ClientTcpCapacityState {
    request_probe: Option<PendingClientTcpCapacityProbe>,
    discarded_request_receipt: Option<DiscardedClientTcpCapacityReceipt>,
    receive: CapacityReceiveTracker,
}

impl ClientTcpCapacityState {
    fn new(mux_limits: MuxLimits) -> Self {
        Self {
            request_probe: None,
            discarded_request_receipt: None,
            receive: CapacityReceiveTracker::new(
                reliable_capacity_measurement_session_limit_bytes(mux_limits),
            ),
        }
    }

    pub(in crate::runtime::path::tcp) fn request_deadline(&self) -> Option<tokio::time::Instant> {
        self.request_probe
            .as_ref()
            .map(|pending| tokio::time::Instant::from_std(pending.probe.expires_at))
    }

    pub(in crate::runtime::path::tcp) fn request_lease(
        &self,
    ) -> Option<RequestTcpCapacityProbeLease> {
        self.request_probe
            .as_ref()
            .map(|pending| pending.probe.request_lease().clone())
    }

    pub(in crate::runtime::path::tcp) fn has_pending_request(&self) -> bool {
        self.request_probe.is_some()
    }

    pub(in crate::runtime::path::tcp) fn publish_request(
        &mut self,
        probe: TcpCapacityProbeCommand,
        measurement: ClientTcpCapacityProbeMeasurement,
    ) {
        self.request_probe = Some(PendingClientTcpCapacityProbe { probe, measurement });
    }

    pub(in crate::runtime::path::tcp) fn discard_pending_receipt(&mut self) {
        let Some(pending) = self.request_probe.take() else {
            return;
        };
        self.discarded_request_receipt = Some(DiscardedClientTcpCapacityReceipt::from_probe(
            &pending.probe,
        ));
    }

    pub(in crate::runtime::path::tcp) fn is_idle(&self) -> bool {
        self.request_probe.is_none()
            && self.discarded_request_receipt.is_none()
            && self.receive.is_idle()
    }

    pub(in crate::runtime::path::tcp) fn record_received_data(
        &mut self,
        measurement_id: u64,
        payload_bytes: usize,
    ) -> Result<(), PathCapacityReceiveError> {
        self.receive.record_data(measurement_id, payload_bytes)
    }

    pub(in crate::runtime::path::tcp) fn finish_received_data(
        &mut self,
        measurement_id: u64,
        declared_bytes: u64,
    ) -> Result<u64, PathCapacityReceiveError> {
        self.receive.finish(measurement_id, declared_bytes)
    }

    pub(in crate::runtime::path::tcp) fn take_request_receipt(
        &mut self,
        measurement_id: u64,
        received_payload_bytes: u64,
    ) -> ClientTcpRequestReceipt {
        if let Some(pending) = self.request_probe.take() {
            return ClientTcpRequestReceipt::Active(pending);
        }
        if self
            .discarded_request_receipt
            .is_some_and(|discarded| discarded.matches(measurement_id, received_payload_bytes))
        {
            self.discarded_request_receipt = None;
            ClientTcpRequestReceipt::Discarded
        } else {
            ClientTcpRequestReceipt::Missing
        }
    }
}

pub(in crate::runtime::path::tcp) struct PendingClientTcpCapacityProbe {
    pub(in crate::runtime::path::tcp) probe: TcpCapacityProbeCommand,
    pub(in crate::runtime::path::tcp) measurement: ClientTcpCapacityProbeMeasurement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::path::tcp) struct ClientTcpCapacityProbeMeasurement {
    pub(in crate::runtime::path::tcp) proof_started_at: Instant,
    pub(in crate::runtime::path::tcp) train_wire_bytes: u64,
}

pub(in crate::runtime::path::tcp) enum ClientTcpRequestReceipt {
    Active(PendingClientTcpCapacityProbe),
    Discarded,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::path::tcp) struct DiscardedClientTcpCapacityReceipt {
    pub(in crate::runtime::path::tcp) measurement_id: u64,
    pub(in crate::runtime::path::tcp) train_payload_bytes: u64,
}

impl DiscardedClientTcpCapacityReceipt {
    fn from_probe(probe: &TcpCapacityProbeCommand) -> Self {
        Self {
            measurement_id: probe.measurement_id,
            train_payload_bytes: probe.train_payload_bytes,
        }
    }

    pub(in crate::runtime::path::tcp) fn matches(
        self,
        measurement_id: u64,
        received_payload_bytes: u64,
    ) -> bool {
        self.measurement_id == measurement_id && self.train_payload_bytes == received_payload_bytes
    }
}
