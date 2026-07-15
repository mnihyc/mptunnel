//! Shared state for the reliable TCP client actor.
//!
//! Algorithms may mutate these bounded state machines, but reconnect, stream,
//! receive, capacity, and writer modules do not own each other's lifetimes.

use super::capacity::RequestTcpCapacityProbeLease;
use super::client_connection::ClientTcpCarrierConnection;
use crate::config::SecurityConfig;
use crate::model::capacity::reliable_capacity_calibration_session_limit_bytes;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::path_capacity::{CapacityReceiveTracker, PathCapacityReceiveError};
use crate::protocol::{PathMetrics, SessionId};
use crate::runtime::path::commands::TcpCapacityProbeCommand;
use crate::runtime::path::proof::PathProofTracker;
use crate::runtime::path::state::ClientPathState;
use crate::scheduler::PathSnapshot;
use crate::transport::PathSpec;
use crate::transport::{CarrierNetworkProvider, CarrierPathIdentity};
use std::sync::Arc;
use std::time::Instant;

pub(super) struct ClientTcpPathConnection {
    pub(super) startup_snapshot: PathSnapshot,
    pub(super) startup_metrics: PathMetrics,
    pub(super) carrier: ClientTcpCarrierConnection,
    pub(super) path_proofs: PathProofTracker,
    pub(super) capacity: ClientTcpCapacityState,
}

impl ClientTcpPathConnection {
    pub(super) fn new(
        startup_snapshot: PathSnapshot,
        startup_metrics: PathMetrics,
        carrier: ClientTcpCarrierConnection,
        mux_limits: MuxLimits,
    ) -> Self {
        Self {
            startup_snapshot,
            startup_metrics,
            carrier,
            path_proofs: PathProofTracker::default(),
            capacity: ClientTcpCapacityState::new(mux_limits),
        }
    }

    pub(super) fn record_outbound_activity(&mut self) {
        self.carrier.refresh_liveness();
    }
}

#[derive(Clone)]
pub(in crate::runtime) struct ClientTcpPathSessionRuntime {
    pub(in crate::runtime) path: PathSpec,
    pub(in crate::runtime) path_index: usize,
    pub(in crate::runtime) carrier_identity: CarrierPathIdentity,
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) security: SecurityConfig,
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) command_queue: usize,
    pub(in crate::runtime) stream_frame_queue: usize,
    pub(in crate::runtime) closed_stream_cache_capacity: usize,
    pub(in crate::runtime) state: Arc<ClientPathState>,
    pub(in crate::runtime) carrier_network: Arc<dyn CarrierNetworkProvider>,
}

/// Reliable-only calibration state must not leak into datagram carrier users.
pub(super) struct ClientTcpCapacityState {
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
                reliable_capacity_calibration_session_limit_bytes(mux_limits),
            ),
        }
    }

    pub(super) fn request_deadline(&self) -> Option<tokio::time::Instant> {
        self.request_probe
            .as_ref()
            .map(|pending| tokio::time::Instant::from_std(pending.probe.expires_at))
    }

    pub(super) fn request_lease(&self) -> Option<RequestTcpCapacityProbeLease> {
        self.request_probe
            .as_ref()
            .and_then(|pending| pending.probe.request_lease())
            .cloned()
    }

    pub(super) fn has_pending_request(&self) -> bool {
        self.request_probe.is_some()
    }

    pub(super) fn publish_request(
        &mut self,
        probe: TcpCapacityProbeCommand,
        measurement: ClientTcpCapacityProbeMeasurement,
    ) {
        self.request_probe = Some(PendingClientTcpCapacityProbe { probe, measurement });
    }

    pub(super) fn discard_pending_receipt(&mut self) {
        let Some(pending) = self.request_probe.take() else {
            return;
        };
        self.discarded_request_receipt = Some(DiscardedClientTcpCapacityReceipt::from_probe(
            &pending.probe,
        ));
    }

    pub(super) fn record_received_data(
        &mut self,
        calibration_id: u64,
        payload_bytes: usize,
    ) -> Result<(), PathCapacityReceiveError> {
        self.receive.record_data(calibration_id, payload_bytes)
    }

    pub(super) fn finish_received_data(
        &mut self,
        calibration_id: u64,
        declared_bytes: u64,
    ) -> Result<u64, PathCapacityReceiveError> {
        self.receive.finish(calibration_id, declared_bytes)
    }

    pub(super) fn take_request_receipt(
        &mut self,
        calibration_id: u64,
        received_payload_bytes: u64,
    ) -> ClientTcpRequestReceipt {
        if let Some(pending) = self.request_probe.take() {
            return ClientTcpRequestReceipt::Active(pending);
        }
        if self
            .discarded_request_receipt
            .is_some_and(|discarded| discarded.matches(calibration_id, received_payload_bytes))
        {
            self.discarded_request_receipt = None;
            ClientTcpRequestReceipt::Discarded
        } else {
            ClientTcpRequestReceipt::Missing
        }
    }
}

pub(super) struct PendingClientTcpCapacityProbe {
    pub(super) probe: TcpCapacityProbeCommand,
    pub(super) measurement: ClientTcpCapacityProbeMeasurement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ClientTcpCapacityProbeMeasurement {
    pub(super) proof_started_at: Instant,
    pub(super) train_wire_bytes: u64,
}

pub(super) enum ClientTcpRequestReceipt {
    Active(PendingClientTcpCapacityProbe),
    Discarded,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DiscardedClientTcpCapacityReceipt {
    pub(super) calibration_id: u64,
    pub(super) train_payload_bytes: u64,
}

impl DiscardedClientTcpCapacityReceipt {
    fn from_probe(probe: &TcpCapacityProbeCommand) -> Self {
        Self {
            calibration_id: probe.calibration_id,
            train_payload_bytes: probe.train_payload_bytes,
        }
    }

    pub(super) fn matches(self, calibration_id: u64, received_payload_bytes: u64) -> bool {
        self.calibration_id == calibration_id && self.train_payload_bytes == received_payload_bytes
    }
}
