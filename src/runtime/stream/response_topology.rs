//! Response attachment identities and output-local state.
//! Ranking and carrier recovery consume this topology without belonging to it.

use super::ServerCarrierPathInstanceId;
use super::response_ack_clock::{ResponseAckClockCalibrationState, ResponseAckClockRateEvidence};
use super::response_evidence::ServerPathMetricsEntry;
use crate::model::path::CarrierPathKey;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::SessionId;
use crate::protocol::StreamOpenRole;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::runtime::path::quic::metrics::QuicCapacityProofCandidate;
use crate::scheduler::PathSnapshot;
use std::collections::HashMap;

/// One carrier output attached to a response stream.
///
/// It owns carrier command access and sender-evidence fields for this stream on
/// this path. Product repair and ordering identity stay in `ResponseStreamBinding`.
#[derive(Clone)]
pub(in crate::runtime) struct ResponseStreamOutputEntry {
    pub(super) key: CarrierPathKey,
    pub(super) path_instance_id: ServerCarrierPathInstanceId,
    pub(super) incarnation: u64,
    pub(super) commands: ReliablePathCommandSender,
    pub(super) role: StreamOpenRole,
    /// Unacknowledged unique OwnerData assigned to this response output.
    /// Repair copies remain in `bytes_in_flight` but never enter this counter.
    pub(super) owner_data_in_flight_bytes: u64,
    pub(super) bytes_in_flight: u64,
    pub(super) product_queue_bytes: u64,
    pub(super) product_progress_rate_bps: Option<f64>,
    pub(super) delivery_rate_bps: Option<f64>,
    /// TCP per-flow goodput from exact OwnerData ACKs. It is not carrier
    /// capacity; assignment-time evidence never publishes a rate or RTT.
    pub(super) tcp_ack_clock_rate_bps: Option<f64>,
    /// Per-output ACK clock; product ordering timestamps can be advanced when a
    /// different path closes a hole and therefore cannot own this boundary.
    pub(super) tcp_product_rate_evidence: Option<ResponseAckClockRateEvidence>,
    /// Temporary carrier-capacity estimate. It may come from a bounded Service
    /// opportunity or exclusive calibration; ordinary exact-ACK evidence must
    /// mature in a separate epoch before replacing it.
    pub(super) tcp_capacity_prior: Option<TcpResponseCapacityPrior>,
    pub(super) srtt_ms: Option<f64>,
    pub(super) delivery_samples: u32,
    /// Cumulative uniquely owned product bytes ACKed on this output.
    ///
    /// The flight ledger increments this only for unambiguous `OwnerData`;
    /// duplicated `RepairData` never contributes.
    pub(super) owner_data_acked_bytes: u64,
    pub(super) local_path_metrics: Option<ServerPathMetricsEntry>,
    pub(super) peer_path_metrics: Option<ServerPathMetricsEntry>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct TcpResponseCapacityPrior {
    pub(super) rate_bps: f64,
    pub(super) ordinary_windows: u32,
}

pub(in crate::runtime) struct ResponseStreamOutputs {
    pub(super) entries: Vec<ResponseStreamOutputEntry>,
    pub(super) ack_clock_calibrations:
        HashMap<(CarrierPathKey, u64), ResponseAckClockCalibrationState>,
    pub(super) active_ack_clock_calibration: Option<(CarrierPathKey, u64)>,
}

#[derive(Clone)]
pub(in crate::runtime) struct ResponseSenderPathTarget {
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) session_id: SessionId,
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) binding_instance_id: u64,
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) path_instance_id: ServerCarrierPathInstanceId,
    pub(in crate::runtime) incarnation: u64,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) attachment_role: StreamOpenRole,
    pub(in crate::runtime) snapshot: PathSnapshot,
    pub(in crate::runtime) owner_data_in_flight_bytes: u64,
    /// Once-captured command pressure used by both projection and commit
    /// revalidation; equality is a value fingerprint, not a queue generation.
    pub(in crate::runtime) command_pending_bytes: u64,
    pub(in crate::runtime) eta_ms: f64,
    /// True only for the persistent response Service snapshot.
    pub(in crate::runtime) is_active: bool,
    /// Request-side Active is independent from response Service ownership.
    pub(in crate::runtime) is_request_active: bool,
    pub(in crate::runtime) has_sender_evidence: bool,
    /// Current-Service feed may use unique product ACK progress or durable
    /// app-limited carrier ACK progress; optional paths still require strict
    /// bulk-rate evidence below.
    pub(in crate::runtime) has_service_feed_evidence: bool,
    pub(in crate::runtime) has_bulk_rate_evidence: bool,
    /// Endpoint-only configuration plus an immature candidate ACK model may
    /// use Service only as a bounded calibration-opportunity prior.
    pub(in crate::runtime) endpoint_only_service_prior_eligible: bool,
    /// Raw receipt marker; handoff may pin it without renewing global freshness.
    pub(in crate::runtime) quic_capacity_proof: Option<QuicCapacityProofCandidate>,
    pub(in crate::runtime) quic_capacity_calibration_attempts: u8,
    pub(in crate::runtime) ack_clock_calibration_eligible: bool,
    pub(in crate::runtime) ack_clock_calibration_proven: bool,
    pub(in crate::runtime) ack_clock_calibration_spent_bytes: u64,
    pub(in crate::runtime) ack_clock_calibration_credit_limit_bytes: u64,
    pub(in crate::runtime) ack_clock_calibration_max_limit_bytes: u64,
    pub(in crate::runtime) ack_clock_calibration_active: bool,
}

/// Compact identity retained after path ranking. Model snapshots and
/// calibration state are intentionally dropped before the per-frame emit path.
#[derive(Clone)]
pub(in crate::runtime) struct ResponseDispatchTarget {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) path_instance_id: ServerCarrierPathInstanceId,
    pub(in crate::runtime) incarnation: u64,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) attachment_role: StreamOpenRole,
    pub(in crate::runtime) has_bulk_rate_evidence: bool,
}

impl From<ResponseSenderPathTarget> for ResponseDispatchTarget {
    fn from(target: ResponseSenderPathTarget) -> Self {
        Self {
            key: target.key,
            path_instance_id: target.path_instance_id,
            incarnation: target.incarnation,
            commands: target.commands,
            attachment_role: target.attachment_role,
            has_bulk_rate_evidence: target.has_bulk_rate_evidence,
        }
    }
}

impl From<&ResponseSenderPathTarget> for ResponseDispatchTarget {
    fn from(target: &ResponseSenderPathTarget) -> Self {
        Self {
            key: target.key,
            path_instance_id: target.path_instance_id,
            incarnation: target.incarnation,
            commands: target.commands.clone(),
            attachment_role: target.attachment_role,
            has_bulk_rate_evidence: target.has_bulk_rate_evidence,
        }
    }
}
