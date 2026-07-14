//! Sender work vocabulary shared by request and response directions.

#[cfg(test)]
use super::*;
use crate::model::capacity::{PATH_OPEN_SCORE_BYTES, reliable_bulk_carrier_feed_quantum_bytes};
use crate::model::path::{CarrierPathKey, RelayPathInstance, RelayPathKey};
use crate::mux::MuxLimits;
use crate::protocol::Frame;
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommandSender, reliable_path_effective_frame_lane,
    reliable_path_stream_ordered_queue_lane,
};
use crate::runtime::relay::io::reliable_ack_gap_repair_delay;
use crate::scheduler::{FlowLane, PathSnapshot};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum CarrierEmitMode {
    Classified,
    StreamOrdered,
}

impl CarrierEmitMode {
    pub(in crate::runtime::sender) fn effective_lane(
        self,
        frame: &Frame,
        lane: FlowLane,
    ) -> FlowLane {
        match self {
            Self::Classified => reliable_path_effective_frame_lane(frame, lane),
            Self::StreamOrdered => reliable_path_stream_ordered_queue_lane(),
        }
    }

    pub(in crate::runtime::sender) fn try_enqueue_frame(
        self,
        commands: &ReliablePathCommandSender,
        frame: Frame,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        match self {
            Self::Classified => commands.try_enqueue_admitted_frame(frame, lane),
            Self::StreamOrdered => commands.try_enqueue_stream_ordered_frame(frame, lane),
        }
    }
}

pub(in crate::runtime) fn sender_extra_traffic_startup_floor_bytes(mux_limits: MuxLimits) -> usize {
    reliable_bulk_carrier_feed_quantum_bytes(mux_limits)
        .max(PATH_OPEN_SCORE_BYTES)
        .min(mux_limits.max_repair_bytes)
        .max(1)
}

pub(in crate::runtime) fn sender_repair_minimum_useful_attempt_bytes(
    mux_limits: MuxLimits,
) -> usize {
    PATH_OPEN_SCORE_BYTES
        .min(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
        .min(mux_limits.max_repair_bytes)
        .max(1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ClientRepairOutputIdentity {
    pub(in crate::runtime::sender) instance: RelayPathInstance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ServerRepairOutputIdentity {
    pub(in crate::runtime::sender) key: CarrierPathKey,
    pub(in crate::runtime::sender) incarnation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct PersistentClientAckGapBatch {
    pub(in crate::runtime::sender) target: ClientRepairOutputIdentity,
    pub(in crate::runtime::sender) expires_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct PersistentServerAckGapBatch {
    pub(in crate::runtime::sender) target: ServerRepairOutputIdentity,
    pub(in crate::runtime::sender) expires_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum RelaySendCause {
    StreamData,
    StreamFin,
    RecvProgress,
    RecvProgressRecovery,
    AckGapRepair,
    PersistentAckGapRepair,
    PersistentClientAckGapRepair(PersistentClientAckGapBatch),
    PersistentServerAckGapRepair(PersistentServerAckGapBatch),
    LiveOwnerTailRepair,
    PathFailureRepair,
}

impl RelaySendCause {
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) fn as_str(self) -> &'static str {
        match self {
            Self::StreamData => "stream_data",
            Self::StreamFin => "stream_fin",
            Self::RecvProgress => "recv_progress",
            Self::RecvProgressRecovery => "recv_progress_recovery",
            Self::AckGapRepair => "ack_gap_repair",
            Self::PersistentAckGapRepair
            | Self::PersistentClientAckGapRepair(_)
            | Self::PersistentServerAckGapRepair(_) => "persistent_ack_gap_repair",
            Self::LiveOwnerTailRepair => "live_owner_tail_repair",
            Self::PathFailureRepair => "path_failure_repair",
        }
    }

    pub(in crate::runtime::sender) fn is_repair(self) -> bool {
        matches!(
            self,
            Self::AckGapRepair
                | Self::PersistentAckGapRepair
                | Self::PersistentClientAckGapRepair(_)
                | Self::PersistentServerAckGapRepair(_)
                | Self::LiveOwnerTailRepair
                | Self::PathFailureRepair
        )
    }

    pub(in crate::runtime::sender) fn is_recv_progress(self) -> bool {
        matches!(self, Self::RecvProgress | Self::RecvProgressRecovery)
    }

    pub(in crate::runtime::sender) fn is_persistent_ack_gap_repair(self) -> bool {
        matches!(
            self,
            Self::PersistentAckGapRepair
                | Self::PersistentClientAckGapRepair(_)
                | Self::PersistentServerAckGapRepair(_)
        )
    }

    pub(in crate::runtime::sender) fn persistent_client_target(self) -> Option<RelayPathInstance> {
        match self {
            Self::PersistentClientAckGapRepair(batch) => Some(batch.target.instance),
            _ => None,
        }
    }

    pub(in crate::runtime::sender) fn persistent_server_target(
        self,
    ) -> Option<ServerRepairOutputIdentity> {
        match self {
            Self::PersistentServerAckGapRepair(batch) => Some(batch.target),
            _ => None,
        }
    }

    pub(in crate::runtime::sender) fn persistent_ack_gap_repair_expired(
        self,
        now: Instant,
    ) -> bool {
        match self {
            Self::PersistentClientAckGapRepair(batch) => now >= batch.expires_at,
            Self::PersistentServerAckGapRepair(batch) => now >= batch.expires_at,
            _ => false,
        }
    }

    pub(in crate::runtime::sender) fn persistent_ack_gap_repair_deadline(self) -> Option<Instant> {
        match self {
            Self::PersistentClientAckGapRepair(batch) => Some(batch.expires_at),
            Self::PersistentServerAckGapRepair(batch) => Some(batch.expires_at),
            _ => None,
        }
    }

    pub(in crate::runtime) fn persistent_client_ack_gap_repair(
        target: ClientRepairOutputIdentity,
        snapshot: PathSnapshot,
        lane: FlowLane,
    ) -> Self {
        Self::PersistentClientAckGapRepair(PersistentClientAckGapBatch {
            target,
            expires_at: Instant::now() + reliable_ack_gap_repair_delay(Some(snapshot), lane),
        })
    }

    pub(in crate::runtime) fn persistent_server_ack_gap_repair(
        target: ServerRepairOutputIdentity,
        snapshot: PathSnapshot,
        lane: FlowLane,
    ) -> Self {
        Self::PersistentServerAckGapRepair(PersistentServerAckGapBatch {
            target,
            expires_at: Instant::now() + reliable_ack_gap_repair_delay(Some(snapshot), lane),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RelaySendOutcome {
    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(in crate::runtime) path_key: RelayPathKey,
}
