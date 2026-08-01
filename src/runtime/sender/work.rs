//! Sender work vocabulary shared by request and response directions.

use crate::model::capacity::{PATH_OPEN_SCORE_BYTES, reliable_bulk_carrier_feed_quantum_bytes};
use crate::model::path::{CarrierPathKey, RelayPathInstance, RelayPathKey};
use crate::model::tcp_carrier::TcpCarrierStableGenerations;
use crate::model::timing::reliable_data_retransmission_interval;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, StreamId};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{ReliablePathCommandSender, ReliablePathFrameReservation};
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::num::NonZeroU64;
use std::time::Instant;

/// Exact logical Product workload in one sender direction.
///
/// The session owner allocates the generation when the logical stream enters
/// that sender's workload set. Every admission fence and ACK receipt uses this
/// same identity; no second synthetic stream generation exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::runtime) struct ProductWorkloadIdentity {
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) lifecycle_generation: NonZeroU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum CarrierEmitMode {
    Classified,
    StreamOrdered,
}

impl CarrierEmitMode {
    pub(in crate::runtime::sender) fn try_reserve_frame<'a>(
        self,
        commands: &'a ReliablePathCommandSender,
        frame: Frame,
        lane: TrafficClass,
    ) -> Result<ReliablePathFrameReservation<'a>, RuntimeError> {
        match self {
            Self::Classified => commands.try_reserve_admitted_frame(frame, lane),
            Self::StreamOrdered => commands.try_reserve_stream_ordered_frame(frame, lane),
        }
    }

    pub(in crate::runtime::sender) fn try_enqueue_frame(
        self,
        commands: &ReliablePathCommandSender,
        frame: Frame,
        lane: TrafficClass,
    ) -> Result<(), RuntimeError> {
        let reservation = self.try_reserve_frame(commands, frame, lane)?;
        reservation.commit();
        Ok(())
    }
}

pub(in crate::runtime) fn sender_extra_traffic_startup_floor_bytes(mux_limits: MuxLimits) -> usize {
    reliable_bulk_carrier_feed_quantum_bytes(mux_limits)
        .max(PATH_OPEN_SCORE_BYTES)
        .min(mux_limits.max_repair_bytes)
        .max(1)
}

pub(in crate::runtime) fn sender_reinjection_minimum_useful_attempt_bytes(
    mux_limits: MuxLimits,
) -> usize {
    PATH_OPEN_SCORE_BYTES
        .min(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
        .min(mux_limits.max_repair_bytes)
        .max(1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ClientReinjectionOutputIdentity {
    pub(in crate::runtime::sender) instance: RelayPathInstance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(in crate::runtime) struct ServerReinjectionOutputIdentity {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) incarnation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct PersistentClientAckGapBatch {
    pub(in crate::runtime::sender) target: ClientReinjectionOutputIdentity,
    pub(in crate::runtime::sender) expires_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct PersistentServerAckGapBatch {
    pub(in crate::runtime::sender) target: ServerReinjectionOutputIdentity,
    pub(in crate::runtime::sender) expires_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum RelaySendCause {
    StreamData,
    StreamFin,
    AckGapReinjection,
    PersistentAckGapReinjection,
    PersistentClientAckGapReinjection(PersistentClientAckGapBatch),
    PersistentServerAckGapReinjection(PersistentServerAckGapBatch),
    TailReinjection,
    PathFailureReinjection,
    StalePathReinjection(RelayPathInstance),
    StaleResponsePathReinjection(ServerReinjectionOutputIdentity),
}

impl RelaySendCause {
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) fn as_str(self) -> &'static str {
        match self {
            Self::StreamData => "stream_data",
            Self::StreamFin => "stream_fin",
            Self::AckGapReinjection => "ack_gap_reinjection",
            Self::PersistentAckGapReinjection
            | Self::PersistentClientAckGapReinjection(_)
            | Self::PersistentServerAckGapReinjection(_) => "persistent_ack_gap_reinjection",
            Self::TailReinjection => "tail_reinjection",
            Self::PathFailureReinjection => "path_failure_reinjection",
            Self::StalePathReinjection(_) | Self::StaleResponsePathReinjection(_) => {
                "stale_path_reinjection"
            }
        }
    }

    pub(in crate::runtime::sender) fn is_reinjection(self) -> bool {
        matches!(
            self,
            Self::AckGapReinjection
                | Self::PersistentAckGapReinjection
                | Self::PersistentClientAckGapReinjection(_)
                | Self::PersistentServerAckGapReinjection(_)
                | Self::TailReinjection
                | Self::PathFailureReinjection
                | Self::StalePathReinjection(_)
                | Self::StaleResponsePathReinjection(_)
        )
    }

    pub(in crate::runtime::sender) fn is_persistent_ack_gap_reinjection(self) -> bool {
        matches!(
            self,
            Self::PersistentAckGapReinjection
                | Self::PersistentClientAckGapReinjection(_)
                | Self::PersistentServerAckGapReinjection(_)
        )
    }

    pub(in crate::runtime::sender) fn is_ack_gap_reinjection(self) -> bool {
        matches!(self, Self::AckGapReinjection) || self.is_persistent_ack_gap_reinjection()
    }

    pub(in crate::runtime::sender) fn persistent_client_target(self) -> Option<RelayPathInstance> {
        match self {
            Self::PersistentClientAckGapReinjection(batch) => Some(batch.target.instance),
            _ => None,
        }
    }

    pub(in crate::runtime::sender) fn persistent_server_target(
        self,
    ) -> Option<ServerReinjectionOutputIdentity> {
        match self {
            Self::PersistentServerAckGapReinjection(batch) => Some(batch.target),
            _ => None,
        }
    }

    pub(in crate::runtime::sender) fn persistent_ack_gap_reinjection_expired(
        self,
        now: Instant,
    ) -> bool {
        match self {
            Self::PersistentClientAckGapReinjection(batch) => now >= batch.expires_at,
            Self::PersistentServerAckGapReinjection(batch) => now >= batch.expires_at,
            _ => false,
        }
    }

    pub(in crate::runtime::sender) fn persistent_ack_gap_reinjection_deadline(
        self,
    ) -> Option<Instant> {
        match self {
            Self::PersistentClientAckGapReinjection(batch) => Some(batch.expires_at),
            Self::PersistentServerAckGapReinjection(batch) => Some(batch.expires_at),
            _ => None,
        }
    }

    pub(in crate::runtime) fn persistent_client_ack_gap_reinjection(
        target: ClientReinjectionOutputIdentity,
        snapshot: PathSnapshot,
    ) -> Self {
        Self::PersistentClientAckGapReinjection(PersistentClientAckGapBatch {
            target,
            expires_at: Instant::now()
                + reliable_data_retransmission_interval(None, Some(snapshot)),
        })
    }

    pub(in crate::runtime) fn persistent_server_ack_gap_reinjection(
        target: ServerReinjectionOutputIdentity,
        snapshot: PathSnapshot,
    ) -> Self {
        Self::PersistentServerAckGapReinjection(PersistentServerAckGapBatch {
            target,
            expires_at: Instant::now()
                + reliable_data_retransmission_interval(None, Some(snapshot)),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RelaySendOutcome {
    pub(in crate::runtime) tcp_carrier_stable: Option<TcpCarrierStableGenerations>,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) path_key: RelayPathKey,
}
