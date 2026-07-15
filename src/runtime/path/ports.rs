//! Contracts crossing between carrier paths and product-stream ownership.
//!
//! Carriers publish accepted transport state here without constructing stream
//! policy objects. The stream layer consumes these values and owns offsets,
//! repair, and attachment behavior.

use super::commands::ReliablePathCommandSender;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, StreamId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::scheduler::{FlowLane, PathSnapshot};
use tokio::sync::mpsc;

/// Keeps a higher-layer reservation alive for exactly one queued carrier command.
///
/// The carrier only owns the lifetime contract; the reservation's policy and
/// release behavior remain in the layer that created the guard.
pub(in crate::runtime) struct CarrierCommandLease {
    _guard: Box<dyn Send + Sync>,
}

impl CarrierCommandLease {
    pub(in crate::runtime) fn hold<T>(guard: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self {
            _guard: Box::new(guard),
        }
    }
}

impl std::fmt::Debug for CarrierCommandLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CarrierCommandLease")
            .finish_non_exhaustive()
    }
}

/// Accepted client carrier state before product-stream ownership begins.
pub(in crate::runtime) struct OpenedReliableCarrierStream {
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) max_offset: u64,
    pub(in crate::runtime) lane: FlowLane,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) max_frame_payload_bytes: usize,
    pub(in crate::runtime) startup: PathSnapshot,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
}

/// QUIC-specific wire-open behavior selected by the relay open transaction.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct UdpStreamOpenOptions {
    pub(in crate::runtime) wait_for_accept: bool,
    pub(in crate::runtime) role: StreamOpenRole,
}

impl UdpStreamOpenOptions {
    pub(in crate::runtime) const ACTIVE_WAIT: Self = Self {
        wait_for_accept: true,
        role: StreamOpenRole::Active,
    };
}
