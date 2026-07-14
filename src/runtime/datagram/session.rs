//! Carrier-neutral state shared by TCP and QUIC datagram sessions.

use crate::mux::datagram::DatagramFlow;
use crate::protocol::{DatagramFlowId, DatagramId, OffsetRange, TargetAddr};
use crate::runtime::error::RuntimeError;
use std::time::{Duration, Instant};

/// Associates one product destination with its framed datagram flow.
pub(super) struct DatagramClientFlow {
    pub(super) target: TargetAddr,
    pub(super) flow: DatagramFlow,
    pub(super) flow_id: DatagramFlowId,
}

/// Retains only the evidence both carriers need until delivery feedback arrives.
#[derive(Debug, Clone, Copy)]
pub(super) struct SentDatagram {
    pub(super) sent_at: Instant,
    pub(super) bytes: usize,
    pub(super) ttl: Duration,
}

pub(in crate::runtime) fn datagram_ack_range(
    datagram_id: DatagramId,
) -> Result<OffsetRange, RuntimeError> {
    let end = datagram_id
        .0
        .checked_add(1)
        .ok_or(RuntimeError::Protocol("datagram ACK range overflow"))?;
    OffsetRange::new(datagram_id.0, end).ok_or(RuntimeError::Protocol("invalid datagram ACK range"))
}

pub(super) fn datagram_id_is_in_ranges(datagram_id: DatagramId, ranges: &[OffsetRange]) -> bool {
    ranges
        .iter()
        .any(|range| datagram_id.0 >= range.start && datagram_id.0 < range.end)
}
