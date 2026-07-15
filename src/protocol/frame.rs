//! Intrinsic frame, offset-range, and byte-accounting semantics.
//!
//! These operations belong to the wire model because their answers depend
//! only on protocol fields, not on carrier state or scheduling policy.

use crate::protocol::{DatagramId, Frame, OffsetRange};

/// Maps one datagram identity to its single-item acknowledgement range.
pub(crate) fn datagram_ack_range(datagram_id: DatagramId) -> Option<OffsetRange> {
    let end = datagram_id.0.checked_add(1)?;
    OffsetRange::new(datagram_id.0, end)
}

/// Canonicalizes byte evidence for ledger and ACK operations.
///
/// Adjacent ranges are one continuous product interval, so they merge just as
/// overlapping ranges do; empty or inverted input carries no byte evidence.
pub(crate) fn normalized_offset_ranges(ranges: &[OffsetRange]) -> Vec<OffsetRange> {
    let mut ranges = ranges.to_vec();
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    let mut merged: Vec<OffsetRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.start >= range.end {
            continue;
        }
        match merged.last_mut() {
            Some(previous) if previous.end >= range.start => {
                previous.end = previous.end.max(range.end);
            }
            _ => merged.push(range),
        }
    }
    merged
}

/// Returns the product-byte extent carried by a non-empty stream data frame.
///
/// Datagram and capacity payloads have independent identity and accounting;
/// they must never enter the reliable product-range ledger.
pub(crate) fn reliable_stream_frame_extent(frame: &Frame) -> Option<(u64, u64, usize)> {
    let Frame::StreamData {
        offset, payload, ..
    } = frame
    else {
        return None;
    };
    let bytes = payload.len();
    if bytes == 0 {
        return None;
    }
    let end = offset.saturating_add(bytes as u64);
    Some((*offset, end, bytes))
}

/// Charges reliable-stream sender queues for product data or one control unit.
pub(crate) fn reliable_stream_frame_accounted_bytes(frame: &Frame) -> usize {
    match frame {
        Frame::StreamData { payload, .. } if !payload.is_empty() => payload.len(),
        _ => 1,
    }
}

/// Returns the payload pressure a reliable carrier should use for pacing.
pub(crate) fn reliable_path_frame_pacing_bytes(frame: &Frame) -> usize {
    match frame {
        Frame::StreamData { payload, .. } | Frame::PathCapacityData { payload, .. } => {
            payload.len().max(1)
        }
        Frame::StreamFin { .. }
        | Frame::StreamAck { .. }
        | Frame::StreamMaxData { .. }
        | Frame::StreamReset { .. }
        | Frame::StreamDetach { .. } => 1,
        _ => 0,
    }
}

/// Returns the explicitly proven zero-based prefix of an ordered range set.
///
/// ACK completeness controls what omitted higher ranges imply, not the prefix
/// stated by ranges that are present, so it is deliberately not an input here.
pub(crate) fn stream_ack_contiguous_frontier(ranges: &[OffsetRange]) -> u64 {
    ranges
        .first()
        .filter(|range| range.start == 0)
        .map_or(0, |range| range.end)
}

#[cfg(test)]
#[path = "frame_test.rs"]
mod tests;
