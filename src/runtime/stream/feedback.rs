//! Product receive-feedback state shared by both relay directions.
//!
//! This module decides when connection-level Data ACK and receive-window
//! updates are due. Carrier ACK and loss recovery remain owned by TCP or QUIC.

use crate::model::capacity::{
    QUIC_TIMER_GRANULARITY, reliable_stream_ack_update_bytes,
    reliable_stream_advertised_window_bytes, reliable_stream_max_data_update_bytes,
};
use crate::model::timing::transport_pto_from_snapshot;
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableRecvStream;
use crate::protocol::{Frame, UnderlayProtocol};
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::runtime) struct StreamMaxDataPublication {
    /// Latest shared offset accepted by at least one live carrier queue.
    pub(in crate::runtime) published_offset: Option<u64>,
    /// At least one live attachment still needs the retained latest value.
    pub(in crate::runtime) pending: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::runtime) struct StreamAckPublication {
    /// At least one ACK frame was accepted by a live attachment queue.
    pub(in crate::runtime) accepted: bool,
    /// At least one live attachment accepted the complete latest generation.
    pub(in crate::runtime) published: bool,
    /// At least one live attachment still needs the latest cumulative state.
    pub(in crate::runtime) pending: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::runtime) struct StreamAckAttachmentPublication {
    pub(in crate::runtime) accepted: bool,
    pub(in crate::runtime) published: bool,
}

/// Per-attachment publication fence for cumulative MPP Data ACK state.
///
/// The receive stream remains the sole range owner. This cursor retains only
/// generation and chunk position, so attachment fanout does not duplicate the
/// bounded receive-range ledger.
#[derive(Debug, Clone, Default)]
pub(in crate::runtime) struct StreamAckPublicationCursor {
    published_generation: u64,
    pending_generation: u64,
    next_cumulative_frame: usize,
}

impl StreamAckPublicationCursor {
    pub(in crate::runtime) fn publish_update<E>(
        &mut self,
        generation: u64,
        update_frames: &[Frame],
        cumulative_frames: &[Frame],
        mut enqueue: E,
    ) -> StreamAckAttachmentPublication
    where
        E: FnMut(Frame) -> bool,
    {
        debug_assert!(generation != 0);
        debug_assert!(!update_frames.is_empty());
        debug_assert!(!cumulative_frames.is_empty());
        debug_assert!(
            update_frames
                .iter()
                .chain(cumulative_frames)
                .all(|frame| matches!(frame, Frame::StreamAck { .. }))
        );
        if self.published_generation == generation {
            return StreamAckAttachmentPublication {
                accepted: false,
                published: true,
            };
        }

        let previous_generation = generation.wrapping_sub(1);
        if self.pending_generation == 0 && self.published_generation == previous_generation {
            let mut accepted = false;
            for frame in update_frames {
                if !enqueue(frame.clone()) {
                    self.pending_generation = generation;
                    self.next_cumulative_frame = 0;
                    return StreamAckAttachmentPublication {
                        accepted,
                        published: false,
                    };
                }
                accepted = true;
            }
            self.published_generation = generation;
            return StreamAckAttachmentPublication {
                accepted,
                published: true,
            };
        }

        self.retry_cumulative(generation, cumulative_frames, enqueue)
    }

    pub(in crate::runtime) fn retry_cumulative<E>(
        &mut self,
        generation: u64,
        cumulative_frames: &[Frame],
        mut enqueue: E,
    ) -> StreamAckAttachmentPublication
    where
        E: FnMut(Frame) -> bool,
    {
        debug_assert!(generation != 0);
        debug_assert!(!cumulative_frames.is_empty());
        debug_assert!(
            cumulative_frames
                .iter()
                .all(|frame| matches!(frame, Frame::StreamAck { .. }))
        );
        if self.published_generation == generation {
            self.pending_generation = 0;
            self.next_cumulative_frame = 0;
            return StreamAckAttachmentPublication {
                accepted: false,
                published: true,
            };
        }
        if self.pending_generation != generation {
            self.pending_generation = generation;
            self.next_cumulative_frame = 0;
        }

        let mut accepted = false;
        while let Some(frame) = cumulative_frames.get(self.next_cumulative_frame) {
            if !enqueue(frame.clone()) {
                return StreamAckAttachmentPublication {
                    accepted,
                    published: false,
                };
            }
            accepted = true;
            self.next_cumulative_frame = self.next_cumulative_frame.saturating_add(1);
        }
        self.published_generation = generation;
        self.pending_generation = 0;
        self.next_cumulative_frame = 0;
        StreamAckAttachmentPublication {
            accepted,
            published: true,
        }
    }

    pub(in crate::runtime) fn is_pending(&self, generation: u64) -> bool {
        generation != 0 && self.published_generation != generation
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::runtime) struct ReliableRecvProgress {
    last_max_data_offset: u64,
    last_max_data_window_bytes: u64,
    ack_generation: u64,
    last_ack_offset: u64,
    last_ack_reorder_bytes: usize,
    last_ack_range_count: usize,
    last_ack_largest_end: u64,
    last_ack_at: Option<Instant>,
}

impl ReliableRecvProgress {
    pub(in crate::runtime) fn has_sent_ack(&self) -> bool {
        self.last_ack_at.is_some()
    }

    pub(in crate::runtime) fn ack_generation(&self) -> u64 {
        self.ack_generation
    }

    pub(in crate::runtime) fn last_ack_at(&self) -> Option<Instant> {
        self.last_ack_at
    }

    pub(in crate::runtime) fn should_send_ack(
        &mut self,
        recv_stream: &ReliableRecvStream,
        path: Option<PathSnapshot>,
        traffic_class: TrafficClass,
        mux_limits: MuxLimits,
        force: bool,
    ) -> bool {
        let now = Instant::now();
        let next_offset = recv_stream.next_offset();
        let reorder_bytes = recv_stream.reorder_bytes();
        let ack_summary = recv_stream.ack_range_summary();
        let range_count = ack_summary.count;
        let largest_end = ack_summary.largest_end;
        let has_progress = next_offset > 0 || reorder_bytes > 0;
        let first_ack = self.last_ack_at.is_none() && has_progress;
        let cumulative_state_changed = self.ack_generation == 0
            || next_offset != self.last_ack_offset
            || reorder_bytes != self.last_ack_reorder_bytes
            || range_count != self.last_ack_range_count
            || largest_end != self.last_ack_largest_end;
        let ack_step = reliable_stream_ack_update_bytes(path, traffic_class, mux_limits);
        let horizon_advanced = largest_end.saturating_sub(self.last_ack_largest_end) >= ack_step;
        let reorder_delta = reorder_bytes.abs_diff(self.last_ack_reorder_bytes) as u64 >= ack_step;
        let gap_state_changed = reorder_bytes > 0
            && (range_count != self.last_ack_range_count || horizon_advanced || reorder_delta);
        let delivered_since_ack = next_offset.saturating_sub(self.last_ack_offset);
        let enough_delivered = delivered_since_ack >= ack_step;
        let ack_timer_elapsed = self.last_ack_at.is_some_and(|last_ack_at| {
            now.saturating_duration_since(last_ack_at)
                >= reliable_stream_recv_progress_interval(path)
        });
        if force
            || first_ack
            || gap_state_changed
            || enough_delivered
            || (has_progress && delivered_since_ack > 0 && ack_timer_elapsed)
        {
            if cumulative_state_changed {
                self.ack_generation = self.ack_generation.wrapping_add(1);
                if self.ack_generation == 0 {
                    self.ack_generation = 1;
                }
            }
            self.last_ack_offset = next_offset;
            self.last_ack_reorder_bytes = reorder_bytes;
            self.last_ack_range_count = range_count;
            self.last_ack_largest_end = largest_end;
            self.last_ack_at = Some(now);
            true
        } else {
            false
        }
    }

    pub(in crate::runtime) fn should_send_max_data(
        &mut self,
        recv_stream: &ReliableRecvStream,
        path: Option<PathSnapshot>,
        traffic_class: TrafficClass,
        mux_limits: MuxLimits,
        force: bool,
    ) -> bool {
        let window_bytes = reliable_stream_advertised_window_bytes(path, traffic_class, mux_limits);
        let max_offset = recv_stream.max_data_offset_with_window(window_bytes);
        let update_step = reliable_stream_max_data_update_bytes(window_bytes, mux_limits);
        let window_changed = self.last_max_data_window_bytes != 0
            && window_bytes.abs_diff(self.last_max_data_window_bytes) >= update_step;
        if force
            || self.last_max_data_offset == 0
            || window_changed
            || max_offset.saturating_sub(self.last_max_data_offset) >= update_step
        {
            self.last_max_data_offset = max_offset;
            self.last_max_data_window_bytes = window_bytes;
            true
        } else {
            false
        }
    }
}

pub(in crate::runtime) fn reliable_relay_recv_progress_resend_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    active_underlay: Option<UnderlayProtocol>,
) -> bool {
    remote_open
        && match active_underlay {
            Some(UnderlayProtocol::Udp) => {
                recv_stream.next_offset() > 0 || recv_stream.reorder_bytes() > 0
            }
            Some(UnderlayProtocol::Tcp) => recv_stream.reorder_bytes() > 0,
            None => false,
        }
}

pub(in crate::runtime) fn reliable_stream_recv_progress_interval(
    path: Option<PathSnapshot>,
) -> Duration {
    transport_pto_from_snapshot(path)
        .div_f64(2.0)
        .max(QUIC_TIMER_GRANULARITY)
}
