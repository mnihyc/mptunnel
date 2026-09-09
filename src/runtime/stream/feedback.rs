//! Product receive-feedback state shared by both relay directions.
//!
//! This module decides when connection-level Data ACK and receive-window
//! updates are due. Carrier ACK and loss recovery remain owned by TCP or QUIC.

use crate::model::capacity::{
    QUIC_TIMER_GRANULARITY, reliable_stream_ack_update_bytes,
    reliable_stream_advertised_window_bytes,
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
    ack_generation: u64,
    last_ack_offset: u64,
    last_ack_reorder_bytes: usize,
    last_ack_range_count: usize,
    last_ack_largest_end: u64,
    /// Changed logical materialization, not unchanged retries or MAX service.
    last_ack_at: Option<Instant>,
    pending_ack_deadline: Option<Instant>,
}

impl ReliableRecvProgress {
    pub(in crate::runtime) fn ack_generation(&self) -> u64 {
        self.ack_generation
    }

    pub(in crate::runtime) fn last_ack_at(&self) -> Option<Instant> {
        self.last_ack_at
    }

    pub(in crate::runtime) fn ack_update_pending(&self) -> bool {
        self.pending_ack_deadline.is_some()
    }

    /// Unmaterialized receipt service, independent of attachment retry debt.
    pub(in crate::runtime) fn pending_ack_deadline(&self) -> Option<Instant> {
        self.pending_ack_deadline
    }

    pub(in crate::runtime) fn should_send_ack(
        &mut self,
        recv_stream: &ReliableRecvStream,
        path: Option<PathSnapshot>,
        traffic_class: TrafficClass,
        mux_limits: MuxLimits,
        force: bool,
    ) -> bool {
        self.should_send_ack_at(
            recv_stream,
            path,
            traffic_class,
            mux_limits,
            force,
            Instant::now(),
        )
    }

    fn should_send_ack_at(
        &mut self,
        recv_stream: &ReliableRecvStream,
        path: Option<PathSnapshot>,
        traffic_class: TrafficClass,
        mux_limits: MuxLimits,
        force: bool,
        now: Instant,
    ) -> bool {
        let next_offset = recv_stream.next_offset();
        let reorder_bytes = recv_stream.reorder_bytes();
        let ack_summary = recv_stream.ack_range_summary();
        let range_count = ack_summary.count;
        let largest_end = ack_summary.largest_end;
        // These are disjoint received byte sets. Filling a hole transfers its
        // old suffix from reorder to the contiguous prefix without counting
        // that suffix as newly received; duplicate copies change neither set.
        let received_bytes = next_offset.saturating_add(reorder_bytes as u64);
        let last_received_bytes = self
            .last_ack_offset
            .saturating_add(self.last_ack_reorder_bytes as u64);
        let received_since_ack = received_bytes.saturating_sub(last_received_bytes);
        let has_progress = received_bytes > 0;
        let first_ack = last_received_bytes == 0 && has_progress;
        let cumulative_state_changed =
            self.ack_generation == 0 || received_bytes != last_received_bytes;
        if has_progress && cumulative_state_changed && self.pending_ack_deadline.is_none() {
            // Freeze on first observation of pending receipt. Neither later
            // snapshots nor new bytes can renew this generation's service.
            // Arithmetic exhaustion makes work due now, never unbounded.
            self.pending_ack_deadline = Some(self.last_ack_at.map_or(now, |last_ack_at| {
                last_ack_at
                    .checked_add(reliable_stream_recv_progress_interval(path))
                    .unwrap_or(now)
            }));
        }
        let ack_step = reliable_stream_ack_update_bytes(path, traffic_class, mux_limits);
        let horizon_advance = largest_end.saturating_sub(self.last_ack_largest_end);
        let gap_filled = self.last_ack_reorder_bytes > 0
            && (next_offset > self.last_ack_offset || received_since_ack > horizon_advance);
        let gap_state_changed = (reorder_bytes > 0 || self.last_ack_reorder_bytes > 0)
            && (range_count != self.last_ack_range_count || gap_filled);
        let enough_received = received_since_ack >= ack_step;
        let ack_timer_elapsed = self
            .pending_ack_deadline
            .is_some_and(|deadline| now >= deadline);
        if force || first_ack || gap_state_changed || enough_received || ack_timer_elapsed {
            if cumulative_state_changed {
                self.ack_generation = self.ack_generation.wrapping_add(1);
                if self.ack_generation == 0 {
                    self.ack_generation = 1;
                }
                self.last_ack_at = Some(now);
            }
            self.last_ack_offset = next_offset;
            self.last_ack_reorder_bytes = reorder_bytes;
            self.last_ack_range_count = range_count;
            self.last_ack_largest_end = largest_end;
            self.pending_ack_deadline = None;
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
        // RFC 8.4: every freed prefix advances the retained grant. Attachment
        // publication already coalesces blocked updates into one latest value;
        // a byte threshold here would withhold usable receive credit.
        if force || self.last_max_data_offset == 0 || max_offset > self.last_max_data_offset {
            self.last_max_data_offset = max_offset;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{PathId, StreamId};
    use bytes::Bytes;

    fn receive(stream: &mut ReliableRecvStream, offset: u64, bytes: usize) {
        stream
            .receive_data(offset, Bytes::from(vec![0x71; bytes]))
            .expect("valid receive trace");
    }

    fn snapshot() -> PathSnapshot {
        PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 351_000.0)
    }

    fn bulk_ack_at(
        progress: &mut ReliableRecvProgress,
        stream: &ReliableRecvStream,
        path: PathSnapshot,
        now: Instant,
    ) -> bool {
        progress.should_send_ack_at(
            stream,
            Some(path),
            TrafficClass::Throughput,
            MuxLimits::default(),
            false,
            now,
        )
    }

    #[test]
    fn receipt_quantum_is_rate_free_and_preserves_resource_clamps() {
        let low = snapshot();
        let high = PathSnapshot {
            delivery_rate_bps: 500_000_000.0,
            product_progress_rate_bps: Some(5_000_000_000.0),
            carrier_delivery_rate_bps: Some(50_000_000_000.0),
            underlay: UnderlayProtocol::Udp,
            ..low
        };
        let small = MuxLimits {
            max_stream_window_bytes: 64 * 1024,
            max_repair_bytes: 64 * 1024,
            max_reorder_bytes: 64 * 1024,
            max_path_flight_bytes: 64 * 1024,
            max_reliable_relay_chunk_bytes: 64 * 1024,
            ..MuxLimits::default()
        };
        for path in [None, Some(low), Some(high)] {
            assert_eq!(
                reliable_stream_ack_update_bytes(
                    path,
                    TrafficClass::Throughput,
                    MuxLimits::default()
                ),
                64 * 1024,
            );
            assert_eq!(
                reliable_stream_ack_update_bytes(path, TrafficClass::Throughput, small),
                small.max_repair_bytes as u64 / 4,
            );
            assert_eq!(
                reliable_stream_ack_update_bytes(path, TrafficClass::Latency, MuxLimits::default()),
                1,
            );
        }
    }

    #[test]
    fn first_nonempty_and_latency_receipts_survive_forced_empty_initialization() {
        let limits = MuxLimits::default();
        let mut stream = ReliableRecvStream::new(StreamId(1), limits);
        let mut progress = ReliableRecvProgress::default();
        let path = snapshot();
        let started = Instant::now();
        assert!(!bulk_ack_at(&mut progress, &stream, path, started));
        assert_eq!(progress.ack_generation(), 0);
        assert!(progress.should_send_ack_at(
            &stream,
            Some(path),
            TrafficClass::Throughput,
            limits,
            true,
            started,
        ));
        assert_eq!(progress.ack_generation(), 1);
        assert_eq!(progress.last_ack_at(), Some(started));
        assert!(progress.should_send_ack_at(
            &stream,
            Some(path),
            TrafficClass::Throughput,
            limits,
            true,
            started + Duration::from_millis(1),
        ));
        assert_eq!(progress.ack_generation(), 1);
        assert_eq!(progress.last_ack_at(), Some(started));

        receive(&mut stream, 0, 1);
        let received_at = started + Duration::from_millis(2);
        assert!(bulk_ack_at(&mut progress, &stream, path, received_at));
        assert_eq!(progress.ack_generation(), 2);
        receive(&mut stream, 1, 1);
        assert!(progress.should_send_ack_at(
            &stream,
            Some(path),
            TrafficClass::Latency,
            limits,
            false,
            received_at + Duration::from_millis(1),
        ));
        assert_eq!(progress.ack_generation(), 3);
        assert_eq!(progress.pending_ack_deadline(), None);
    }

    #[test]
    fn pending_receipt_deadline_ignores_duplicates_max_and_later_snapshots() {
        let limits = MuxLimits::default();
        let mut stream = ReliableRecvStream::new(StreamId(2), limits);
        let mut progress = ReliableRecvProgress::default();
        let path = snapshot();
        let started = Instant::now();
        let interval = reliable_stream_recv_progress_interval(Some(path));
        receive(&mut stream, 0, 1024);
        assert!(bulk_ack_at(&mut progress, &stream, path, started));

        assert!(progress.should_send_ack_at(
            &stream,
            Some(path),
            TrafficClass::Throughput,
            limits,
            true,
            started + Duration::from_millis(10),
        ));
        assert_eq!(progress.last_ack_at(), Some(started));
        assert_eq!(progress.ack_generation(), 1);
        receive(&mut stream, 1024, 1024);
        assert!(!bulk_ack_at(
            &mut progress,
            &stream,
            path,
            started + Duration::from_millis(20),
        ));
        let deadline = started + interval;
        assert_eq!(progress.pending_ack_deadline(), Some(deadline));
        assert!(progress.ack_update_pending());

        let slower = PathSnapshot {
            srtt_ms: 60_000.0,
            ..path
        };
        receive(&mut stream, 2048, 1024);
        assert!(!bulk_ack_at(
            &mut progress,
            &stream,
            slower,
            started + Duration::from_millis(30),
        ));
        assert!(progress.should_send_max_data(
            &stream,
            Some(slower),
            TrafficClass::Throughput,
            limits,
            true,
        ));
        receive(&mut stream, 1024, 1024);
        assert!(!bulk_ack_at(
            &mut progress,
            &stream,
            slower,
            deadline - Duration::from_nanos(1),
        ));
        assert_eq!(progress.pending_ack_deadline(), Some(deadline));
        assert_eq!(progress.last_ack_at(), Some(started));
        assert!(bulk_ack_at(&mut progress, &stream, slower, deadline));
        assert_eq!(progress.ack_generation(), 2);
        assert_eq!(progress.last_ack_at(), Some(deadline));
        assert_eq!(progress.pending_ack_deadline(), None);
        assert!(!bulk_ack_at(
            &mut progress,
            &stream,
            path,
            deadline + interval
        ));
    }

    #[test]
    fn receipt_quantum_counts_unique_contiguous_and_sparse_bytes() {
        let limits = MuxLimits::default();
        let quantum = reliable_stream_ack_update_bytes(None, TrafficClass::Throughput, limits);
        let path = snapshot();
        let started = Instant::now();
        // Both traces add the same unique bytes. The second never advances
        // the contiguous frontier and must still obey the receipt quantum.
        for first_offset in [0, 8192] {
            let mut stream = ReliableRecvStream::new(StreamId(3), limits);
            let mut progress = ReliableRecvProgress::default();
            receive(&mut stream, first_offset, 1);
            assert!(bulk_ack_at(&mut progress, &stream, path, started));
            receive(&mut stream, first_offset + 1, quantum as usize - 1);
            assert!(!bulk_ack_at(&mut progress, &stream, path, started));
            receive(&mut stream, first_offset + quantum, 1);
            assert!(bulk_ack_at(&mut progress, &stream, path, started));
            assert_eq!(progress.ack_generation(), 2);
            assert_eq!(progress.pending_ack_deadline(), None);
            receive(&mut stream, first_offset, quantum as usize + 1);
            assert!(!bulk_ack_at(&mut progress, &stream, path, started));
        }
    }

    #[test]
    fn sparse_same_range_extension_expires_without_frontier_movement() {
        let mut stream = ReliableRecvStream::new(StreamId(4), MuxLimits::default());
        let mut progress = ReliableRecvProgress::default();
        let path = snapshot();
        let started = Instant::now();
        receive(&mut stream, 8192, 1024);
        assert!(bulk_ack_at(&mut progress, &stream, path, started));
        receive(&mut stream, 9216, 1);
        assert!(!bulk_ack_at(&mut progress, &stream, path, started));
        assert_eq!(stream.next_offset(), 0);
        let deadline = progress
            .pending_ack_deadline()
            .expect("sparse receipt has a deadline");
        assert!(!bulk_ack_at(
            &mut progress,
            &stream,
            path,
            deadline - Duration::from_nanos(1),
        ));
        assert!(bulk_ack_at(&mut progress, &stream, path, deadline));
        assert_eq!(stream.next_offset(), 0);
        assert_eq!(progress.ack_generation(), 2);
        assert_eq!(progress.pending_ack_deadline(), None);
    }

    #[test]
    fn gap_appearance_partial_fill_and_closure_are_immediate_below_quantum() {
        let mut stream = ReliableRecvStream::new(StreamId(5), MuxLimits::default());
        let mut progress = ReliableRecvProgress::default();
        let path = snapshot();
        let now = Instant::now();
        receive(&mut stream, 0, 1);
        assert!(bulk_ack_at(&mut progress, &stream, path, now));
        receive(&mut stream, 64, 1);
        assert!(bulk_ack_at(&mut progress, &stream, path, now));
        assert_eq!(stream.ack_range_summary().count, 2);

        // Partial fill extends an island backward: count, largest end and
        // contiguous frontier are unchanged, but the advertised gap shrinks.
        receive(&mut stream, 32, 32);
        assert_eq!(stream.ack_range_summary().count, 2);
        assert_eq!(stream.next_offset(), 1);
        assert!(bulk_ack_at(&mut progress, &stream, path, now));
        let received_before = stream.next_offset() + stream.reorder_bytes() as u64;
        receive(&mut stream, 1, 31);
        assert_eq!(stream.next_offset(), 65);
        assert_eq!(stream.reorder_bytes(), 0);
        assert_eq!(stream.next_offset() - received_before, 31);
        assert!(bulk_ack_at(&mut progress, &stream, path, now));
        assert_eq!(progress.ack_generation(), 4);
        assert_eq!(progress.pending_ack_deadline(), None);
    }

    #[test]
    fn receipt_after_expired_changed_generation_is_immediately_due() {
        let mut stream = ReliableRecvStream::new(StreamId(6), MuxLimits::default());
        let mut progress = ReliableRecvProgress::default();
        let path = snapshot();
        let started = Instant::now();
        let interval = reliable_stream_recv_progress_interval(Some(path));
        receive(&mut stream, 0, 1);
        assert!(bulk_ack_at(&mut progress, &stream, path, started));
        receive(&mut stream, 1, 1);
        let received_at = started + interval;
        assert!(bulk_ack_at(&mut progress, &stream, path, received_at));
        assert_eq!(progress.last_ack_at(), Some(received_at));
        assert_eq!(progress.ack_generation(), 2);
        assert_eq!(progress.pending_ack_deadline(), None);
    }
}
