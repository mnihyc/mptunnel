//! Product receive-feedback state shared by both relay directions.
//!
//! This module decides when connection-level Data ACK and receive-window
//! updates are due. Carrier ACK and loss recovery remain owned by TCP or QUIC.

use super::feedback_route::StreamFeedbackProbe;
use crate::model::capacity::{
    QUIC_TIMER_GRANULARITY, reliable_stream_ack_update_bytes,
    reliable_stream_advertised_window_bytes,
};
use crate::model::timing::transport_pto_from_snapshot;
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableRecvStream;
use crate::protocol::{Frame, StreamId, UnderlayProtocol};
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::collections::VecDeque;
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

/// One desired feedback state per directional logical stream, not per output.
#[derive(Debug, Default)]
pub(in crate::runtime) struct StreamFeedbackState {
    pub(in crate::runtime) ack_generation: u64,
    pub(in crate::runtime) cumulative_ack_frames: Vec<Frame>,
    pub(in crate::runtime) max_data_offset: u64,
}

/// Every service entrypoint may publish either kind. Callers must apply both
/// effects, including receiver credit and the latest-generation terminal fence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::runtime) struct StreamFeedbackPublication {
    pub(in crate::runtime) ack_generation: u64,
    pub(in crate::runtime) ack: StreamAckPublication,
    pub(in crate::runtime) max_data: StreamMaxDataPublication,
}

impl StreamFeedbackPublication {
    pub(in crate::runtime) fn merge(&mut self, other: Self) {
        debug_assert!(self.ack_generation == 0 || self.ack_generation == other.ack_generation);
        self.ack_generation = other.ack_generation;
        self.ack.accepted |= other.ack.accepted;
        self.ack.published |= other.ack.published;
        self.ack.pending |= other.ack.pending;
        self.max_data.published_offset = self
            .max_data
            .published_offset
            .max(other.max_data.published_offset);
        self.max_data.pending |= other.max_data.pending;
    }
}

/// One exact output's current service eligibility. Proof and reply messages
/// use the same ordinary FIFO even when this output owes no eligible ACK/MAX.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct StreamFeedbackService {
    pub(in crate::runtime) allow_facts: bool,
    pub(in crate::runtime) probe: Option<StreamFeedbackProbe>,
    /// Already accepted by the logical actor and covered by actual peer MAX.
    pub(in crate::runtime) receipt: Option<u64>,
}

impl Default for StreamFeedbackService {
    fn default() -> Self {
        Self {
            allow_facts: true,
            probe: None,
            receipt: None,
        }
    }
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct StreamFeedbackOutputPublication {
    pub(in crate::runtime) feedback: StreamFeedbackPublication,
    pub(in crate::runtime) probe_admitted: Option<u64>,
    pub(in crate::runtime) receipt_admitted: Option<u64>,
}

/// Exact-output publication ownership. Only a blocked immutable ACK tail is
/// retained; admitted prefix frames release their range allocations at once.
/// New generations cannot restart this finite job. MAX remains one latest
/// scalar and receives alternating successful service beside ACK chunks.
#[derive(Debug, Clone, Default)]
pub(in crate::runtime) struct StreamFeedbackPublicationCursor {
    published_generation: u64,
    pending_generation: u64,
    pending_frames: VecDeque<Frame>,
    prefer_max_data: bool,
}

impl StreamFeedbackPublicationCursor {
    pub(in crate::runtime) fn service<E>(
        &mut self,
        state: &StreamFeedbackState,
        update_frames: Option<&[Frame]>,
        stream_id: StreamId,
        published_max_offset: &mut u64,
        policy: StreamFeedbackService,
        mut enqueue: E,
    ) -> StreamFeedbackOutputPublication
    where
        E: FnMut(Frame) -> bool,
    {
        let generation = state.ack_generation;
        debug_assert!(generation == 0 || !state.cumulative_ack_frames.is_empty());
        debug_assert!(state.cumulative_ack_frames.iter().chain(update_frames.into_iter().flatten()).all(
            |frame| matches!(frame, Frame::StreamAck { stream_id: id, .. } if *id == stream_id)
        ));
        let mut output = StreamFeedbackOutputPublication {
            feedback: StreamFeedbackPublication {
                ack_generation: generation,
                ..Default::default()
            },
            ..Default::default()
        };
        let result = &mut output.feedback;

        // Keep the common immediate path borrowed. Materialize an immutable
        // tail only on actual blocked admission, never a second healthy copy.
        let mut current_frames: Option<&[Frame]> = None;
        let mut current_index = 0;
        loop {
            if let Some(token) = policy.receipt
                && output.receipt_admitted.is_none()
            {
                if !enqueue(Frame::StreamFeedbackReceipt { stream_id, token }) {
                    if let Some(frames) = current_frames {
                        self.retain_tail(generation, &frames[current_index..]);
                    }
                    break;
                }
                output.receipt_admitted = Some(token);
                continue;
            }
            if let Some(probe) = policy.probe
                && output.probe_admitted.is_none()
                && self.published_generation != 0
                // Published lies within the captured-to-current generation
                // interval, including counter wrap. No active proof can span
                // a complete u64 generation cycle.
                && self.published_generation.wrapping_sub(probe.ack_generation)
                    <= generation.wrapping_sub(probe.ack_generation)
                && *published_max_offset >= probe.max_offset
            {
                if !enqueue(Frame::StreamFeedbackProbe {
                    stream_id,
                    token: probe.token,
                    max_offset: probe.max_offset,
                }) {
                    if let Some(frames) = current_frames {
                        self.retain_tail(generation, &frames[current_index..]);
                    }
                    break;
                }
                output.probe_admitted = Some(probe.token);
                continue;
            }
            let ack_pending = policy.allow_facts && self.is_pending(generation);
            let max_pending = policy.allow_facts && *published_max_offset < state.max_data_offset;
            if !ack_pending && !max_pending {
                break;
            }
            if max_pending && (self.prefer_max_data || !ack_pending) {
                if !enqueue(Frame::StreamMaxData {
                    stream_id,
                    max_offset: state.max_data_offset,
                }) {
                    // ACK may already have an admitted prefix in this call.
                    // Preserve its remaining borrowed tail before returning.
                    if let Some(frames) = current_frames {
                        self.retain_tail(generation, &frames[current_index..]);
                    }
                    break;
                }
                *published_max_offset = state.max_data_offset;
                result.max_data.published_offset = Some(state.max_data_offset);
                self.prefer_max_data = false;
                continue;
            }

            if let Some(frame) = self.pending_frames.front() {
                if !enqueue(frame.clone()) {
                    break;
                }
                self.pending_frames.pop_front();
                result.ack.accepted = true;
                self.prefer_max_data = true;
                if self.pending_frames.is_empty() {
                    self.published_generation = self.pending_generation;
                    self.pending_generation = 0;
                    // Drop even the now-empty container's backing storage.
                    self.pending_frames = VecDeque::new();
                }
                continue;
            }

            let frames = *current_frames.get_or_insert_with(|| {
                if self.published_generation == generation.wrapping_sub(1) {
                    update_frames.unwrap_or(&state.cumulative_ack_frames)
                } else {
                    &state.cumulative_ack_frames
                }
            });
            debug_assert!(!frames.is_empty());
            if !enqueue(frames[current_index].clone()) {
                self.retain_tail(generation, &frames[current_index..]);
                break;
            }
            result.ack.accepted = true;
            self.prefer_max_data = true;
            current_index += 1;
            if current_index == frames.len() {
                self.published_generation = generation;
                current_frames = None;
                current_index = 0;
            }
        }
        result.ack.published =
            policy.allow_facts && generation != 0 && !self.is_pending(generation);
        result.ack.pending = policy.allow_facts && self.is_pending(generation);
        result.max_data.pending =
            policy.allow_facts && *published_max_offset < state.max_data_offset;
        output
    }

    fn retain_tail(&mut self, generation: u64, frames: &[Frame]) {
        debug_assert!(self.pending_frames.is_empty());
        debug_assert!(!frames.is_empty());
        self.pending_generation = generation;
        self.pending_frames = frames.iter().cloned().collect();
    }

    pub(in crate::runtime) fn is_pending(&self, generation: u64) -> bool {
        generation != 0 && self.published_generation != generation
    }

    pub(in crate::runtime) fn has_ack_baseline(&self) -> bool {
        self.published_generation != 0
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
    last_ack_at: Option<Instant>,
    ack_update_pending: bool,
}

impl ReliableRecvProgress {
    pub(in crate::runtime) fn ack_generation(&self) -> u64 {
        self.ack_generation
    }

    pub(in crate::runtime) fn last_ack_at(&self) -> Option<Instant> {
        self.last_ack_at
    }

    pub(in crate::runtime) fn ack_update_pending(&self) -> bool {
        self.ack_update_pending
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
        if has_progress && cumulative_state_changed {
            self.ack_update_pending = true;
        }
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
            self.ack_update_pending = false;
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
mod publication_tests {
    use super::*;
    use crate::protocol::OffsetRange;

    fn ack(end: u64) -> Frame {
        Frame::StreamAck {
            stream_id: StreamId(1),
            scope_start: None,
            ranges: vec![OffsetRange {
                start: end - 1,
                end,
            }],
        }
    }

    fn service_slots(
        cursor: &mut StreamFeedbackPublicationCursor,
        state: &StreamFeedbackState,
        update: Option<&[Frame]>,
        max_offset: &mut u64,
        slots: usize,
    ) -> (StreamFeedbackPublication, Vec<Frame>) {
        let mut frames = Vec::new();
        let result = cursor.service(
            state,
            update,
            StreamId(1),
            max_offset,
            StreamFeedbackService::default(),
            |frame| {
                if frames.len() == slots {
                    return false;
                }
                frames.push(frame);
                true
            },
        );
        (result.feedback, frames)
    }

    #[test]
    fn feedback_partial_delta_finishes_before_new_generation_without_claiming_latest() {
        let mut cursor = StreamFeedbackPublicationCursor::default();
        let mut max_offset = 0;
        let mut state = StreamFeedbackState {
            ack_generation: 1,
            cumulative_ack_frames: vec![ack(1)],
            max_data_offset: 0,
        };
        assert!(
            service_slots(&mut cursor, &state, Some(&[ack(1)]), &mut max_offset, 1)
                .0
                .ack
                .published
        );
        assert_eq!(cursor.pending_frames.capacity(), 0);
        state.ack_generation = 2;
        state.cumulative_ack_frames = vec![ack(1), ack(2), ack(3)];
        let (partial, frames) = service_slots(
            &mut cursor,
            &state,
            Some(&[ack(2), ack(3)]),
            &mut max_offset,
            1,
        );
        assert_eq!(frames, vec![ack(2)]);
        assert!(partial.ack.pending);
        assert_eq!(
            cursor.pending_frames.iter().cloned().collect::<Vec<_>>(),
            vec![ack(3)]
        );

        state.ack_generation = 3;
        state.cumulative_ack_frames.push(ack(4));
        state.max_data_offset = 100;
        let (credit, frames) =
            service_slots(&mut cursor, &state, Some(&[ack(4)]), &mut max_offset, 1);
        assert_eq!(
            frames,
            vec![Frame::StreamMaxData {
                stream_id: StreamId(1),
                max_offset: 100
            }]
        );
        assert_eq!(credit.max_data.published_offset, Some(100));
        assert!(!credit.ack.published);
        let (old, frames) = service_slots(&mut cursor, &state, Some(&[ack(4)]), &mut max_offset, 1);
        assert_eq!(frames, vec![ack(3)]);
        assert!(
            !old.ack.published,
            "old job completion is not the current terminal fence"
        );
        assert_eq!(cursor.pending_generation, 3);
        assert_eq!(
            cursor.pending_frames.iter().cloned().collect::<Vec<_>>(),
            vec![ack(4)]
        );
        let (latest, frames) = service_slots(&mut cursor, &state, None, &mut max_offset, 1);
        assert_eq!(frames, vec![ack(4)]);
        assert!(latest.ack.published && !latest.ack.pending);
        assert_eq!(cursor.pending_frames.capacity(), 0);
    }

    #[test]
    fn feedback_failed_credit_admission_preserves_borrowed_ack_tail_and_turn() {
        let mut cursor = StreamFeedbackPublicationCursor::default();
        let mut max_offset = 0;
        let state = StreamFeedbackState {
            ack_generation: 1,
            cumulative_ack_frames: vec![ack(1), ack(2), ack(3)],
            max_data_offset: 100,
        };
        let (result, frames) = service_slots(&mut cursor, &state, None, &mut max_offset, 1);
        assert_eq!(frames, vec![ack(1)]);
        assert!(result.ack.pending && result.max_data.pending);
        assert_eq!(cursor.pending_frames.len(), 2);
        let retained_pointer = cursor.pending_frames.front().unwrap() as *const Frame;
        for _ in 0..3 {
            let (retry, frames) = service_slots(&mut cursor, &state, None, &mut max_offset, 0);
            assert!(frames.is_empty() && !retry.ack.accepted);
            assert_eq!(retry.max_data.published_offset, None);
            assert!(cursor.prefer_max_data);
            assert_eq!(
                cursor.pending_frames.front().unwrap() as *const Frame,
                retained_pointer
            );
        }
        let (result, frames) = service_slots(&mut cursor, &state, None, &mut max_offset, 3);
        assert_eq!(
            frames,
            vec![
                Frame::StreamMaxData {
                    stream_id: StreamId(1),
                    max_offset: 100
                },
                ack(2),
                ack(3)
            ]
        );
        assert!(result.ack.published && !result.max_data.pending);
        assert_eq!(cursor.pending_frames.capacity(), 0);
    }

    #[test]
    fn feedback_probe_follows_complete_frozen_ack_and_credit_before_newer_facts() {
        let mut cursor = StreamFeedbackPublicationCursor::default();
        let mut max_offset = 0;
        let mut state = StreamFeedbackState {
            ack_generation: 1,
            cumulative_ack_frames: vec![ack(1)],
            max_data_offset: 100,
        };
        service_slots(&mut cursor, &state, None, &mut max_offset, 2);
        state.ack_generation = 2;
        state.cumulative_ack_frames = vec![ack(2), ack(3)];
        state.max_data_offset = 200;
        let policy = StreamFeedbackService {
            probe: Some(StreamFeedbackProbe {
                token: 7,
                ack_generation: 2,
                max_offset: 200,
            }),
            ..Default::default()
        };
        let mut admitted = Vec::new();
        for turn in 0..4 {
            if turn == 1 {
                state.ack_generation = 3;
                state.cumulative_ack_frames.push(ack(4));
            }
            let mut slot = None;
            let result = cursor.service(
                &state,
                None,
                StreamId(1),
                &mut max_offset,
                policy,
                |frame| {
                    if slot.is_some() {
                        return false;
                    }
                    slot = Some(frame);
                    true
                },
            );
            if turn < 3 {
                assert_eq!(result.probe_admitted, None);
            } else {
                assert_eq!(result.probe_admitted, Some(7));
                assert!(
                    result.feedback.ack.pending,
                    "proof is not current ACK publication"
                );
            }
            admitted.push(slot.unwrap());
        }
        assert_eq!(
            admitted,
            vec![
                ack(2),
                Frame::StreamMaxData {
                    stream_id: StreamId(1),
                    max_offset: 200
                },
                ack(3),
                Frame::StreamFeedbackProbe {
                    stream_id: StreamId(1),
                    token: 7,
                    max_offset: 200
                },
            ]
        );
        assert_eq!(cursor.pending_generation, 3);
        assert!(cursor.is_pending(3));
    }

    #[test]
    fn feedback_ready_receipt_has_no_ack_or_credit_publication_authority() {
        let mut cursor = StreamFeedbackPublicationCursor::default();
        let mut max_offset = 0;
        let state = StreamFeedbackState {
            ack_generation: 2,
            cumulative_ack_frames: vec![ack(1), ack(2)],
            max_data_offset: 100,
        };
        service_slots(&mut cursor, &state, None, &mut max_offset, 0);
        let retained = cursor.pending_frames.clone();
        let mut frames = Vec::new();
        let result = cursor.service(
            &state,
            None,
            StreamId(1),
            &mut max_offset,
            StreamFeedbackService {
                allow_facts: false,
                receipt: Some(9),
                probe: None,
            },
            |frame| {
                frames.push(frame);
                true
            },
        );
        assert_eq!(
            frames,
            vec![Frame::StreamFeedbackReceipt {
                stream_id: StreamId(1),
                token: 9
            }]
        );
        assert_eq!(result.receipt_admitted, Some(9));
        assert!(
            !result.feedback.ack.accepted
                && !result.feedback.ack.published
                && !result.feedback.ack.pending
        );
        assert_eq!(
            result.feedback.max_data,
            StreamMaxDataPublication::default()
        );
        assert_eq!(max_offset, 0);
        assert_eq!(cursor.pending_frames, retained);
    }

    #[test]
    fn feedback_skipped_generation_requires_cumulative_not_delta() {
        let mut cursor = StreamFeedbackPublicationCursor::default();
        let mut max_offset = 0;
        let mut state = StreamFeedbackState {
            ack_generation: 1,
            cumulative_ack_frames: vec![ack(1)],
            max_data_offset: 100,
        };
        let (_, frames) = service_slots(&mut cursor, &state, Some(&[ack(1)]), &mut max_offset, 2);
        assert_eq!(
            frames,
            vec![
                ack(1),
                Frame::StreamMaxData {
                    stream_id: StreamId(1),
                    max_offset: 100
                }
            ]
        );
        state.ack_generation = 3;
        state.cumulative_ack_frames.extend([ack(2), ack(3)]);
        let (result, frames) =
            service_slots(&mut cursor, &state, Some(&[ack(3)]), &mut max_offset, 3);
        assert_eq!(frames, state.cumulative_ack_frames);
        assert!(result.ack.published);
        assert_eq!(cursor.pending_frames.capacity(), 0);
    }
}
