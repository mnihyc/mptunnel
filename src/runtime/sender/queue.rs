//! Byte-bounded product work queue for reliable streams.
//!
//! This queue is above TCP and QUIC carrier queues. It prioritizes correctness
//! work and accounts product bytes without selecting or mutating a path.

use super::{RelaySendCause, ServerReinjectionOutputIdentity};
use crate::model::capacity::reliable_relay_buffer_len;
use crate::model::work::ReliableWorkClass;
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
use crate::protocol::frame::{reliable_stream_frame_accounted_bytes, reliable_stream_frame_extent};
use crate::protocol::{Frame, OffsetRange};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::collections::VecDeque;
use std::time::Instant;

#[derive(Debug, Clone)]
pub(in crate::runtime) enum ReliableRelayQueuedWorkKind {
    Control(Frame),
    Data(Bytes),
    Reinjection { frame: Frame, cause: RelaySendCause },
}

#[derive(Debug, Clone)]
/// Byte-bounded queue for product reliable work awaiting sender admission.
///
/// This is above carrier paths: it is sized by product flow-control and
/// reinjection
/// envelopes, not by TCP socket buffers or QUIC packet queues. Normal target
/// bytes remain raw bytes until dispatch, so the sender-service executor owns
/// the point where bytes become STREAM_DATA. Reinserted STREAM_DATA enters a
/// separate lane even though its wire frame kind is still STREAM_DATA.
pub(in crate::runtime) struct ReliableRelayQueuedWork {
    pub(in crate::runtime) kind: ReliableRelayQueuedWorkKind,
    pub(in crate::runtime) payload_bytes: usize,
    pub(in crate::runtime) data_lane: Option<TrafficClass>,
    pub(in crate::runtime) stream_ordered_carrier_emit: bool,
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) enqueue_id: u64,
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) queued_at: Instant,
}

#[derive(Debug, Default)]
/// Lane staging queue used by the response sender service.
///
/// It owns queued product work and queue age before dispatch. Path command
/// queues must receive only already-admitted frames.
pub(in crate::runtime) struct ReliableRelaySenderQueue {
    critical_reinjection: VecDeque<ReliableRelayQueuedWork>,
    reinjection: VecDeque<ReliableRelayQueuedWork>,
    data: VecDeque<ReliableRelayQueuedWork>,
    final_control: VecDeque<ReliableRelayQueuedWork>,
    bytes: usize,
    data_bytes: usize,
    #[cfg(feature = "lab-diagnostics")]
    next_enqueue_id: u64,
}

impl ReliableRelaySenderQueue {
    pub(in crate::runtime) fn is_empty(&self) -> bool {
        self.critical_reinjection.is_empty()
            && self.reinjection.is_empty()
            && self.data.is_empty()
            && self.final_control.is_empty()
    }

    pub(in crate::runtime) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(in crate::runtime) fn data_bytes(&self) -> usize {
        self.data_bytes
    }

    pub(in crate::runtime) fn push_final_control(&mut self, frame: Frame) -> u64 {
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        self.push_work(
            ReliableWorkClass::Control,
            ReliableRelayQueuedWorkKind::Control(frame),
            None,
            true,
            payload_bytes,
        )
    }

    pub(in crate::runtime) fn push_data(&mut self, payload: Bytes) -> u64 {
        self.push_data_for_lane(payload, TrafficClass::Throughput)
    }

    pub(in crate::runtime) fn push_data_for_lane(
        &mut self,
        payload: Bytes,
        lane: TrafficClass,
    ) -> u64 {
        let payload_bytes = payload.len();
        self.push_work(
            ReliableWorkClass::Data,
            ReliableRelayQueuedWorkKind::Data(payload),
            Some(lane),
            false,
            payload_bytes,
        )
    }

    pub(in crate::runtime) fn push_reinjection(&mut self, frame: Frame) -> u64 {
        self.push_reinjection_with_cause(frame, RelaySendCause::AckGapReinjection)
    }

    pub(in crate::runtime) fn push_reinjection_with_cause(
        &mut self,
        frame: Frame,
        cause: RelaySendCause,
    ) -> u64 {
        self.push_reinjection_with_priority(frame, cause, false)
    }

    pub(in crate::runtime) fn push_critical_reinjection_with_cause(
        &mut self,
        frame: Frame,
        cause: RelaySendCause,
    ) -> u64 {
        self.push_reinjection_with_priority(frame, cause, true)
    }

    fn push_reinjection_with_priority(
        &mut self,
        frame: Frame,
        cause: RelaySendCause,
        critical: bool,
    ) -> u64 {
        debug_assert!(cause.is_reinjection());
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        let enqueue_id = self.push_work(
            ReliableWorkClass::Reinjection,
            ReliableRelayQueuedWorkKind::Reinjection { frame, cause },
            None,
            false,
            payload_bytes,
        );
        if critical {
            let work = self
                .reinjection
                .pop_back()
                .expect("newly pushed reinjection must exist");
            self.critical_reinjection.push_back(work);
        }
        enqueue_id
    }

    fn push_work(
        &mut self,
        lane: ReliableWorkClass,
        kind: ReliableRelayQueuedWorkKind,
        data_lane: Option<TrafficClass>,
        final_control: bool,
        payload_bytes: usize,
    ) -> u64 {
        #[cfg(feature = "lab-diagnostics")]
        let enqueue_id = {
            let enqueue_id = self.next_enqueue_id;
            self.next_enqueue_id = self.next_enqueue_id.saturating_add(1);
            enqueue_id
        };
        #[cfg(not(feature = "lab-diagnostics"))]
        let enqueue_id = 0;
        self.bytes = self.bytes.saturating_add(payload_bytes);
        if lane == ReliableWorkClass::Data {
            self.data_bytes = self.data_bytes.saturating_add(payload_bytes);
        }
        let work = ReliableRelayQueuedWork {
            kind,
            payload_bytes,
            data_lane,
            stream_ordered_carrier_emit: final_control,
            #[cfg(feature = "lab-diagnostics")]
            enqueue_id,
            #[cfg(feature = "lab-diagnostics")]
            queued_at: Instant::now(),
        };
        match lane {
            ReliableWorkClass::Control => {
                debug_assert!(final_control);
                self.final_control.push_back(work);
            }
            ReliableWorkClass::Data => self.data.push_back(work),
            ReliableWorkClass::Reinjection => self.reinjection.push_back(work),
        }
        enqueue_id
    }

    pub(in crate::runtime) fn front(
        &self,
    ) -> Option<(ReliableWorkClass, &ReliableRelayQueuedWork)> {
        if let Some(work) = self.critical_reinjection.front() {
            Some((ReliableWorkClass::Reinjection, work))
        } else if let Some(work) = self.data.front() {
            Some((ReliableWorkClass::Data, work))
        } else if let Some(work) = self.reinjection.front() {
            Some((ReliableWorkClass::Reinjection, work))
        } else {
            self.final_control
                .front()
                .map(|work| (ReliableWorkClass::Control, work))
        }
    }

    pub(super) fn persistent_ack_gap_reinjection_deadline(&self) -> Option<Instant> {
        self.critical_reinjection
            .iter()
            .chain(self.reinjection.iter())
            .filter_map(|work| match &work.kind {
                ReliableRelayQueuedWorkKind::Reinjection { cause, .. } => {
                    cause.persistent_ack_gap_reinjection_deadline()
                }
                _ => None,
            })
            .min()
    }

    pub(in crate::runtime) fn has_queued_reinjection_overlap(&self, frame: &Frame) -> bool {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return false;
        };
        self.critical_reinjection
            .iter()
            .chain(self.reinjection.iter())
            .any(|work| {
                let ReliableRelayQueuedWorkKind::Reinjection { frame: queued, .. } = &work.kind
                else {
                    return false;
                };
                let Some((queued_start, queued_end, _)) = reliable_stream_frame_extent(queued)
                else {
                    return false;
                };
                queued_start < end && start < queued_end
            })
    }

    pub(in crate::runtime) fn has_queued_reinjection_range_overlap(
        &self,
        ranges: &[crate::protocol::OffsetRange],
    ) -> bool {
        self.critical_reinjection
            .iter()
            .chain(self.reinjection.iter())
            .any(|work| {
                let ReliableRelayQueuedWorkKind::Reinjection { frame, .. } = &work.kind else {
                    return false;
                };
                let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
                    return false;
                };
                ranges
                    .iter()
                    .any(|range| start < range.end && range.start < end)
            })
    }

    pub(in crate::runtime) fn release_normalized_acked_reinjections(
        &mut self,
        ranges: &[OffsetRange],
    ) -> usize {
        if ranges.is_empty() {
            return 0;
        }
        let released = prune_acked_reinjection_queue(&mut self.critical_reinjection, ranges)
            .saturating_add(prune_acked_reinjection_queue(&mut self.reinjection, ranges));
        self.bytes = self.bytes.saturating_sub(released);
        released
    }

    pub(in crate::runtime) fn discard_unusable_tail_reinjections(
        &mut self,
        usable: impl Fn(&Frame) -> bool,
    ) -> usize {
        let released =
            discard_unusable_tail_reinjection_queue(&mut self.critical_reinjection, &usable)
                .saturating_add(discard_unusable_tail_reinjection_queue(
                    &mut self.reinjection,
                    &usable,
                ));
        self.bytes = self.bytes.saturating_sub(released);
        released
    }

    pub(in crate::runtime) fn discard_stale_persistent_ack_gap_reinjections(
        &mut self,
        usable: impl Fn(RelaySendCause) -> bool,
    ) -> usize {
        let now = Instant::now();
        let released = discard_stale_persistent_ack_gap_reinjection_queue(
            &mut self.critical_reinjection,
            now,
            &usable,
        )
        .saturating_add(discard_stale_persistent_ack_gap_reinjection_queue(
            &mut self.reinjection,
            now,
            &usable,
        ));
        self.bytes = self.bytes.saturating_sub(released);
        released
    }

    pub(in crate::runtime) fn discard_resolved_stale_path_reinjections(
        &mut self,
        path_is_stale: impl Fn(crate::model::path::RelayPathInstance) -> bool,
    ) -> usize {
        let released = discard_resolved_stale_path_reinjection_queue(
            &mut self.critical_reinjection,
            &path_is_stale,
        )
        .saturating_add(discard_resolved_stale_path_reinjection_queue(
            &mut self.reinjection,
            &path_is_stale,
        ));
        self.bytes = self.bytes.saturating_sub(released);
        released
    }

    pub(in crate::runtime) fn discard_resolved_stale_response_path_reinjections(
        &mut self,
        path_is_stale: impl Fn(ServerReinjectionOutputIdentity) -> bool,
    ) -> usize {
        let released = discard_resolved_stale_response_path_reinjection_queue(
            &mut self.critical_reinjection,
            &path_is_stale,
        )
        .saturating_add(discard_resolved_stale_response_path_reinjection_queue(
            &mut self.reinjection,
            &path_is_stale,
        ));
        self.bytes = self.bytes.saturating_sub(released);
        released
    }

    pub(super) fn discard_persistent_ack_gap_reinjection_batch(
        &mut self,
        cause: RelaySendCause,
    ) -> usize {
        if !matches!(
            cause,
            RelaySendCause::PersistentClientAckGapReinjection(_)
                | RelaySendCause::PersistentServerAckGapReinjection(_)
        ) {
            return 0;
        }
        let released = discard_persistent_ack_gap_reinjection_batch_from_queue(
            &mut self.critical_reinjection,
            cause,
        )
        .saturating_add(discard_persistent_ack_gap_reinjection_batch_from_queue(
            &mut self.reinjection,
            cause,
        ));
        self.bytes = self.bytes.saturating_sub(released);
        released
    }

    pub(in crate::runtime) fn commit_front(
        &mut self,
    ) -> Option<(ReliableWorkClass, ReliableRelayQueuedWork)> {
        let (lane, work) = if let Some(work) = self.critical_reinjection.pop_front() {
            (ReliableWorkClass::Reinjection, work)
        } else if let Some(work) = self.data.pop_front() {
            (ReliableWorkClass::Data, work)
        } else if let Some(work) = self.reinjection.pop_front() {
            (ReliableWorkClass::Reinjection, work)
        } else {
            (ReliableWorkClass::Control, self.final_control.pop_front()?)
        };
        self.bytes = self.bytes.saturating_sub(work.payload_bytes);
        if lane == ReliableWorkClass::Data {
            self.data_bytes = self.data_bytes.saturating_sub(work.payload_bytes);
        }
        Some((lane, work))
    }

    pub(super) fn commit_front_data_prefix(
        &mut self,
        prefix_len: usize,
    ) -> Option<ReliableRelayQueuedWork> {
        let work = self.data.front_mut()?;
        let ReliableRelayQueuedWorkKind::Data(payload) = &mut work.kind else {
            return None;
        };
        let prefix_len = prefix_len.min(payload.len()).max(1);
        if prefix_len >= payload.len() {
            let (_, work) = self.commit_front()?;
            return Some(work);
        }

        let prefix = payload.slice(..prefix_len);
        let remaining = payload.slice(prefix_len..);
        *payload = remaining;
        work.payload_bytes = work.payload_bytes.saturating_sub(prefix_len);
        self.bytes = self.bytes.saturating_sub(prefix_len);
        self.data_bytes = self.data_bytes.saturating_sub(prefix_len);

        Some(ReliableRelayQueuedWork {
            kind: ReliableRelayQueuedWorkKind::Data(prefix),
            payload_bytes: prefix_len,
            data_lane: work.data_lane,
            stream_ordered_carrier_emit: work.stream_ordered_carrier_emit,
            #[cfg(feature = "lab-diagnostics")]
            enqueue_id: work.enqueue_id,
            #[cfg(feature = "lab-diagnostics")]
            queued_at: work.queued_at,
        })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn pop_front(
        &mut self,
    ) -> Option<(ReliableWorkClass, ReliableRelayQueuedWork)> {
        self.commit_front()
    }
}

fn prune_acked_reinjection_queue(
    queue: &mut VecDeque<ReliableRelayQueuedWork>,
    ranges: &[OffsetRange],
) -> usize {
    let mut released = 0usize;
    let mut retained = VecDeque::with_capacity(queue.len());
    while let Some(work) = queue.pop_front() {
        let ReliableRelayQueuedWorkKind::Reinjection { frame, cause } = &work.kind else {
            retained.push_back(work);
            continue;
        };
        let slices = unacked_reinjection_frame_slices(frame, ranges);
        let retained_bytes = slices
            .iter()
            .map(reliable_stream_frame_accounted_bytes)
            .sum::<usize>();
        released = released.saturating_add(work.payload_bytes.saturating_sub(retained_bytes));
        for frame in slices {
            let mut retained_work = work.clone();
            retained_work.payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
            retained_work.kind = ReliableRelayQueuedWorkKind::Reinjection {
                frame,
                cause: *cause,
            };
            retained.push_back(retained_work);
        }
    }
    *queue = retained;
    released
}

fn discard_unusable_tail_reinjection_queue(
    queue: &mut VecDeque<ReliableRelayQueuedWork>,
    usable: &impl Fn(&Frame) -> bool,
) -> usize {
    let mut released = 0usize;
    queue.retain(|work| {
        let ReliableRelayQueuedWorkKind::Reinjection { frame, cause } = &work.kind else {
            return true;
        };
        let keep = *cause != RelaySendCause::TailReinjection || usable(frame);
        if !keep {
            released = released.saturating_add(work.payload_bytes);
        }
        keep
    });
    released
}

fn discard_stale_persistent_ack_gap_reinjection_queue(
    queue: &mut VecDeque<ReliableRelayQueuedWork>,
    now: Instant,
    usable: &impl Fn(RelaySendCause) -> bool,
) -> usize {
    let mut released = 0usize;
    queue.retain(|work| {
        let ReliableRelayQueuedWorkKind::Reinjection { cause, .. } = &work.kind else {
            return true;
        };
        let bound = matches!(
            cause,
            RelaySendCause::PersistentClientAckGapReinjection(_)
                | RelaySendCause::PersistentServerAckGapReinjection(_)
        );
        let keep = !bound || (!cause.persistent_ack_gap_reinjection_expired(now) && usable(*cause));
        if !keep {
            released = released.saturating_add(work.payload_bytes);
        }
        keep
    });
    released
}

fn discard_resolved_stale_path_reinjection_queue(
    queue: &mut VecDeque<ReliableRelayQueuedWork>,
    path_is_stale: &impl Fn(crate::model::path::RelayPathInstance) -> bool,
) -> usize {
    let mut released = 0usize;
    queue.retain(|work| {
        let keep = !matches!(
            &work.kind,
            ReliableRelayQueuedWorkKind::Reinjection {
                cause: RelaySendCause::StalePathReinjection(path),
                ..
            } if !path_is_stale(*path)
        );
        if !keep {
            released = released.saturating_add(work.payload_bytes);
        }
        keep
    });
    released
}

fn discard_resolved_stale_response_path_reinjection_queue(
    queue: &mut VecDeque<ReliableRelayQueuedWork>,
    path_is_stale: &impl Fn(ServerReinjectionOutputIdentity) -> bool,
) -> usize {
    let mut released = 0usize;
    queue.retain(|work| {
        let keep = !matches!(
            &work.kind,
            ReliableRelayQueuedWorkKind::Reinjection {
                cause: RelaySendCause::StaleResponsePathReinjection(path),
                ..
            } if !path_is_stale(*path)
        );
        if !keep {
            released = released.saturating_add(work.payload_bytes);
        }
        keep
    });
    released
}

fn discard_persistent_ack_gap_reinjection_batch_from_queue(
    queue: &mut VecDeque<ReliableRelayQueuedWork>,
    batch_cause: RelaySendCause,
) -> usize {
    let mut released = 0usize;
    queue.retain(|work| {
        let keep = !matches!(
            &work.kind,
            ReliableRelayQueuedWorkKind::Reinjection { cause, .. } if *cause == batch_cause
        );
        if !keep {
            released = released.saturating_add(work.payload_bytes);
        }
        keep
    });
    released
}

fn unacked_reinjection_frame_slices(frame: &Frame, ranges: &[OffsetRange]) -> Vec<Frame> {
    let Frame::StreamData {
        stream_id,
        offset,
        payload,
    } = frame
    else {
        return vec![frame.clone()];
    };
    let frame_end = offset.saturating_add(payload.len() as u64);
    let mut remaining = vec![(*offset, frame_end)];
    for range in ranges {
        let mut next = Vec::with_capacity(remaining.len().saturating_add(1));
        for (start, end) in remaining {
            if range.end <= start || range.start >= end {
                next.push((start, end));
                continue;
            }
            if start < range.start {
                next.push((start, range.start.min(end)));
            }
            if range.end < end {
                next.push((range.end.max(start), end));
            }
        }
        remaining = next;
        if remaining.is_empty() {
            break;
        }
    }
    remaining
        .into_iter()
        .filter_map(|(start, end)| {
            let slice_start = usize::try_from(start.saturating_sub(*offset)).ok()?;
            let slice_end = usize::try_from(end.saturating_sub(*offset)).ok()?;
            (slice_start < slice_end && slice_end <= payload.len()).then(|| Frame::StreamData {
                stream_id: *stream_id,
                offset: start,
                payload: payload.slice(slice_start..slice_end),
            })
        })
        .collect()
}

pub(in crate::runtime) fn reliable_relay_sender_queue_limit(
    mux_limits: MuxLimits,
    inflight_limit: usize,
) -> usize {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    inflight_limit
        .max(reliable_relay_buffer_len(mux_limits))
        .min(mux_limits.max_repair_bytes)
        .min(stream_window)
        .min(mux_limits.max_path_flight_bytes)
        .max(1)
}

pub(in crate::runtime) fn reliable_relay_can_read_into_sender_queue(
    send_stream: &ReliableSendStream,
    sender_queue: &ReliableRelaySenderQueue,
    queue_limit: usize,
) -> bool {
    sender_queue.bytes() < queue_limit
        && sender_queue.data_bytes() < send_stream.send_credit_bytes()
}

pub(in crate::runtime) fn reliable_relay_can_read_product_source(
    local_open: bool,
    queued_send_blocked: bool,
    send_stream: &ReliableSendStream,
    sender_queue: &ReliableRelaySenderQueue,
    queue_limit: usize,
) -> bool {
    local_open
        && !queued_send_blocked
        && reliable_relay_can_read_into_sender_queue(send_stream, sender_queue, queue_limit)
}

pub(in crate::runtime) fn reliable_relay_sender_queue_read_budget(
    send_stream: &ReliableSendStream,
    sender_queue: &ReliableRelaySenderQueue,
    queue_limit: usize,
    buffer_len: usize,
) -> usize {
    queue_limit
        .saturating_sub(sender_queue.bytes())
        .min(
            send_stream
                .send_credit_bytes()
                .saturating_sub(sender_queue.data_bytes()),
        )
        .min(buffer_len)
}

#[cfg(test)]
#[path = "queue_test.rs"]
mod tests;
