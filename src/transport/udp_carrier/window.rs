use super::packet::{PacketAckRange, PacketPayload};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::time::{Duration, Instant};

const RECENT_DECLARED_LOSS_WINDOW: usize = 8192;

#[derive(Debug, Clone)]
pub(super) struct PendingPacket {
    pub(super) payload: PacketPayload,
    pub(super) encoded_len: usize,
    pub(super) sample: PacketSample,
    pub(super) sent_at: Instant,
    pub(super) last_sent_at: Instant,
    pub(super) deadline: Instant,
    pub(super) generation: u32,
    pub(super) retransmit_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PacketSample {
    Data { app_limited: bool },
    Control,
}

impl PacketSample {
    pub(super) fn counts_delivery_rate(self) -> bool {
        matches!(self, Self::Data { .. })
    }

    pub(super) fn app_limited(self) -> bool {
        matches!(self, Self::Data { app_limited: true })
    }

    pub(super) fn with_app_limited(self, app_limited: bool) -> Self {
        match self {
            Self::Data { .. } => Self::Data { app_limited },
            Self::Control => Self::Control,
        }
    }
}

#[derive(Debug)]
pub(super) struct AckedPacket {
    pub(super) packet_number: u64,
    pub(super) packet: PendingPacket,
}

#[derive(Debug)]
pub(super) struct AckOutcome {
    pub(super) released: Vec<AckedPacket>,
    pub(super) spurious_losses: u64,
}

#[derive(Debug)]
pub(super) struct RecoveryPacket {
    pub(super) payload: PacketPayload,
    pub(super) sample: PacketSample,
    pub(super) encoded_len: usize,
}

#[derive(Debug, Default)]
pub(super) struct PacketWindow {
    base: u64,
    slots: VecDeque<Option<PendingPacket>>,
    deadlines: BinaryHeap<Reverse<PacketDeadline>>,
    bytes: usize,
    next_probe_at: Option<Instant>,
    recent_declared_losses: HashSet<u64>,
    recent_declared_loss_order: VecDeque<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PacketDeadline {
    at: Instant,
    packet_number: u64,
    generation: u32,
}

impl PacketWindow {
    pub(super) fn insert(&mut self, packet_number: u64, packet: PendingPacket) {
        if self.slots.is_empty() {
            self.base = packet_number;
            self.next_probe_at = None;
        }
        if packet_number < self.base {
            return;
        }
        let Ok(index) = usize::try_from(packet_number - self.base) else {
            return;
        };
        if index >= self.slots.len() {
            self.slots.resize_with(index + 1, || None);
        }
        if let Some(existing) = self.slots[index].replace(packet) {
            self.bytes = self.bytes.saturating_sub(existing.encoded_len);
        }
        if let Some(packet) = self.slots[index].as_ref() {
            self.bytes = self.bytes.saturating_add(packet.encoded_len);
            self.deadlines.push(Reverse(PacketDeadline {
                at: packet.deadline,
                packet_number,
                generation: packet.generation,
            }));
        }
    }

    pub(super) fn remove_acked_ranges(&mut self, ranges: &[PacketAckRange]) -> AckOutcome {
        let mut released = Vec::new();
        let mut spurious_losses = 0u64;
        for range in ranges {
            if range.start >= range.end {
                continue;
            }
            if !self.recent_declared_losses.is_empty() {
                let spurious: Vec<u64> = self
                    .recent_declared_losses
                    .iter()
                    .copied()
                    .filter(|packet_number| {
                        range.start <= *packet_number && *packet_number < range.end
                    })
                    .collect();
                for packet_number in spurious {
                    if self.recent_declared_losses.remove(&packet_number) {
                        spurious_losses = spurious_losses.saturating_add(1);
                    }
                }
            }
            if !self.slots.is_empty() {
                let window_end = self.base.saturating_add(self.slots.len() as u64);
                let start = range.start.max(self.base);
                let end = range.end.min(window_end);
                if start >= end {
                    continue;
                }
                let start_index = usize::try_from(start - self.base).unwrap_or(usize::MAX);
                let end_index = usize::try_from(end - self.base).unwrap_or(usize::MAX);
                for index in start_index..end_index.min(self.slots.len()) {
                    if let Some(packet) = self.slots[index].take() {
                        self.bytes = self.bytes.saturating_sub(packet.encoded_len);
                        released.push(AckedPacket {
                            packet_number: self.base.saturating_add(index as u64),
                            packet,
                        });
                    }
                }
            }
        }
        self.trim_front();
        if !released.is_empty() || spurious_losses > 0 {
            self.next_probe_at = None;
        }
        AckOutcome {
            released,
            spurious_losses,
        }
    }

    pub(super) fn detect_losses(
        &mut self,
        largest_acked: u64,
        now: Instant,
        packet_threshold: u64,
        time_threshold: Duration,
        min_spacing: Duration,
        limit: usize,
    ) -> Vec<RecoveryPacket> {
        if largest_acked < self.base || self.slots.is_empty() || limit == 0 {
            return Vec::new();
        }
        let end = largest_acked.min(self.base.saturating_add(self.slots.len() as u64));
        let end_index = usize::try_from(end - self.base).unwrap_or(self.slots.len());
        let mut packets = Vec::new();
        for index in 0..end_index.min(self.slots.len()) {
            let packet_number = self.base.saturating_add(index as u64);
            let lost = {
                let Some(packet) = self.slots[index].as_ref() else {
                    continue;
                };
                if now.duration_since(packet.last_sent_at) < min_spacing {
                    continue;
                }
                let packet_gap_lost =
                    largest_acked.saturating_sub(packet_number) >= packet_threshold;
                let time_lost = now.duration_since(packet.sent_at) >= time_threshold;
                packet_gap_lost || time_lost
            };
            if !lost {
                continue;
            }
            if let Some(packet) = self.slots[index].take() {
                self.bytes = self.bytes.saturating_sub(packet.encoded_len);
                self.remember_declared_loss(packet_number);
                packets.push(RecoveryPacket {
                    payload: packet.payload,
                    sample: packet.sample,
                    encoded_len: packet.encoded_len,
                });
            }
            if packets.len() >= limit {
                break;
            }
        }
        self.trim_front();
        packets
    }

    pub(super) fn due_retransmits(
        &mut self,
        now: Instant,
        rto: Duration,
        limit: usize,
    ) -> Vec<RecoveryPacket> {
        if self.next_probe_at.is_some_and(|deadline| deadline > now) {
            return Vec::new();
        }
        let mut packets = Vec::new();
        while packets.len() < limit {
            let Some(Reverse(deadline)) = self.deadlines.peek().copied() else {
                break;
            };
            if deadline.at > now {
                break;
            }
            self.deadlines.pop();
            let Some(packet) = self.get_mut(deadline.packet_number) else {
                continue;
            };
            if packet.generation != deadline.generation || packet.deadline != deadline.at {
                continue;
            }
            let (payload, next_deadline, generation) = {
                let payload = Self::mark_retransmit(packet, now, rto);
                (payload, packet.deadline, packet.generation)
            };
            self.deadlines.push(Reverse(PacketDeadline {
                at: next_deadline,
                packet_number: deadline.packet_number,
                generation,
            }));
            packets.push(payload);
        }
        if !packets.is_empty() {
            self.next_probe_at = Some(now + rto.max(super::controller::MIN_RTO));
        }
        packets
    }

    fn get_mut(&mut self, packet_number: u64) -> Option<&mut PendingPacket> {
        if packet_number < self.base {
            return None;
        }
        let index = usize::try_from(packet_number - self.base).ok()?;
        self.slots.get_mut(index)?.as_mut()
    }

    fn mark_retransmit(
        packet: &mut PendingPacket,
        now: Instant,
        delay: Duration,
    ) -> RecoveryPacket {
        packet.last_sent_at = now;
        packet.retransmit_count = packet.retransmit_count.saturating_add(1);
        packet.generation = packet.generation.saturating_add(1);
        packet.deadline = now + delay.max(super::controller::MIN_RTO);
        RecoveryPacket {
            payload: packet.payload.clone(),
            sample: packet.sample,
            encoded_len: packet.encoded_len,
        }
    }

    fn remember_declared_loss(&mut self, packet_number: u64) {
        if self.recent_declared_losses.insert(packet_number) {
            self.recent_declared_loss_order.push_back(packet_number);
        }
        while self.recent_declared_loss_order.len() > RECENT_DECLARED_LOSS_WINDOW {
            if let Some(oldest) = self.recent_declared_loss_order.pop_front() {
                self.recent_declared_losses.remove(&oldest);
            }
        }
    }

    fn trim_front(&mut self) {
        while matches!(self.slots.front(), Some(None)) {
            self.slots.pop_front();
            self.base = self.base.saturating_add(1);
        }
        if self.slots.is_empty() {
            self.deadlines.clear();
            self.next_probe_at = None;
        }
    }
}
