use super::packet::PacketAckRange;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::time::Instant;

pub(super) const ACK_FLUSH_PACKET_THRESHOLD: usize = 32;
pub(super) const ACK_IMMEDIATE_MIN_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(1);
pub(super) const MAX_ACK_RANGES_PER_PACKET: usize = 64;
const ACK_RECEIVE_HISTORY_WINDOW: usize = 65_536;

#[derive(Debug, Default)]
pub(super) struct AckState {
    pub(super) pending: Vec<u64>,
    received: BTreeSet<u64>,
    received_order: VecDeque<u64>,
    received_at: HashMap<u64, Instant>,
    pub(super) largest_seen: u64,
    pub(super) pending_largest_acked: Option<u64>,
    pub(super) pending_largest_acked_at: Option<Instant>,
    pub(super) last_flush_at: Option<Instant>,
    pub(super) scheduled: bool,
}

impl AckState {
    pub(super) fn remember_received(&mut self, packet_number: u64, now: Instant) -> bool {
        let inserted = self.received.insert(packet_number);
        if inserted {
            self.received_order.push_back(packet_number);
        }
        self.received_at.insert(packet_number, now);
        while self.received_order.len() > ACK_RECEIVE_HISTORY_WINDOW {
            let Some(oldest) = self.received_order.pop_front() else {
                break;
            };
            if self.received.remove(&oldest) {
                self.received_at.remove(&oldest);
            }
        }
        inserted
    }

    pub(super) fn ack_packets(&self) -> Vec<u64> {
        self.received.iter().copied().collect()
    }

    pub(super) fn received_at(&self, packet_number: u64) -> Option<Instant> {
        self.received_at.get(&packet_number).copied()
    }
}

pub(super) fn packet_ack_ranges(packets: &mut Vec<u64>) -> Vec<PacketAckRange> {
    packets.sort_unstable();
    packets.dedup();
    let mut ranges = Vec::new();
    let mut current: Option<PacketAckRange> = None;
    for packet_number in packets.iter().rev() {
        match current.as_mut() {
            Some(range) if packet_number.saturating_add(1) == range.start => {
                range.start = *packet_number;
            }
            Some(_) => {
                if let Some(range) = current.take() {
                    ranges.push(range);
                }
                current = Some(PacketAckRange {
                    start: *packet_number,
                    end: packet_number.saturating_add(1),
                });
            }
            None => {
                current = Some(PacketAckRange {
                    start: *packet_number,
                    end: packet_number.saturating_add(1),
                });
            }
        }
        if ranges.len() >= MAX_ACK_RANGES_PER_PACKET {
            break;
        }
    }
    if ranges.len() < MAX_ACK_RANGES_PER_PACKET
        && let Some(range) = current
    {
        ranges.push(range);
    }
    ranges
}
