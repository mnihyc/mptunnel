//! Direction-local inner-flow affinity and recent-load accounting.

use crate::model::tun_l3::IpPacketFlowKey;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::hash::Hash;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct PacketFlowBinding<C> {
    carrier: C,
    last_packet_at: Instant,
    flowlet_timeout: Duration,
    load_generation: u64,
    load_counted: bool,
}

#[derive(Debug)]
struct PacketFlowLoadExpiry {
    deadline: Instant,
    generation: u64,
    flow: IpPacketFlowKey,
}

impl PartialEq for PacketFlowLoadExpiry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.generation == other.generation
    }
}

impl Eq for PacketFlowLoadExpiry {}

impl PartialOrd for PacketFlowLoadExpiry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PacketFlowLoadExpiry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.generation.cmp(&other.generation))
    }
}

/// One bounded affinity table shared by the client and server packet planes.
///
/// Bindings survive an idle boundary so a later packet can make a fresh path
/// choice without allocating another cache entry. Load counts only flows that
/// have remained active within their own transport-derived flowlet timeout.
#[derive(Debug)]
pub(super) struct PacketFlowTable<C> {
    bindings: HashMap<IpPacketFlowKey, PacketFlowBinding<C>>,
    load: HashMap<C, u32>,
    load_expiries: BinaryHeap<Reverse<PacketFlowLoadExpiry>>,
    next_load_generation: u64,
    order: VecDeque<IpPacketFlowKey>,
    capacity: usize,
}

impl<C> PacketFlowTable<C>
where
    C: Copy + Eq + Hash,
{
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            bindings: HashMap::new(),
            load: HashMap::new(),
            load_expiries: BinaryHeap::new(),
            next_load_generation: 0,
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    pub(super) fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity.max(1);
        self.evict_to_capacity();
        self.compact_expiries_if_needed();
    }

    #[cfg(test)]
    pub(super) fn current(
        &mut self,
        flow: &IpPacketFlowKey,
        now: Instant,
        eligible: impl FnOnce(C) -> bool,
    ) -> Option<C> {
        self.expire(now);
        let binding = self.bindings.get_mut(flow)?;
        if !eligible(binding.carrier)
            || now.saturating_duration_since(binding.last_packet_at) >= binding.flowlet_timeout
        {
            return None;
        }
        binding.last_packet_at = now;
        Some(binding.carrier)
    }

    /// Observe an affinity candidate without refreshing its activity clock.
    /// Planning is advisory; only an accepted packet publication calls
    /// `bind`, so a rejected/stale plan cannot keep a flow artificially live.
    pub(super) fn planned_current(
        &mut self,
        flow: &IpPacketFlowKey,
        now: Instant,
        eligible: impl FnOnce(C) -> bool,
    ) -> Option<C> {
        self.expire(now);
        let binding = self.bindings.get(flow)?;
        if !eligible(binding.carrier)
            || now.saturating_duration_since(binding.last_packet_at) >= binding.flowlet_timeout
        {
            return None;
        }
        Some(binding.carrier)
    }

    /// Commit activity only after a packet was accepted by the carrier.
    ///
    /// The preceding `planned_current` read is advisory. A blocked, stale, or
    /// retired send must not extend affinity or recent-load ownership.
    pub(super) fn commit_planned_current(
        &mut self,
        flow: &IpPacketFlowKey,
        carrier: C,
        now: Instant,
    ) -> bool {
        self.expire(now);
        let Some(binding) = self.bindings.get_mut(flow) else {
            return false;
        };
        if binding.carrier != carrier
            || now.saturating_duration_since(binding.last_packet_at) >= binding.flowlet_timeout
        {
            return false;
        }
        binding.last_packet_at = now;
        true
    }

    pub(super) fn active_load_for(&self, carrier: C, excluding: &IpPacketFlowKey) -> u32 {
        self.load
            .get(&carrier)
            .copied()
            .unwrap_or(0)
            .saturating_sub(u32::from(self.bindings.get(excluding).is_some_and(
                |binding| binding.load_counted && binding.carrier == carrier,
            )))
    }

    pub(super) fn bind(
        &mut self,
        flow: IpPacketFlowKey,
        carrier: C,
        now: Instant,
        flowlet_timeout: Duration,
    ) {
        self.expire(now);
        if !self.bindings.contains_key(&flow) {
            self.order.push_back(flow.clone());
        }
        self.next_load_generation = self.next_load_generation.wrapping_add(1).max(1);
        let load_generation = self.next_load_generation;
        if let Some(previous) = self.bindings.insert(
            flow.clone(),
            PacketFlowBinding {
                carrier,
                last_packet_at: now,
                flowlet_timeout,
                load_generation,
                load_counted: true,
            },
        ) && previous.load_counted
        {
            decrement_load(&mut self.load, previous.carrier);
        }
        let count = self.load.entry(carrier).or_default();
        *count = count.saturating_add(1);
        self.load_expiries.push(Reverse(PacketFlowLoadExpiry {
            deadline: now.checked_add(flowlet_timeout).unwrap_or(now),
            generation: load_generation,
            flow,
        }));
        self.evict_to_capacity();
        self.compact_expiries_if_needed();
    }

    pub(super) fn remove_carrier(&mut self, carrier: C) {
        self.retain_carriers(|candidate| candidate != carrier);
    }

    pub(super) fn retain_carriers(&mut self, keep: impl Fn(C) -> bool) {
        self.bindings.retain(|_, binding| keep(binding.carrier));
        self.rebuild_load();
        let bindings = &self.bindings;
        self.order.retain(|flow| bindings.contains_key(flow));
    }

    fn expire(&mut self, now: Instant) {
        while self
            .load_expiries
            .peek()
            .is_some_and(|Reverse(expiry)| expiry.deadline <= now)
        {
            let Reverse(mut expiry) = self.load_expiries.pop().expect("peeked packet-flow expiry");
            let Some(binding) = self.bindings.get_mut(&expiry.flow) else {
                continue;
            };
            if !binding.load_counted || binding.load_generation != expiry.generation {
                continue;
            }
            let deadline = binding
                .last_packet_at
                .checked_add(binding.flowlet_timeout)
                .unwrap_or(now);
            if deadline > now {
                expiry.deadline = deadline;
                self.load_expiries.push(Reverse(expiry));
                continue;
            }
            binding.load_counted = false;
            decrement_load(&mut self.load, binding.carrier);
        }
    }

    fn evict_to_capacity(&mut self) {
        while self.bindings.len() > self.capacity {
            let Some(flow) = self.order.pop_front() else {
                break;
            };
            if let Some(binding) = self.bindings.remove(&flow)
                && binding.load_counted
            {
                decrement_load(&mut self.load, binding.carrier);
            }
        }
    }

    fn compact_expiries_if_needed(&mut self) {
        let live_bound = self.bindings.len().saturating_mul(2);
        if self.load_expiries.len() > live_bound {
            self.rebuild_load();
        }
    }

    fn rebuild_load(&mut self) {
        self.load.clear();
        self.load_expiries.clear();
        for (flow, binding) in &self.bindings {
            if !binding.load_counted {
                continue;
            }
            let count = self.load.entry(binding.carrier).or_default();
            *count = count.saturating_add(1);
            self.load_expiries.push(Reverse(PacketFlowLoadExpiry {
                deadline: binding
                    .last_packet_at
                    .checked_add(binding.flowlet_timeout)
                    .unwrap_or(binding.last_packet_at),
                generation: binding.load_generation,
                flow: flow.clone(),
            }));
        }
    }
}

fn decrement_load<C>(load: &mut HashMap<C, u32>, carrier: C)
where
    C: Copy + Eq + Hash,
{
    let remove = if let Some(count) = load.get_mut(&carrier) {
        *count = count.saturating_sub(1);
        *count == 0
    } else {
        false
    };
    if remove {
        load.remove(&carrier);
    }
}

#[cfg(test)]
#[path = "tests_flow.rs"]
mod tests;
