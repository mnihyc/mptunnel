//! Transient scored-range lookup for one serialized recovery pass.
//!
//! This index stores bucket geometry, never copied flight clocks, authority,
//! Native observations or payload. A max-end tree preserves arbitrarily old
//! crossing copies; taking only the preceding start bucket would be incorrect.
use super::RequestFlight;
use crate::protocol::OffsetRange;
use smallvec::SmallVec;
use std::collections::{BTreeMap, btree_map};

#[derive(Debug)]
pub(super) struct FlightRangeIndex {
    starts: Vec<u64>,
    max_ends: Vec<u64>,
    leaf_base: usize,
    horizon: u64,
    geometry_revision: u64,
    ledger_location: usize,
}

impl FlightRangeIndex {
    pub(super) fn new(
        flights: &BTreeMap<u64, Vec<RequestFlight>>,
        horizon: u64,
        geometry_revision: u64,
        ledger_location: usize,
    ) -> Self {
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        for (start, entries) in flights.range(..horizon) {
            starts.push(*start);
            ends.push(entries.iter().map(|f| f.end).max().unwrap_or(*start));
            #[cfg(test)]
            super::recovery_lookup_work(entries.len());
        }
        let leaf_base = starts.len().max(1).next_power_of_two();
        let mut max_ends = vec![0; leaf_base * 2];
        max_ends[leaf_base..leaf_base + ends.len()].copy_from_slice(&ends);
        for i in (1..leaf_base).rev() {
            max_ends[i] = max_ends[2 * i].max(max_ends[2 * i + 1]);
            #[cfg(test)]
            super::recovery_lookup_work(1);
        }
        Self {
            starts,
            max_ends,
            leaf_base,
            horizon,
            geometry_revision,
            ledger_location,
        }
    }

    // These checks only choose indexed versus canonical lookup; they never
    // grant publication. The existing view contract still requires one pass
    // of this same ledger before structural mutation. Movement or mutation
    // conservatively falls back. No pointer is dereferenced through the index.
    pub(super) fn matches(&self, range: OffsetRange, revision: u64, location: usize) -> bool {
        !range.is_empty()
            && range.end <= self.horizon
            && self.geometry_revision == revision
            && self.ledger_location == location
    }

    pub(super) fn starts(&self, range: OffsetRange) -> SmallVec<[u64; 16]> {
        let mut out = SmallVec::new();
        if range.is_empty() {
            return out;
        }
        let until = self.starts.partition_point(|start| *start < range.end);
        self.collect(1, 0, self.leaf_base, until, range.start, &mut out);
        out
    }

    fn collect(
        &self,
        node: usize,
        lo: usize,
        hi: usize,
        until: usize,
        start: u64,
        out: &mut SmallVec<[u64; 16]>,
    ) {
        #[cfg(test)]
        super::recovery_lookup_work(1);
        if lo >= until || self.max_ends[node] <= start {
            return;
        }
        if hi - lo == 1 {
            out.push(self.starts[lo]);
            return;
        }
        let mid = lo + (hi - lo) / 2;
        self.collect(node * 2, lo, mid, until, start, out);
        self.collect(node * 2 + 1, mid, hi, until, start, out);
    }
}

pub(super) enum FlightBuckets<'a> {
    Prefix(btree_map::Range<'a, u64, Vec<RequestFlight>>),
    Indexed {
        flights: &'a BTreeMap<u64, Vec<RequestFlight>>,
        starts: smallvec::IntoIter<[u64; 16]>,
    },
}
impl<'a> Iterator for FlightBuckets<'a> {
    type Item = (u64, &'a Vec<RequestFlight>);
    fn next(&mut self) -> Option<Self::Item> {
        let item = match self {
            Self::Prefix(iter) => iter.next().map(|(start, flights)| (*start, flights)),
            Self::Indexed { flights, starts } => {
                let start = starts.next()?;
                Some((
                    start,
                    flights
                        .get(&start)
                        .expect("same-pass indexed bucket exists"),
                ))
            }
        };
        #[cfg(test)]
        if let Some((_, flights)) = item {
            super::recovery_lookup_work(flights.len());
        }
        item
    }
}
