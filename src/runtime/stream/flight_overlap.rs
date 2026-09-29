//! Product-flight overlap ownership shared by the request and response ledgers.
//!
//! `covered` (U) and `ambiguous` (M) describe byte multiplicity across the
//! retained Product publications. A response owner may compact finally retired
//! copies into coverage witnesses while preserving U/M until Product ACK; the
//! physical identity count then differs from historical multiplicity.
//! Publishing a new flight I applies
//! `M += U ∩ I; U += I`. An authoritative Product ACK removes every copy in
//! its mask, so callers snapshot M before mutation and then subtract the mask
//! from both unions. Failure or evidence invalidation does not release covered
//! bytes. Retained
//! ACK fragments are indexed without being published again. A selective
//! one-flight deletion is deliberately not exposed: U/M alone cannot represent
//! its multiplicity change.
//!
//! The AVL stores exact `(start, bucket order)` identities and subtree maximum
//! end offsets. It is deterministic for adversarial offsets and finds an old
//! long flight crossing a sparse late ACK without scanning the earlier map
//! prefix. The ordered BTreeMap+Vec remains the payload/order oracle; bucket
//! order is rebuilt whenever a bucket changes.

use crate::protocol::OffsetRange;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::ops::{
    Bound::{Excluded, Unbounded},
    Range,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::runtime::stream) struct FlightIndexKey {
    pub(in crate::runtime::stream) start: u64,
    pub(in crate::runtime::stream) order: usize,
}

#[derive(Debug, Default, Clone)]
struct IntervalUnion {
    ranges: BTreeMap<u64, u64>,
    covered_bytes: u128,
}

impl IntervalUnion {
    /// Return the slice of normalized, ordered ACK masks that can intersect
    /// this union. An envelope is sufficient to skip only masks wholly before
    /// its first byte or at/after its last byte; a long crossing range keeps
    /// the envelope wide enough to remain visible.
    fn relevant_mask_indices(&self, masks: &[OffsetRange]) -> Range<usize> {
        if masks.len() <= 1 {
            return 0..masks.len();
        }
        let Some((&first_start, _)) = self.ranges.first_key_value() else {
            return 0..0;
        };
        let (_, &last_end) = self.ranges.last_key_value().expect("nonempty union");
        let first = masks.partition_point(|mask| mask.end <= first_start);
        let end = masks.partition_point(|mask| mask.start < last_end);
        first..end
    }

    fn add(&mut self, range: OffsetRange) {
        if range.is_empty() {
            return;
        }
        // Every old interval encountered by the merge is disjoint. Account
        // only its overlap with the original input, not the expanded span.
        let mut newly_covered = range.end.saturating_sub(range.start) as u128;
        let mut start = range.start;
        let mut end = range.end;
        if let Some((&prior_start, &prior_end)) = self.ranges.range(..=start).next_back()
            && prior_end >= start
        {
            if prior_end >= end {
                return;
            }
            if prior_end == start
                && self
                    .ranges
                    .range((Excluded(prior_start), Unbounded))
                    .next()
                    .is_none_or(|(&next_start, _)| next_start > end)
            {
                *self
                    .ranges
                    .get_mut(&prior_start)
                    .expect("observed union range") = end;
                self.covered_bytes += newly_covered;
                return;
            }
            newly_covered -= prior_end
                .min(range.end)
                .saturating_sub(prior_start.max(range.start)) as u128;
            start = prior_start;
            end = end.max(prior_end);
            self.ranges.remove(&prior_start);
        }
        let merged = self
            .ranges
            .range(start..=end)
            .filter_map(|(&next_start, &next_end)| {
                (next_start <= end).then_some((next_start, next_end))
            })
            .collect::<Vec<_>>();
        for (next_start, next_end) in merged {
            newly_covered -= next_end
                .min(range.end)
                .saturating_sub(next_start.max(range.start)) as u128;
            end = end.max(next_end);
            self.ranges.remove(&next_start);
        }
        self.ranges.insert(start, end);
        self.covered_bytes += newly_covered;
    }

    fn intersections(&self, range: OffsetRange) -> Vec<OffsetRange> {
        self.overlapping_entries(range)
            .into_iter()
            .map(|(start, end)| OffsetRange {
                start: start.max(range.start),
                end: end.min(range.end),
            })
            .collect()
    }

    fn overlapping_entries(&self, range: OffsetRange) -> Vec<(u64, u64)> {
        if range.is_empty() {
            return Vec::new();
        }
        let mut result = Vec::new();
        if let Some((&start, &end)) = self.ranges.range(..=range.start).next_back()
            && end > range.start
        {
            result.push((start, end));
        }
        result.extend(
            self.ranges
                .range((Excluded(range.start), Excluded(range.end)))
                .map(|(&start, &end)| (start, end)),
        );
        result
    }

    fn subtract(&mut self, range: OffsetRange) {
        if range.is_empty() {
            return;
        }
        let affected = self.overlapping_entries(range);
        self.covered_bytes -= affected
            .iter()
            .map(|&(start, end)| end.min(range.end).saturating_sub(start.max(range.start)) as u128)
            .sum::<u128>();
        for (start, end) in affected {
            self.ranges.remove(&start);
            if start < range.start {
                self.ranges.insert(start, range.start.min(end));
            }
            if end > range.end {
                self.ranges.insert(range.end.max(start), end);
            }
        }
    }

    fn subset_of_ranges(&self, ranges: &[OffsetRange]) -> bool {
        let ack_bytes = ranges
            .iter()
            .map(|range| range.end.saturating_sub(range.start) as u128)
            .sum::<u128>();
        if ack_bytes < self.covered_bytes {
            return false;
        }
        if let (Some(first), Some(last), [one]) = (
            self.ranges.first_key_value(),
            self.ranges.last_key_value(),
            ranges,
        ) && one.start <= *first.0
            && one.end >= *last.1
        {
            return true;
        }
        let mut range_index = 0;
        for (&start, &end) in &self.ranges {
            while range_index < ranges.len() && ranges[range_index].end <= start {
                range_index += 1;
            }
            let Some(mask) = ranges.get(range_index) else {
                return false;
            };
            if mask.start > start || mask.end < end {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
struct Node {
    key: FlightIndexKey,
    end: u64,
    max_end: u64,
    height: i16,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
}

impl Node {
    fn new(key: FlightIndexKey, end: u64) -> Box<Self> {
        Box::new(Self {
            key,
            end,
            max_end: end,
            height: 1,
            left: None,
            right: None,
        })
    }

    fn height(node: &Option<Box<Node>>) -> i16 {
        node.as_ref().map_or(0, |node| node.height)
    }

    fn max_end(node: &Option<Box<Node>>) -> u64 {
        node.as_ref().map_or(0, |node| node.max_end)
    }

    fn refresh(&mut self) {
        self.height = 1 + Self::height(&self.left).max(Self::height(&self.right));
        self.max_end = self
            .end
            .max(Self::max_end(&self.left))
            .max(Self::max_end(&self.right));
    }

    fn rotate_right(mut root: Box<Self>) -> Box<Self> {
        let mut pivot = root.left.take().expect("AVL left rotation precondition");
        root.left = pivot.right.take();
        root.refresh();
        pivot.right = Some(root);
        pivot.refresh();
        pivot
    }

    fn rotate_left(mut root: Box<Self>) -> Box<Self> {
        let mut pivot = root.right.take().expect("AVL right rotation precondition");
        root.right = pivot.left.take();
        root.refresh();
        pivot.left = Some(root);
        pivot.refresh();
        pivot
    }

    fn balance(mut root: Box<Self>) -> Box<Self> {
        root.refresh();
        let factor = Self::height(&root.left) - Self::height(&root.right);
        if factor > 1 {
            let left = root.left.as_ref().expect("AVL balance left precondition");
            if Self::height(&left.left) < Self::height(&left.right) {
                root.left = root.left.take().map(Self::rotate_left);
            }
            return Self::rotate_right(root);
        }
        if factor < -1 {
            let right = root.right.as_ref().expect("AVL balance right precondition");
            if Self::height(&right.right) < Self::height(&right.left) {
                root.right = root.right.take().map(Self::rotate_right);
            }
            return Self::rotate_left(root);
        }
        root
    }

    fn insert(
        root: Option<Box<Self>>,
        key: FlightIndexKey,
        end: u64,
        inserted: &mut bool,
    ) -> Option<Box<Self>> {
        let Some(mut root) = root else {
            *inserted = true;
            return Some(Self::new(key, end));
        };
        if key < root.key {
            root.left = Self::insert(root.left.take(), key, end, inserted);
        } else if key > root.key {
            root.right = Self::insert(root.right.take(), key, end, inserted);
        } else {
            root.end = end;
            return Some(Self::balance(root));
        }
        Some(Self::balance(root))
    }

    fn take_min(mut root: Box<Self>) -> (Option<Box<Self>>, Box<Self>) {
        let Some(left) = root.left.take() else {
            let right = root.right.take();
            return (right, root);
        };
        let (new_left, min) = Self::take_min(left);
        root.left = new_left;
        (Some(Self::balance(root)), min)
    }

    fn remove(
        root: Option<Box<Self>>,
        key: FlightIndexKey,
        removed: &mut bool,
    ) -> Option<Box<Self>> {
        let mut root = root?;
        if key < root.key {
            root.left = Self::remove(root.left.take(), key, removed);
            return Some(Self::balance(root));
        }
        if key > root.key {
            root.right = Self::remove(root.right.take(), key, removed);
            return Some(Self::balance(root));
        }
        *removed = true;
        match (root.left.take(), root.right.take()) {
            (None, right) => right,
            (left, None) => left,
            (left, Some(right)) => {
                let (new_right, mut successor) = Self::take_min(right);
                successor.left = left;
                successor.right = new_right;
                Some(Self::balance(successor))
            }
        }
    }

    fn query(
        node: &Option<Box<Self>>,
        range: OffsetRange,
        output: &mut SmallVec<[FlightIndexKey; 4]>,
    ) {
        let Some(node) = node else { return };
        if range.is_empty() || node.max_end <= range.start {
            return;
        }
        if Self::max_end(&node.left) > range.start {
            Self::query(&node.left, range, output);
        }
        if node.key.start < range.end && node.end > range.start {
            output.push(node.key);
        }
        if node.key.start < range.end {
            Self::query(&node.right, range, output);
        }
    }

    #[cfg(test)]
    fn assert_valid(
        node: &Option<Box<Self>>,
        low: Option<FlightIndexKey>,
        high: Option<FlightIndexKey>,
    ) -> (i16, u64, usize) {
        let Some(node) = node else { return (0, 0, 0) };
        assert!(low.is_none_or(|low| low < node.key));
        assert!(high.is_none_or(|high| node.key < high));
        let (lh, lm, ln) = Self::assert_valid(&node.left, low, Some(node.key));
        let (rh, rm, rn) = Self::assert_valid(&node.right, Some(node.key), high);
        assert!((lh - rh).abs() <= 1);
        assert_eq!(node.height, 1 + lh.max(rh));
        assert_eq!(node.max_end, node.end.max(lm).max(rm));
        (node.height, node.max_end, ln + rn + 1)
    }
}

#[derive(Debug, Default, Clone)]
struct IndexedFlightState {
    root: Option<Box<Node>>,
    len: usize,
    covered: IntervalUnion,
    ambiguous: IntervalUnion,
}

impl IndexedFlightState {
    fn publish(&mut self, key: FlightIndexKey, end: u64) {
        assert!(key.start < end, "published Product flight must be nonempty");
        let range = OffsetRange {
            start: key.start,
            end,
        };
        let newly_ambiguous = self.covered.intersections(range);
        for piece in newly_ambiguous {
            self.ambiguous.add(piece);
        }
        self.covered.add(range);
        self.insert_retained(key, end);
    }

    /// Index an already-owned fragment without treating it as another copy.
    fn insert_retained(&mut self, key: FlightIndexKey, end: u64) {
        assert!(
            key.start < end,
            "retained Product fragment must be nonempty"
        );
        let mut inserted = false;
        self.root = Node::insert(self.root.take(), key, end, &mut inserted);
        assert!(inserted, "duplicate Product interval identity");
        self.len += 1;
    }

    fn remove(&mut self, key: FlightIndexKey) {
        let mut removed = false;
        self.root = Node::remove(self.root.take(), key, &mut removed);
        assert!(removed, "removing a missing Product interval identity");
        self.len -= 1;
    }

    fn intersecting(&self, ranges: &[OffsetRange]) -> SmallVec<[FlightIndexKey; 4]> {
        let mut keys = SmallVec::new();
        let relevant = &ranges[self.covered.relevant_mask_indices(ranges)];
        for &range in relevant {
            Node::query(&self.root, range, &mut keys);
        }
        // A single mask is queried once, and the tree visits in key order.
        // Multiple disjoint masks can select one long flight repeatedly.
        if relevant.len() > 1 {
            keys.sort_unstable();
            keys.dedup();
        }
        keys
    }

    fn ambiguous_intersections_for_ack(&self, ranges: &[OffsetRange]) -> Vec<OffsetRange> {
        let relevant = self.ambiguous.relevant_mask_indices(ranges);
        ranges[relevant]
            .iter()
            .flat_map(|&range| self.ambiguous.intersections(range))
            .collect()
    }

    fn acknowledge(&mut self, ranges: &[OffsetRange]) {
        let relevant = self.covered.relevant_mask_indices(ranges);
        for &range in &ranges[relevant] {
            self.covered.subtract(range);
            self.ambiguous.subtract(range);
        }
    }

    fn ack_covers_all(&self, ranges: &[OffsetRange]) -> bool {
        let relevant = self.covered.relevant_mask_indices(ranges);
        self.covered.subset_of_ranges(&ranges[relevant])
    }

    /// Rebuild just one touched ordered bucket after a split/removal. The
    /// payload vector remains the output-order oracle.
    fn rebuild_bucket(&mut self, start: u64, old_len: usize, ends: impl IntoIterator<Item = u64>) {
        for order in 0..old_len {
            self.remove(FlightIndexKey { start, order });
        }
        for (order, end) in ends.into_iter().enumerate() {
            self.insert_retained(FlightIndexKey { start, order }, end);
        }
    }

    #[cfg(test)]
    fn assert_valid(&self) -> usize {
        let (_, _, count) = Node::assert_valid(&self.root, None, None);
        assert_eq!(count, self.len);
        count
    }
}

#[derive(Debug, Default, Clone)]
enum ProductFlightIndexState {
    #[default]
    Empty,
    Single {
        key: FlightIndexKey,
        end: u64,
    },
    Indexed(IndexedFlightState),
}

/// One Product owner's overlap state. Its methods intentionally distinguish a
/// new accepted publication from storage of an ACK-retained fragment. Empty
/// and one-flight ledgers stay inline; a partial ACK keeps Indexed alive until
/// the serialized mutation is complete, even if bucket rebuilding temporarily
/// removes every indexed identity.
#[derive(Debug, Default, Clone)]
pub(in crate::runtime::stream) struct ProductFlightIndex {
    state: ProductFlightIndexState,
}

impl ProductFlightIndex {
    /// Add a newly accepted Product flight and update U/M from pre-insert U.
    pub(in crate::runtime::stream) fn publish(&mut self, key: FlightIndexKey, end: u64) {
        assert!(key.start < end, "published Product flight must be nonempty");
        if let ProductFlightIndexState::Indexed(indexed) = &mut self.state {
            indexed.publish(key, end);
            return;
        }
        self.state = match std::mem::take(&mut self.state) {
            ProductFlightIndexState::Empty => ProductFlightIndexState::Single { key, end },
            ProductFlightIndexState::Single {
                key: prior_key,
                end: prior_end,
            } => {
                let mut indexed = IndexedFlightState::default();
                indexed.covered.add(OffsetRange {
                    start: prior_key.start,
                    end: prior_end,
                });
                indexed.insert_retained(prior_key, prior_end);
                indexed.publish(key, end);
                ProductFlightIndexState::Indexed(indexed)
            }
            ProductFlightIndexState::Indexed(_) => unreachable!("indexed publication was handled"),
        };
    }

    /// Return exact retained identities intersecting an ACK mask, in Product
    /// ledger order, with duplicate hits across mask ranges removed.
    pub(in crate::runtime::stream) fn intersecting(
        &self,
        ranges: &[OffsetRange],
    ) -> SmallVec<[FlightIndexKey; 4]> {
        match &self.state {
            ProductFlightIndexState::Empty => SmallVec::new(),
            ProductFlightIndexState::Single { key, end } => {
                let mut keys = SmallVec::new();
                if ranges
                    .iter()
                    .any(|range| key.start < range.end && *end > range.start)
                {
                    keys.push(*key);
                }
                keys
            }
            ProductFlightIndexState::Indexed(indexed) => indexed.intersecting(ranges),
        }
    }

    #[cfg(test)]
    fn ambiguous_intersections(&self, range: OffsetRange) -> Vec<OffsetRange> {
        match &self.state {
            ProductFlightIndexState::Indexed(indexed) => indexed.ambiguous.intersections(range),
            ProductFlightIndexState::Empty | ProductFlightIndexState::Single { .. } => Vec::new(),
        }
    }

    pub(in crate::runtime::stream) fn ambiguous_intersections_for_ack(
        &self,
        ranges: &[OffsetRange],
    ) -> Vec<OffsetRange> {
        match &self.state {
            ProductFlightIndexState::Indexed(indexed) => {
                indexed.ambiguous_intersections_for_ack(ranges)
            }
            ProductFlightIndexState::Empty | ProductFlightIndexState::Single { .. } => Vec::new(),
        }
    }

    pub(in crate::runtime::stream) fn ack_covers_all(&self, ranges: &[OffsetRange]) -> bool {
        match &self.state {
            ProductFlightIndexState::Empty => true,
            ProductFlightIndexState::Single { key, end } => {
                let mut cursor = key.start;
                for range in ranges {
                    if range.end <= cursor {
                        continue;
                    }
                    if range.start > cursor {
                        return false;
                    }
                    cursor = cursor.max(range.end.min(*end));
                    if cursor >= *end {
                        return true;
                    }
                }
                false
            }
            ProductFlightIndexState::Indexed(indexed) => indexed.ack_covers_all(ranges),
        }
    }

    /// Update the identity set while preserving the current lifetime's U/M.
    /// Final-retirement compaction may retain one non-proving coverage witness
    /// in place of nested retired copies. Their historical ambiguity remains in
    /// M until a Product ACK; do not infer M afresh from compact physical rows.
    /// Single retains its original summary until `finish_partial_ack`, because
    /// a split may briefly have no start-key fragment before a staged right
    /// fragment is reinserted.
    pub(in crate::runtime::stream) fn rebuild_bucket(
        &mut self,
        start: u64,
        old_len: usize,
        ends: impl IntoIterator<Item = u64>,
    ) {
        match &mut self.state {
            ProductFlightIndexState::Empty => {
                let mut ends = ends.into_iter();
                assert!(
                    old_len == 0 && ends.next().is_none(),
                    "empty overlap ledger bucket"
                );
            }
            ProductFlightIndexState::Single { .. } => {
                // The owner has at most one identity; reconstruct it from the
                // final retained payload only after the entire ACK transaction.
            }
            ProductFlightIndexState::Indexed(indexed) => {
                indexed.rebuild_bucket(start, old_len, ends);
            }
        }
    }

    /// Commit a partial global ACK after payload fragments and ordered buckets
    /// have reached their final shape. Indexed owners preserve the incremental
    /// U/M algebra; a former Single owner can promote only if it actually has
    /// multiple retained fragments, inserted as geometry rather than copies.
    pub(in crate::runtime::stream) fn finish_partial_ack(
        &mut self,
        ranges: &[OffsetRange],
        retained: impl IntoIterator<Item = (FlightIndexKey, u64)>,
    ) {
        let indexed_empty = if let ProductFlightIndexState::Indexed(indexed) = &mut self.state {
            indexed.acknowledge(ranges);
            let empty = indexed.len == 0;
            if empty {
                assert!(indexed.root.is_none());
            }
            Some(empty)
        } else {
            None
        };
        if let Some(empty) = indexed_empty {
            if empty {
                self.state = ProductFlightIndexState::Empty;
            }
            return;
        }

        let state = std::mem::take(&mut self.state);
        match state {
            ProductFlightIndexState::Empty => {
                assert!(retained.into_iter().next().is_none());
            }
            ProductFlightIndexState::Single { .. } => {
                let mut fragments = retained.into_iter();
                let Some((key, end)) = fragments.next() else {
                    self.state = ProductFlightIndexState::Empty;
                    return;
                };
                assert!(
                    key.start < end,
                    "retained Product fragment must be nonempty"
                );
                let Some((second_key, second_end)) = fragments.next() else {
                    self.state = ProductFlightIndexState::Single { key, end };
                    return;
                };
                let mut indexed = IndexedFlightState::default();
                for (key, end) in [(key, end), (second_key, second_end)]
                    .into_iter()
                    .chain(fragments)
                {
                    assert!(
                        key.start < end,
                        "retained Product fragment must be nonempty"
                    );
                    debug_assert!(
                        indexed
                            .covered
                            .intersections(OffsetRange {
                                start: key.start,
                                end
                            })
                            .is_empty()
                    );
                    indexed.covered.add(OffsetRange {
                        start: key.start,
                        end,
                    });
                    indexed.insert_retained(key, end);
                }
                self.state = ProductFlightIndexState::Indexed(indexed);
            }
            ProductFlightIndexState::Indexed(_) => unreachable!("indexed ACK was handled"),
        }
    }

    /// Test-only direct algebra path for a stable indexed owner.
    #[cfg(test)]
    fn acknowledge(&mut self, ranges: &[OffsetRange]) {
        match &mut self.state {
            ProductFlightIndexState::Indexed(indexed) => indexed.acknowledge(ranges),
            ProductFlightIndexState::Empty | ProductFlightIndexState::Single { .. } => {
                panic!("direct ACK algebra requires indexed owner")
            }
        }
    }

    #[cfg(test)]
    fn remove(&mut self, key: FlightIndexKey) {
        match &mut self.state {
            ProductFlightIndexState::Indexed(indexed) => indexed.remove(key),
            ProductFlightIndexState::Empty | ProductFlightIndexState::Single { .. } => {
                panic!("direct identity removal requires indexed owner")
            }
        }
    }

    #[cfg(test)]
    fn insert_retained(&mut self, key: FlightIndexKey, end: u64) {
        match &mut self.state {
            ProductFlightIndexState::Indexed(indexed) => indexed.insert_retained(key, end),
            ProductFlightIndexState::Empty | ProductFlightIndexState::Single { .. } => {
                panic!("direct fragment insertion requires indexed owner")
            }
        }
    }

    pub(in crate::runtime::stream) fn clear(&mut self) {
        self.state = ProductFlightIndexState::Empty;
    }

    #[cfg(test)]
    pub(in crate::runtime::stream) fn flight_count(&self) -> usize {
        match &self.state {
            ProductFlightIndexState::Empty => 0,
            ProductFlightIndexState::Single { .. } => 1,
            ProductFlightIndexState::Indexed(indexed) => indexed.len,
        }
    }

    #[cfg(test)]
    pub(in crate::runtime::stream) fn covered_intersections(
        &self,
        range: OffsetRange,
    ) -> Vec<OffsetRange> {
        match &self.state {
            ProductFlightIndexState::Indexed(indexed) => indexed.covered.intersections(range),
            ProductFlightIndexState::Single { key, end } => {
                let start = key.start.max(range.start);
                let end = (*end).min(range.end);
                (start < end)
                    .then_some(OffsetRange { start, end })
                    .into_iter()
                    .collect()
            }
            ProductFlightIndexState::Empty => Vec::new(),
        }
    }

    #[cfg(test)]
    fn assert_valid(&self) -> usize {
        match &self.state {
            ProductFlightIndexState::Empty => 0,
            ProductFlightIndexState::Single { key, end } => {
                assert!(key.start < *end);
                1
            }
            ProductFlightIndexState::Indexed(indexed) => indexed.assert_valid(),
        }
    }

    #[cfg(test)]
    pub(in crate::runtime::stream) fn layout_for_test() -> (usize, usize, usize, usize) {
        (
            std::mem::size_of::<Self>(),
            std::mem::size_of::<Node>(),
            std::mem::size_of::<IntervalUnion>(),
            std::mem::size_of::<FlightIndexKey>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn r(start: u64, end: u64) -> OffsetRange {
        OffsetRange { start, end }
    }

    fn byte_set_ranges(bytes: &BTreeSet<u64>) -> Vec<OffsetRange> {
        let mut ranges = Vec::<OffsetRange>::new();
        for &byte in bytes {
            if let Some(last) = ranges.last_mut()
                && last.end == byte
            {
                last.end = byte + 1;
                continue;
            }
            ranges.push(OffsetRange {
                start: byte,
                end: byte + 1,
            });
        }
        ranges
    }

    #[test]
    fn interval_union_mutations_match_bounded_byte_set_at_u64_edge() {
        let base = u64::MAX - 24;
        let operations = [
            (true, r(base + 2, base + 10)),
            (true, r(base + 15, u64::MAX)),
            (true, r(base + 9, base + 16)),
            (false, r(base + 4, base + 6)),
            (false, r(base + 6, base + 8)),
            (true, r(base + 4, base + 8)),
            (false, r(base + 17, u64::MAX - 2)),
            (true, r(u64::MAX - 1, u64::MAX)),
            (false, r(base, base + 3)),
            (true, r(base + 3, base + 3)),
            (false, r(u64::MAX, u64::MAX)),
            (true, r(base + 1, base + 2)),
        ];
        let mut union = IntervalUnion::default();
        let mut oracle = BTreeSet::new();

        for (add, range) in operations {
            if add {
                union.add(range);
                oracle.extend(range.start..range.end);
            } else {
                union.subtract(range);
                for byte in range.start..range.end {
                    oracle.remove(&byte);
                }
            }

            let expected = byte_set_ranges(&oracle);
            assert_eq!(union.covered_bytes, oracle.len() as u128);
            assert_eq!(union.intersections(r(base, u64::MAX)), expected);
            assert!(
                union.subset_of_ranges(&[r(base, u64::MAX)]),
                "the full mask covers every retained byte"
            );
            assert_eq!(
                union.subset_of_ranges(&[]),
                oracle.is_empty(),
                "empty masks cover only an empty union"
            );

            let partial_masks = [r(base, base + 8), r(base + 14, u64::MAX)];
            let expected_subset = oracle.iter().all(|byte| {
                partial_masks
                    .iter()
                    .any(|mask| mask.start <= *byte && *byte < mask.end)
            });
            assert_eq!(union.subset_of_ranges(&partial_masks), expected_subset);
        }
    }

    #[test]
    fn union_insertions_match_byte_set_and_full_width_coverage() {
        for base in [0, u64::MAX - 6] {
            for coverage_bits in 0u64..64 {
                let bytes = (0..6)
                    .filter(|bit| coverage_bits & (1 << bit) != 0)
                    .map(|bit| base + bit)
                    .collect::<BTreeSet<_>>();
                let mut initial = IntervalUnion::default();
                for covered in byte_set_ranges(&bytes) {
                    initial.add(covered);
                }
                for start in 0..=6 {
                    for end in start..=6 {
                        let mut union = initial.clone();
                        union.add(r(base + start, base + end));
                        let mut expected = bytes.clone();
                        expected.extend(base + start..base + end);
                        assert_eq!(union.covered_bytes, expected.len() as u128);
                        assert_eq!(
                            union.intersections(r(base, base + 6)),
                            byte_set_ranges(&expected)
                        );
                    }
                }
            }
        }
        let mut wide = IntervalUnion::default();
        wide.add(r(4, u64::MAX - 4));
        wide.add(r(0, 8));
        wide.add(r(u64::MAX - 8, u64::MAX));
        assert_eq!(wide.covered_bytes, u64::MAX as u128);
        assert_eq!(wide.intersections(r(0, u64::MAX)), vec![r(0, u64::MAX)]);
        wide.add(r(1, u64::MAX - 1));
        assert_eq!(wide.covered_bytes, u64::MAX as u128);
        wide.subtract(r(1, u64::MAX - 1));
        assert_eq!(wide.covered_bytes, 2);
        wide.add(r(0, u64::MAX));
        assert_eq!(wide.covered_bytes, u64::MAX as u128);
        assert_eq!(wide.intersections(r(0, u64::MAX)), vec![r(0, u64::MAX)]);
    }

    #[test]
    fn normalized_mask_bounds_match_full_union_operations() {
        // Exhaust every coverage/mask combination over a small domain, then
        // translate it to the largest offsets. The oracle visits every mask;
        // it does not use envelope endpoints or the seek operation.
        for base in [0, u64::MAX - 6] {
            for coverage_bits in 0u64..64 {
                let mut union = IntervalUnion::default();
                for bit in 0..6 {
                    if coverage_bits & (1 << bit) != 0 {
                        union.add(r(base + bit, base + bit + 1));
                    }
                }
                for ack_bits in 0u64..64 {
                    let bytes = (0..6)
                        .filter(|bit| ack_bits & (1 << bit) != 0)
                        .map(|bit| base + bit)
                        .collect::<BTreeSet<_>>();
                    let masks = byte_set_ranges(&bytes);
                    let selected = &masks[union.relevant_mask_indices(&masks)];
                    let intersections = |masks: &[OffsetRange]| {
                        masks
                            .iter()
                            .flat_map(|&mask| union.intersections(mask))
                            .collect::<Vec<_>>()
                    };
                    assert_eq!(intersections(selected), intersections(&masks));
                    assert_eq!(
                        union.subset_of_ranges(selected),
                        union.subset_of_ranges(&masks)
                    );
                    let mut sought = union.clone();
                    for &mask in selected {
                        sought.subtract(mask);
                    }
                    let mut full = union.clone();
                    for &mask in &masks {
                        full.subtract(mask);
                    }
                    assert_eq!(sought.ranges, full.ranges);
                    assert_eq!(sought.covered_bytes, full.covered_bytes);
                }
            }
        }
    }

    #[test]
    fn union_insert_and_global_ack_preserve_multiplicity() {
        let mut index = ProductFlightIndex::default();
        index.publish(FlightIndexKey { start: 0, order: 0 }, 100);
        index.publish(
            FlightIndexKey {
                start: 40,
                order: 0,
            },
            80,
        );
        assert_eq!(index.ambiguous_intersections(r(0, 100)), vec![r(40, 80)]);
        index.acknowledge(&[r(40, 60)]);
        assert_eq!(index.ambiguous_intersections(r(0, 100)), vec![r(60, 80)]);
        assert_eq!(
            index.covered_intersections(r(0, 100)),
            vec![r(0, 40), r(60, 100)]
        );
        assert_eq!(index.intersecting(&[r(0, 100)]).len(), 2);
        index.assert_valid();
    }

    #[test]
    fn exact_index_finds_old_long_crossing_flight_and_orders_collisions() {
        let mut index = ProductFlightIndex::default();
        index.publish(FlightIndexKey { start: 0, order: 0 }, 1_000_000);
        for start in 10..10_010 {
            index.publish(FlightIndexKey { start, order: 0 }, start + 1);
        }
        index.publish(
            FlightIndexKey {
                start: 10_000,
                order: 1,
            },
            10_002,
        );
        let hits = index.intersecting(&[r(10_000, 10_001)]);
        assert_eq!(
            hits.as_slice(),
            &[
                FlightIndexKey { start: 0, order: 0 },
                FlightIndexKey {
                    start: 10_000,
                    order: 0
                },
                FlightIndexKey {
                    start: 10_000,
                    order: 1
                },
            ]
        );
        assert_eq!(index.assert_valid(), 10_002);
    }

    #[test]
    fn normalized_sparse_masks_match_full_scan_and_keep_long_crossing_flights() {
        let masks = (0..256)
            .map(|index| r(index * 40, index * 40 + 1))
            .collect::<Vec<_>>();
        let sparse_flights = [
            (
                FlightIndexKey {
                    start: 10_000,
                    order: 0,
                },
                10_002,
            ),
            (
                FlightIndexKey {
                    start: 10_000,
                    order: 1,
                },
                10_001,
            ),
            (
                FlightIndexKey {
                    start: 10_200,
                    order: 0,
                },
                10_202,
            ),
        ];
        let mut sparse = ProductFlightIndex::default();
        for (key, end) in sparse_flights {
            sparse.publish(key, end);
        }

        let expected_keys = sparse_flights
            .iter()
            .filter_map(|&(key, end)| {
                masks
                    .iter()
                    .any(|mask| key.start < mask.end && end > mask.start)
                    .then_some(key)
            })
            .collect::<Vec<_>>();
        assert_eq!(sparse.intersecting(&masks).as_slice(), expected_keys);
        let ProductFlightIndexState::Indexed(sparse_indexed) = &sparse.state else {
            panic!("three published flights must use the indexed state")
        };
        let expected_ambiguity = masks
            .iter()
            .flat_map(|&mask| sparse_indexed.ambiguous.intersections(mask))
            .collect::<Vec<_>>();
        assert_eq!(
            sparse.ambiguous_intersections_for_ack(&masks),
            expected_ambiguity
        );
        assert_eq!(
            sparse.ack_covers_all(&masks),
            sparse_indexed.covered.subset_of_ranges(&masks)
        );

        // Compare the bounded implementation with the former full mask walk
        // after a partial ACK, including both union bytes and ambiguity.
        let mut optimized = sparse.clone();
        optimized.acknowledge(&masks);
        let mut full_scan = sparse.clone();
        let ProductFlightIndexState::Indexed(full_scan_indexed) = &mut full_scan.state else {
            unreachable!("clone preserves the indexed state")
        };
        for &mask in &masks {
            full_scan_indexed.covered.subtract(mask);
            full_scan_indexed.ambiguous.subtract(mask);
        }
        assert_eq!(
            optimized.covered_intersections(r(0, 20_300)),
            full_scan.covered_intersections(r(0, 20_300))
        );
        assert_eq!(
            optimized.ambiguous_intersections(r(0, 20_300)),
            full_scan.ambiguous_intersections(r(0, 20_300))
        );

        // A crossing flight widens U's envelope back to zero, so masks at its
        // beginning still find it even though most other retained flights are
        // near the late end of the ACK list.
        let crossing_flights = [
            (FlightIndexKey { start: 0, order: 0 }, 10_002),
            (
                FlightIndexKey {
                    start: 10_000,
                    order: 0,
                },
                10_002,
            ),
        ];
        let mut crossing = ProductFlightIndex::default();
        for (key, end) in crossing_flights {
            crossing.publish(key, end);
        }
        let ProductFlightIndexState::Indexed(crossing_indexed) = &crossing.state else {
            unreachable!("two published flights must use the indexed state")
        };
        assert_eq!(
            crossing_indexed.covered.relevant_mask_indices(&masks).start,
            0
        );
        let expected_crossing_keys = crossing_flights
            .iter()
            .filter_map(|&(key, end)| {
                masks
                    .iter()
                    .any(|mask| key.start < mask.end && end > mask.start)
                    .then_some(key)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            crossing.intersecting(&masks).as_slice(),
            expected_crossing_keys
        );
        let expected_crossing_ambiguity = masks
            .iter()
            .flat_map(|&mask| crossing_indexed.ambiguous.intersections(mask))
            .collect::<Vec<_>>();
        assert_eq!(
            crossing.ambiguous_intersections_for_ack(&masks),
            expected_crossing_ambiguity
        );
    }

    #[test]
    fn retained_fragment_is_not_a_second_publication() {
        let mut index = ProductFlightIndex::default();
        index.publish(FlightIndexKey { start: 0, order: 0 }, 100);
        index.finish_partial_ack(
            &[r(40, 60)],
            [
                (FlightIndexKey { start: 0, order: 0 }, 40),
                (
                    FlightIndexKey {
                        start: 60,
                        order: 0,
                    },
                    100,
                ),
            ],
        );
        assert!(index.ambiguous_intersections(r(0, 100)).is_empty());
        index.assert_valid();
    }

    #[test]
    fn empty_single_and_fragmented_owners_use_actual_retained_cardinality() {
        let key = FlightIndexKey { start: 0, order: 0 };
        let mut index = ProductFlightIndex::default();
        assert!(matches!(&index.state, ProductFlightIndexState::Empty));

        index.publish(key, 100);
        assert!(matches!(
            &index.state,
            ProductFlightIndexState::Single { .. }
        ));
        assert_eq!(index.intersecting(&[r(20, 30)]).as_slice(), &[key]);
        assert_eq!(
            index.intersecting(&[r(20, 30), r(40, 50)]).as_slice(),
            &[key]
        );

        // A middle ACK leaves two real fragments and promotes from their final
        // geometry without replaying either as a new publication.
        index.finish_partial_ack(
            &[r(40, 60)],
            [
                (FlightIndexKey { start: 0, order: 0 }, 40),
                (
                    FlightIndexKey {
                        start: 60,
                        order: 0,
                    },
                    100,
                ),
            ],
        );
        let ProductFlightIndexState::Indexed(indexed) = &index.state else {
            panic!("two retained fragments must promote to Indexed")
        };
        assert_eq!(indexed.len, 2);
        assert!(indexed.ambiguous.intersections(r(0, 100)).is_empty());
        assert_eq!(
            index.covered_intersections(r(0, 100)),
            vec![r(0, 40), r(60, 100)]
        );

        // A following ACK removes both identities, then completed-empty state
        // demotes to Empty. No temporary index-empty transition loses geometry.
        index.rebuild_bucket(0, 1, []);
        index.rebuild_bucket(60, 1, []);
        assert!(matches!(&index.state, ProductFlightIndexState::Indexed(i) if i.len == 0));
        index.finish_partial_ack(&[r(0, 40), r(60, 100)], []);
        assert!(matches!(&index.state, ProductFlightIndexState::Empty));
        assert_eq!(index.assert_valid(), 0);
    }

    #[test]
    fn indexed_rebuild_keeps_pre_ack_ambiguity_until_repeated_ack_finishes() {
        let first = FlightIndexKey { start: 0, order: 0 };
        let second = FlightIndexKey {
            start: 20,
            order: 0,
        };
        let third = FlightIndexKey {
            start: 20,
            order: 1,
        };
        let mut index = ProductFlightIndex::default();
        index.publish(first, 100);
        index.publish(second, 120);
        index.publish(third, 110);
        assert_eq!(index.ambiguous_intersections(r(0, 120)), vec![r(20, 110)]);

        // Model the production remove-all-then-stage-right-fragments boundary.
        // The AVL becomes temporarily empty, but old U/M remain the ACK oracle.
        index.rebuild_bucket(0, 1, []);
        index.rebuild_bucket(20, 2, []);
        assert!(matches!(&index.state, ProductFlightIndexState::Indexed(i) if i.len == 0));
        assert_eq!(index.ambiguous_intersections(r(0, 120)), vec![r(20, 110)]);
        index.rebuild_bucket(80, 0, [100, 120, 110]);
        assert_eq!(index.ambiguous_intersections(r(0, 120)), vec![r(20, 110)]);
        index.finish_partial_ack(
            &[r(0, 80)],
            [
                (
                    FlightIndexKey {
                        start: 80,
                        order: 0,
                    },
                    100,
                ),
                (
                    FlightIndexKey {
                        start: 80,
                        order: 1,
                    },
                    120,
                ),
                (
                    FlightIndexKey {
                        start: 80,
                        order: 2,
                    },
                    110,
                ),
            ],
        );
        assert_eq!(index.covered_intersections(r(0, 120)), vec![r(80, 120)]);
        assert_eq!(index.ambiguous_intersections(r(0, 120)), vec![r(80, 110)]);
        assert_eq!(index.assert_valid(), 3);

        index.rebuild_bucket(80, 3, []);
        assert!(matches!(&index.state, ProductFlightIndexState::Indexed(i) if i.len == 0));
        index.rebuild_bucket(90, 0, [100, 120, 110]);
        index.finish_partial_ack(
            &[r(80, 90)],
            [
                (
                    FlightIndexKey {
                        start: 90,
                        order: 0,
                    },
                    100,
                ),
                (
                    FlightIndexKey {
                        start: 90,
                        order: 1,
                    },
                    120,
                ),
                (
                    FlightIndexKey {
                        start: 90,
                        order: 2,
                    },
                    110,
                ),
            ],
        );
        assert_eq!(index.covered_intersections(r(0, 120)), vec![r(90, 120)]);
        assert_eq!(index.ambiguous_intersections(r(0, 120)), vec![r(90, 110)]);
        assert_eq!(index.assert_valid(), 3);

        index.rebuild_bucket(90, 3, []);
        index.finish_partial_ack(&[r(90, 120)], []);
        assert!(matches!(&index.state, ProductFlightIndexState::Empty));
    }

    #[test]
    fn persistent_insert_and_global_ack_trace_matches_byte_and_identity_oracles() {
        let mut index = ProductFlightIndex::default();
        let mut flights = BTreeMap::<FlightIndexKey, u64>::new();
        let mut next_order = BTreeMap::<u64, usize>::new();
        // Keep this low-level algebra trace in the indexed representation; the
        // separate cardinality tests exercise Empty/Single promotion and demotion.
        let seed_a = FlightIndexKey {
            start: 1_000,
            order: 0,
        };
        let seed_b = FlightIndexKey {
            start: 1_001,
            order: 0,
        };
        index.publish(seed_a, 1_002);
        index.publish(seed_b, 1_003);
        flights.insert(seed_a, 1_002);
        flights.insert(seed_b, 1_003);
        for step in 0..2_000u64 {
            let start = step.wrapping_mul(37) % 127;
            let end = start + 1 + step.wrapping_mul(19) % 31;
            let order = next_order.entry(start).or_default();
            let key = FlightIndexKey {
                start,
                order: *order,
            };
            *order += 1;
            index.publish(key, end);
            flights.insert(key, end);

            if step % 7 == 0 {
                let ack_start = step.wrapping_mul(11) % 127;
                let ack_end = ack_start + 1 + step.wrapping_mul(13) % 23;
                let ack = r(ack_start, ack_end);
                let expected_ambiguous = (0..192u64)
                    .filter(|byte| {
                        ack.start <= *byte
                            && *byte < ack.end
                            && flights
                                .iter()
                                .filter(|(key, end)| key.start <= *byte && *byte < **end)
                                .count()
                                >= 2
                    })
                    .collect::<Vec<_>>();
                let actual_ambiguous = index
                    .ambiguous_intersections(ack)
                    .into_iter()
                    .flat_map(|part| part.start..part.end)
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual_ambiguous, expected_ambiguous,
                    "pre-ACK M at step {step}"
                );

                let expected_hits = flights
                    .iter()
                    .filter_map(|(&key, &end)| {
                        (key.start < ack.end && end > ack.start).then_some(key)
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    index.intersecting(&[ack]).as_slice(),
                    expected_hits.as_slice(),
                    "query at step {step}"
                );

                for key in expected_hits {
                    let end = flights
                        .remove(&key)
                        .expect("reference identity remains present");
                    index.remove(key);
                    let mut retained = Vec::with_capacity(2);
                    if key.start < ack.start {
                        retained.push((key.start, ack.start.min(end)));
                    }
                    if end > ack.end {
                        retained.push((ack.end.max(key.start), end));
                    }
                    for (start, end) in retained.into_iter().filter(|(start, end)| start < end) {
                        let order = next_order.entry(start).or_default();
                        let fragment_key = FlightIndexKey {
                            start,
                            order: *order,
                        };
                        *order += 1;
                        index.insert_retained(fragment_key, end);
                        flights.insert(fragment_key, end);
                    }
                }
                index.acknowledge(&[ack]);

                let full = r(0, 192);
                let expected_covered = (0..192u64)
                    .filter(|byte| {
                        flights
                            .iter()
                            .any(|(key, end)| key.start <= *byte && *byte < *end)
                    })
                    .collect::<Vec<_>>();
                let actual_covered = index
                    .covered_intersections(full)
                    .into_iter()
                    .flat_map(|part| part.start..part.end)
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual_covered, expected_covered,
                    "post-ACK U at step {step}"
                );
                let expected_ambiguous = (0..192u64)
                    .filter(|byte| {
                        flights
                            .iter()
                            .filter(|(key, end)| key.start <= *byte && *byte < **end)
                            .count()
                            >= 2
                    })
                    .collect::<Vec<_>>();
                let actual_ambiguous = index
                    .ambiguous_intersections(full)
                    .into_iter()
                    .flat_map(|part| part.start..part.end)
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual_ambiguous, expected_ambiguous,
                    "post-ACK M at step {step}"
                );
            }
            assert_eq!(index.assert_valid(), flights.len());
        }
    }
}
