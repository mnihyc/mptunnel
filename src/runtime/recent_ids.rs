//! Bounded replay and closed-object identity retention.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::Hash;

#[derive(Debug)]
pub(super) struct RecentIdCache<T>
where
    T: Copy + Eq + Hash,
{
    capacity: usize,
    order: VecDeque<T>,
    set: HashSet<T>,
}

impl<T> RecentIdCache<T>
where
    T: Copy + Eq + Hash,
{
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::new(),
            set: HashSet::new(),
        }
    }

    pub(super) fn insert(&mut self, id: T) {
        if self.set.contains(&id) {
            return;
        }
        self.order.push_back(id);
        self.set.insert(id);
        while self.order.len() > self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.set.remove(&expired);
            }
        }
    }

    pub(super) fn contains(&self, id: &T) -> bool {
        self.set.contains(id)
    }
}

/// In-memory replay state whose entries live through their last valid
/// wall-clock second.
///
/// Unlike [`RecentIdCache`], this cache never evicts a live identity to admit a
/// new one. Capacity pressure therefore fails authentication closed. The cache
/// is intentionally process-local: restarting an identity runtime establishes
/// a new replay boundary, which callers must treat as part of credential
/// rotation and graceful-restart policy.
#[derive(Debug)]
pub(super) struct ExpiringReplayCache<T>
where
    T: Clone + Eq + Hash,
{
    capacity: usize,
    entries: HashMap<T, u64>,
    expirations: BTreeMap<u64, Vec<T>>,
    observed_unix_secs: u64,
}

impl<T> ExpiringReplayCache<T>
where
    T: Clone + Eq + Hash,
{
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: HashMap::new(),
            expirations: BTreeMap::new(),
            observed_unix_secs: 0,
        }
    }

    /// Atomically checks and admits one authenticated identity.
    ///
    /// `expires_at_unix_secs` is inclusive because protocol freshness accepts
    /// timestamps exactly on the configured window boundary.
    pub(super) fn try_insert(
        &mut self,
        id: T,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) -> bool {
        self.observed_unix_secs = self.observed_unix_secs.max(now_unix_secs);
        self.prune_expired(self.observed_unix_secs);
        if expires_at_unix_secs < self.observed_unix_secs
            || self.entries.contains_key(&id)
            || self.entries.len() >= self.capacity
        {
            return false;
        }
        self.entries.insert(id.clone(), expires_at_unix_secs);
        self.expirations
            .entry(expires_at_unix_secs)
            .or_default()
            .push(id);
        true
    }

    fn prune_expired(&mut self, now_unix_secs: u64) {
        while let Some((&expires_at_unix_secs, _)) = self.expirations.first_key_value() {
            if expires_at_unix_secs >= now_unix_secs {
                break;
            }
            let (_, expired_ids) = self.expirations.pop_first().expect("first expiry exists");
            for id in expired_ids {
                if self.entries.get(&id) == Some(&expires_at_unix_secs) {
                    self.entries.remove(&id);
                }
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

pub(super) fn reliable_closed_stream_cache_capacity(max_streams: usize) -> usize {
    // Retain both live-scale and recently closed identities without imposing a
    // fixed global cap on high-concurrency deployments.
    max_streams.max(1).saturating_mul(2)
}

pub(super) fn path_join_replay_cache_capacity(max_streams: usize) -> usize {
    // Compatibility rule: retain four accepted joins per configured stream.
    // The factor is resource sizing, not a TCP or QUIC congestion parameter.
    const RETAINED_PATH_JOINS_PER_STREAM: usize = 4;

    max_streams
        .max(1)
        .saturating_mul(RETAINED_PATH_JOINS_PER_STREAM)
}

#[cfg(test)]
#[path = "tests_recent_ids.rs"]
mod tests;
