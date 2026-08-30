//! Exact-attachment Product requalification state.
//!
//! Native reachability is intentionally outside this model.  A stale stream
//! direction returns to Product placement only through an exact data-bearing
//! probe receipt followed by fresh uniquely owned OriginalData progress.

use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StreamRequalificationProbe {
    pub(crate) id: u64,
    pub(crate) offset: u64,
    pub(crate) payload_bytes: u32,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamPathQualification {
    #[default]
    Qualified,
    Stale {
        retry_at: Instant,
    },
    Requalifying {
        probe: StreamRequalificationProbe,
        retry_at: Instant,
    },
    Acquiring {
        started_at: Instant,
    },
}

impl StreamPathQualification {
    pub(crate) fn stale_for_original_data(self) -> bool {
        matches!(self, Self::Stale { .. } | Self::Requalifying { .. })
    }

    #[cfg(test)]
    pub(crate) fn acquiring(self) -> bool {
        matches!(self, Self::Acquiring { .. })
    }

    /// Accepts uniquely owned OriginalData evidence from the current
    /// qualification epoch. The caller separately fences pre-stale flights.
    pub(crate) fn observe_unique_original_progress(
        &mut self,
        original_assigned_at: Instant,
    ) -> bool {
        match *self {
            Self::Qualified => true,
            Self::Acquiring { started_at } if original_assigned_at >= started_at => {
                *self = Self::Qualified;
                true
            }
            _ => false,
        }
    }
}

/// One stream direction's exact-attachment qualification ledger.
///
/// At most one entry can be `Requalifying`.  This bounds speculative traffic
/// independently of the number of attached carriers.  Probe IDs are local to
/// this stream direction and are never reused after exhaustion.
#[derive(Debug)]
pub(crate) struct StreamPathRequalification<Candidate> {
    entries: Vec<(Candidate, StreamPathQualification)>,
    next_probe_id: Option<u64>,
    next_candidate_index: usize,
}

impl<Candidate> Default for StreamPathRequalification<Candidate> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            next_probe_id: Some(1),
            next_candidate_index: 0,
        }
    }
}

impl<Candidate: Copy + Eq> StreamPathRequalification<Candidate> {
    pub(crate) fn state(&self, candidate: Candidate) -> StreamPathQualification {
        self.entries
            .iter()
            .find_map(|(entry, state)| (*entry == candidate).then_some(*state))
            .unwrap_or_default()
    }

    pub(crate) fn stale_for_original_data(&self, candidate: Candidate) -> bool {
        self.state(candidate).stale_for_original_data()
    }

    pub(crate) fn mark_stale(&mut self, candidate: Candidate, now: Instant) -> bool {
        let state = self.state_mut(candidate);
        if state.stale_for_original_data() {
            return false;
        }
        *state = StreamPathQualification::Stale { retry_at: now };
        true
    }

    #[cfg(test)]
    pub(crate) fn eligible_probe_candidate(&mut self, now: Instant) -> Option<Candidate> {
        self.eligible_probe_candidates_where(now, |_| true)
            .into_iter()
            .next()
    }

    pub(crate) fn eligible_probe_candidates_where(
        &mut self,
        now: Instant,
        mut eligible: impl FnMut(Candidate) -> bool,
    ) -> Vec<Candidate> {
        self.expire_probe_at(now);
        if self
            .entries
            .iter()
            .any(|(_, state)| matches!(state, StreamPathQualification::Requalifying { .. }))
        {
            return Vec::new();
        }
        let len = self.entries.len();
        (0..len)
            .filter_map(|distance| {
                let index = (self.next_candidate_index + distance) % len;
                let (candidate, state) = self.entries[index];
                (matches!(state, StreamPathQualification::Stale { retry_at } if retry_at <= now)
                    && eligible(candidate))
                .then_some(candidate)
            })
            .collect()
    }

    pub(crate) fn start_probe(
        &mut self,
        candidate: Candidate,
        offset: u64,
        payload_bytes: usize,
        retry_after: Duration,
        now: Instant,
    ) -> Option<StreamRequalificationProbe> {
        self.expire_probe_at(now);
        if self
            .entries
            .iter()
            .any(|(_, state)| matches!(state, StreamPathQualification::Requalifying { .. }))
            || !matches!(
                self.state(candidate),
                StreamPathQualification::Stale { retry_at } if retry_at <= now
            )
        {
            return None;
        }
        let payload_bytes = u32::try_from(payload_bytes)
            .ok()
            .filter(|bytes| *bytes > 0)?;
        let id = self.next_probe_id?;
        self.next_probe_id = id.checked_add(1);
        let probe = StreamRequalificationProbe {
            id,
            offset,
            payload_bytes,
        };
        *self.state_mut(candidate) = StreamPathQualification::Requalifying {
            probe,
            retry_at: now.checked_add(retry_after)?,
        };
        if let Some(index) = self
            .entries
            .iter()
            .position(|(entry, _)| *entry == candidate)
        {
            self.next_candidate_index = (index + 1) % self.entries.len();
        }
        Some(probe)
    }

    /// Exact receipt enters acquisition; it never establishes qualification.
    pub(crate) fn acknowledge_probe(
        &mut self,
        candidate: Candidate,
        probe: StreamRequalificationProbe,
        now: Instant,
    ) -> bool {
        let state = self.state_mut(candidate);
        if !matches!(
            state,
            StreamPathQualification::Requalifying {
                probe: expected,
                ..
            } if *expected == probe
        ) {
            return false;
        }
        *state = StreamPathQualification::Acquiring { started_at: now };
        true
    }

    pub(crate) fn cancel_unpublished_probe(
        &mut self,
        candidate: Candidate,
        probe: StreamRequalificationProbe,
        now: Instant,
    ) -> bool {
        let state = self.state_mut(candidate);
        if !matches!(
            state,
            StreamPathQualification::Requalifying {
                probe: expected,
                ..
            } if *expected == probe
        ) {
            return false;
        }
        *state = StreamPathQualification::Stale { retry_at: now };
        true
    }

    /// Only unique OriginalData assigned after the probe receipt qualifies.
    pub(crate) fn observe_unique_original_progress(
        &mut self,
        candidate: Candidate,
        original_assigned_at: Instant,
    ) -> bool {
        self.state_mut(candidate)
            .observe_unique_original_progress(original_assigned_at)
    }

    pub(crate) fn retain_live(&mut self, mut live: impl FnMut(Candidate) -> bool) {
        self.entries.retain(|(candidate, _)| live(*candidate));
        self.next_candidate_index = if self.entries.is_empty() {
            0
        } else {
            self.next_candidate_index % self.entries.len()
        };
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.entries
            .iter()
            .filter_map(|(_, state)| match state {
                StreamPathQualification::Requalifying { retry_at, .. } => Some(*retry_at),
                _ => None,
            })
            .min()
    }

    pub(crate) fn stale_candidates(&self) -> impl Iterator<Item = Candidate> + '_ {
        self.entries
            .iter()
            .filter_map(|(candidate, state)| state.stale_for_original_data().then_some(*candidate))
    }

    fn expire_probe_at(&mut self, now: Instant) {
        for (_, state) in &mut self.entries {
            if let StreamPathQualification::Requalifying { retry_at, .. } = *state
                && retry_at <= now
            {
                *state = StreamPathQualification::Stale { retry_at };
            }
        }
    }

    fn state_mut(&mut self, candidate: Candidate) -> &mut StreamPathQualification {
        if let Some(index) = self
            .entries
            .iter()
            .position(|(entry, _)| *entry == candidate)
        {
            return &mut self.entries[index].1;
        }
        self.entries
            .push((candidate, StreamPathQualification::Qualified));
        let index = self.entries.len() - 1;
        &mut self.entries[index].1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_probe_then_fresh_unique_progress_is_required() {
        let now = Instant::now();
        let mut state = StreamPathRequalification::<u8>::default();
        assert!(state.mark_stale(7, now));
        let probe = state
            .start_probe(7, 4096, 1024, Duration::from_secs(1), now)
            .expect("probe");
        assert!(!state.observe_unique_original_progress(7, now));
        assert!(!state.acknowledge_probe(8, probe, now + Duration::from_millis(1)));
        assert!(!state.acknowledge_probe(
            7,
            StreamRequalificationProbe {
                id: probe.id + 1,
                ..probe
            },
            now + Duration::from_millis(1)
        ));
        let acquired_at = now + Duration::from_millis(2);
        assert!(state.acknowledge_probe(7, probe, acquired_at));
        assert!(state.state(7).acquiring());
        assert!(!state.observe_unique_original_progress(7, now));
        assert!(state.observe_unique_original_progress(7, acquired_at));
        assert_eq!(state.state(7), StreamPathQualification::Qualified);
    }

    #[test]
    fn one_pending_probe_and_retry_interval_are_bounded() {
        let now = Instant::now();
        let retry = Duration::from_secs(2);
        let mut state = StreamPathRequalification::<u8>::default();
        assert!(state.mark_stale(1, now));
        assert!(state.mark_stale(2, now));
        let first = state.start_probe(1, 0, 64, retry, now).expect("first");
        assert_eq!(state.eligible_probe_candidate(now), None);
        assert_eq!(state.eligible_probe_candidate(now + retry), Some(2));
        let second = state
            .start_probe(2, 0, 64, retry, now + retry)
            .expect("next stale candidate");
        assert_ne!(first.id, second.id);
    }
}
