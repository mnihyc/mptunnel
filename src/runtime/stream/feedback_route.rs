//! Proof of logical feedback service changes publication, never byte authority.

use super::feedback::StreamFeedbackState;
use std::time::{Duration, Instant};

#[cfg(feature = "lab-diagnostics")]
pub(in crate::runtime) fn lab_feedback_return(
    scope: Option<(crate::protocol::SessionId, crate::protocol::StreamId)>,
    kind: &str,
    fields: std::fmt::Arguments<'_>,
) {
    if let Some((session_id, stream_id)) = scope {
        crate::lab_diagnostics::lab_diagnostic(
            "feedback_return",
            format_args!(
                "session_id={} stream_id={} kind={} {}",
                session_id.0, stream_id.0, kind, fields
            ),
        );
    }
}

#[cfg(feature = "lab-diagnostics")]
fn deadline_from_observation_us(deadline: Instant, observed_at: Instant) -> i128 {
    if deadline >= observed_at {
        deadline.duration_since(observed_at).as_micros() as i128
    } else {
        -(observed_at.duration_since(deadline).as_micros() as i128)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FeedbackCut {
    ack_generation: u64,
    max_offset: u64,
}

impl From<&StreamFeedbackState> for FeedbackCut {
    fn from(state: &StreamFeedbackState) -> Self {
        Self {
            ack_generation: state.ack_generation,
            max_offset: state.max_data_offset,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct StreamFeedbackProbe {
    pub(in crate::runtime) token: u64,
    pub(in crate::runtime) ack_generation: u64,
    pub(in crate::runtime) max_offset: u64,
}

#[derive(Debug)]
struct Attempt<I> {
    output: I,
    probe: StreamFeedbackProbe,
    deadline: Instant,
    admitted: bool,
    // A receipt for the current cut must not renew debt formed after it.
    successor_deadline: Option<Instant>,
}

impl<I> Attempt<I> {
    fn next_deadline(&self) -> Instant {
        self.successor_deadline
            .map_or(self.deadline, |next| next.min(self.deadline))
    }
}

/// One directional logical owner. Exact output identities are supplied by its
/// existing attachment model, not inferred from the returning receipt carrier.
#[derive(Debug)]
pub(in crate::runtime) struct StreamFeedbackRoute<I> {
    selected: Option<I>,
    attempts: Vec<Attempt<I>>,
    next_token: Option<u64>,
    latest: FeedbackCut,
    last_attempted: Option<FeedbackCut>,
    terminal: bool,
    #[cfg(feature = "lab-diagnostics")]
    diagnostic_scope: Option<(crate::protocol::SessionId, crate::protocol::StreamId)>,
}

impl<I> Default for StreamFeedbackRoute<I> {
    fn default() -> Self {
        Self {
            selected: None,
            attempts: Vec::new(),
            next_token: Some(0),
            latest: FeedbackCut::default(),
            last_attempted: None,
            terminal: false,
            #[cfg(feature = "lab-diagnostics")]
            diagnostic_scope: None,
        }
    }
}

impl<I: Copy + Eq + std::fmt::Debug> StreamFeedbackRoute<I> {
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) fn set_diagnostic_scope(
        &mut self,
        session_id: crate::protocol::SessionId,
        stream_id: crate::protocol::StreamId,
    ) {
        self.diagnostic_scope = Some((session_id, stream_id));
    }

    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) fn diagnostic_scope(
        &self,
    ) -> Option<(crate::protocol::SessionId, crate::protocol::StreamId)> {
        self.diagnostic_scope
    }

    /// Called before publication/receipt processing, with current membership.
    /// Native intervals are evaluated only when a deadline is first created.
    pub(in crate::runtime) fn prepare<F>(
        &mut self,
        state: &StreamFeedbackState,
        live: &[I],
        now: Instant,
        mut interval: F,
    ) where
        F: FnMut(I) -> Duration,
    {
        self.latest = state.into();
        if self.terminal || live.len() < 2 || state.ack_generation <= 1 {
            #[cfg(feature = "lab-diagnostics")]
            if self.selected.is_some() || !self.attempts.is_empty() {
                lab_feedback_return(
                    self.diagnostic_scope,
                    "ineligible",
                    format_args!(
                        "selected={:?} attempts={} live={} generation={} terminal={}",
                        self.selected,
                        self.attempts.len(),
                        live.len(),
                        state.ack_generation,
                        self.terminal,
                    ),
                );
            }
            self.selected = None;
            self.attempts.clear();
            self.last_attempted = None;
            return;
        }
        if self
            .selected
            .is_some_and(|selected| !live.contains(&selected))
        {
            #[cfg(feature = "lab-diagnostics")]
            lab_feedback_return(
                self.diagnostic_scope,
                "membership_lost",
                format_args!(
                    "selected={:?} attempts={}",
                    self.selected,
                    self.attempts.len(),
                ),
            );
            self.selected = None;
            self.attempts.clear();
            self.last_attempted = None;
        }
        self.attempts
            .retain(|attempt| live.contains(&attempt.output));
        self.expire(now);
        let mut finite_successor = true;
        for attempt in &mut self.attempts {
            let cut = FeedbackCut {
                ack_generation: attempt.probe.ack_generation,
                max_offset: attempt.probe.max_offset,
            };
            if cut != self.latest && attempt.successor_deadline.is_none() {
                attempt.successor_deadline = now.checked_add(interval(attempt.output));
                finite_successor &= attempt.successor_deadline.is_some();
                #[cfg(feature = "lab-diagnostics")]
                lab_feedback_return(
                    self.diagnostic_scope,
                    "successor_anchor",
                    format_args!(
                        "output={:?} token={} generation={} max_offset={} deadline_from_observation_us={:?} observation_age_us={}",
                        attempt.output,
                        attempt.probe.token,
                        self.latest.ack_generation,
                        self.latest.max_offset,
                        attempt
                            .successor_deadline
                            .map(|deadline| deadline_from_observation_us(deadline, now)),
                        now.elapsed().as_micros(),
                    ),
                );
            }
        }
        if !finite_successor {
            #[cfg(feature = "lab-diagnostics")]
            lab_feedback_return(
                self.diagnostic_scope,
                "nonfinite_deadline",
                format_args!("selected={:?}", self.selected),
            );
            self.selected = None;
            self.attempts.clear();
            self.last_attempted = Some(self.latest);
            return;
        }
        if !self.attempts.is_empty() || self.last_attempted == Some(self.latest) {
            return;
        }
        self.last_attempted = Some(self.latest);
        for &output in live {
            if self.selected.is_some_and(|selected| selected != output) {
                continue;
            }
            let Some(deadline) = now.checked_add(interval(output)) else {
                #[cfg(feature = "lab-diagnostics")]
                lab_feedback_return(
                    self.diagnostic_scope,
                    "nonfinite_deadline",
                    format_args!("selected={:?} output={:?}", self.selected, output),
                );
                // No finite validation interval: keep ordinary full fanout.
                self.selected = None;
                self.attempts.clear();
                return;
            };
            if !self.start_attempt(output, self.latest, deadline) {
                return;
            }
        }
    }

    fn start_attempt(&mut self, output: I, cut: FeedbackCut, deadline: Instant) -> bool {
        let Some(token) = self.next_token else {
            #[cfg(feature = "lab-diagnostics")]
            lab_feedback_return(
                self.diagnostic_scope,
                "token_exhausted",
                format_args!("selected={:?}", self.selected),
            );
            // Tokens never wrap into a still-delayed receipt's identity.
            self.selected = None;
            self.attempts.clear();
            return false;
        };
        self.next_token = token.checked_add(1);
        #[cfg(feature = "lab-diagnostics")]
        {
            let observed_at = Instant::now();
            lab_feedback_return(
                self.diagnostic_scope,
                "probe_created",
                format_args!(
                    "output={:?} token={} generation={} max_offset={} selected={:?} deadline_from_observation_us={} observation_age_us={}",
                    output,
                    token,
                    cut.ack_generation,
                    cut.max_offset,
                    self.selected,
                    deadline_from_observation_us(deadline, observed_at),
                    observed_at.elapsed().as_micros(),
                ),
            );
        }
        self.attempts.push(Attempt {
            output,
            probe: StreamFeedbackProbe {
                token,
                ack_generation: cut.ack_generation,
                max_offset: cut.max_offset,
            },
            deadline,
            admitted: false,
            successor_deadline: None,
        });
        true
    }

    fn expire(&mut self, now: Instant) {
        #[cfg(feature = "lab-diagnostics")]
        for attempt in self
            .attempts
            .iter()
            .filter(|attempt| attempt.next_deadline() <= now)
        {
            lab_feedback_return(
                self.diagnostic_scope,
                "expired",
                format_args!(
                    "output={:?} token={} admitted={} selected={:?} deadline_from_observation_us={} observation_age_us={}",
                    attempt.output,
                    attempt.probe.token,
                    attempt.admitted,
                    self.selected,
                    deadline_from_observation_us(attempt.next_deadline(), now),
                    now.elapsed().as_micros(),
                ),
            );
        }
        if self.selected.is_some()
            && self
                .attempts
                .iter()
                .any(|attempt| attempt.next_deadline() <= now)
        {
            self.selected = None;
            self.attempts.clear();
            self.last_attempted = None;
        } else {
            self.attempts
                .retain(|attempt| attempt.next_deadline() > now);
        }
    }

    pub(in crate::runtime) fn probe_for(&self, output: I) -> Option<StreamFeedbackProbe> {
        self.attempts
            .iter()
            .find(|attempt| attempt.output == output && !attempt.admitted)
            .map(|attempt| attempt.probe)
    }

    pub(in crate::runtime) fn record_probe_admission(&mut self, output: I, token: u64) {
        if let Some(attempt) = self
            .attempts
            .iter_mut()
            .find(|attempt| attempt.output == output && attempt.probe.token == token)
        {
            #[cfg(feature = "lab-diagnostics")]
            if !attempt.admitted {
                lab_feedback_return(
                    self.diagnostic_scope,
                    "probe_admitted",
                    format_args!(
                        "output={:?} token={} generation={} max_offset={}",
                        output, token, attempt.probe.ack_generation, attempt.probe.max_offset,
                    ),
                );
            }
            attempt.admitted = true;
        }
    }

    /// Prepare current membership first. The receipt's ingress is deliberately
    /// absent: the token identifies the still-owned *probed* exact output.
    pub(in crate::runtime) fn receive_receipt(&mut self, token: u64, now: Instant) -> bool {
        self.expire(now);
        let Some(index) = self
            .attempts
            .iter()
            .position(|attempt| attempt.probe.token == token && attempt.admitted)
        else {
            #[cfg(feature = "lab-diagnostics")]
            lab_feedback_return(
                self.diagnostic_scope,
                "ignored_receipt",
                format_args!("token={} selected={:?}", token, self.selected),
            );
            return false;
        };
        let attempt = self.attempts.remove(index);
        self.attempts.clear();
        if attempt
            .successor_deadline
            .is_some_and(|deadline| deadline <= now)
        {
            #[cfg(feature = "lab-diagnostics")]
            lab_feedback_return(
                self.diagnostic_scope,
                "ignored_receipt",
                format_args!("token={} reason=successor_expired", token),
            );
            self.selected = None;
            self.last_attempted = None;
            return false;
        }
        #[cfg(feature = "lab-diagnostics")]
        lab_feedback_return(
            self.diagnostic_scope,
            "selected",
            format_args!(
                "output={:?} token={} previous={:?} generation={} max_offset={} successor_from_observation_us={:?} observation_age_us={}",
                attempt.output,
                token,
                self.selected,
                attempt.probe.ack_generation,
                attempt.probe.max_offset,
                attempt
                    .successor_deadline
                    .map(|deadline| deadline_from_observation_us(deadline, now)),
                now.elapsed().as_micros(),
            ),
        );
        self.selected = Some(attempt.output);
        if let Some(deadline) = attempt.successor_deadline {
            self.last_attempted = Some(self.latest);
            self.start_attempt(attempt.output, self.latest, deadline);
        }
        true
    }

    /// A new attachment still receives its initial complete baseline even
    /// while another output owns ordinary subsequent feedback.
    pub(in crate::runtime) fn requires_output(&self, output: I, baseline_ready: bool) -> bool {
        !baseline_ready || self.selected.is_none_or(|selected| selected == output)
    }

    pub(in crate::runtime) fn next_deadline(&self) -> Option<Instant> {
        self.attempts.iter().map(Attempt::next_deadline).min()
    }

    pub(in crate::runtime) fn forget_output(&mut self, output: I) {
        #[cfg(feature = "lab-diagnostics")]
        lab_feedback_return(
            self.diagnostic_scope,
            "detach",
            format_args!("output={:?} selected={:?}", output, self.selected),
        );
        if self.selected == Some(output) {
            self.selected = None;
            self.attempts.clear();
            self.last_attempted = None;
        } else {
            self.attempts.retain(|attempt| attempt.output != output);
        }
    }

    pub(in crate::runtime) fn finish(&mut self) {
        #[cfg(feature = "lab-diagnostics")]
        if !self.terminal {
            lab_feedback_return(
                self.diagnostic_scope,
                "terminal",
                format_args!(
                    "selected={:?} attempts={}",
                    self.selected,
                    self.attempts.len()
                ),
            );
        }
        self.terminal = true;
        self.selected = None;
        self.attempts.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(generation: u64, max_offset: u64) -> StreamFeedbackState {
        StreamFeedbackState {
            ack_generation: generation,
            max_data_offset: max_offset,
            ..Default::default()
        }
    }

    #[test]
    fn discovery_requires_actual_admission_and_selects_token_owner_not_reply_ingress() {
        let now = Instant::now();
        let mut route = StreamFeedbackRoute::default();
        let state = state(2, 100);
        route.prepare(&state, &[10, 20], now, |_| Duration::from_millis(100));
        let a = route.probe_for(10).unwrap();
        let b = route.probe_for(20).unwrap();
        assert_ne!(a.token, b.token);
        assert!(route.requires_output(10, true) && route.requires_output(20, true));
        assert!(!route.receive_receipt(b.token, now));
        route.record_probe_admission(20, b.token);
        assert!(
            route.probe_for(20).is_none(),
            "an admitted marker is not queued again"
        );
        assert!(route.receive_receipt(b.token, now + Duration::from_millis(50)));
        assert!(!route.requires_output(10, true));
        assert!(route.requires_output(20, true));
        assert!(
            route.requires_output(10, false),
            "new exact outputs still get a baseline"
        );
        assert!(!route.receive_receipt(a.token, now + Duration::from_millis(51)));
        route.prepare(&state, &[10, 20], now + Duration::from_secs(100), |_| {
            panic!("idle proof loop")
        });
        assert!(route.next_deadline().is_none());
    }

    #[test]
    fn older_receipt_never_renews_successor_debt_or_changed_native_interval() {
        let now = Instant::now();
        let mut route = StreamFeedbackRoute::default();
        route.prepare(&state(2, 100), &[10, 20], now, |_| {
            Duration::from_millis(100)
        });
        let first = route.probe_for(10).unwrap();
        route.record_probe_admission(10, first.token);
        assert!(route.receive_receipt(first.token, now + Duration::from_millis(5)));

        route.prepare(
            &state(3, 110),
            &[10, 20],
            now + Duration::from_millis(10),
            |_| Duration::from_millis(100),
        );
        let current = route.probe_for(10).unwrap();
        route.record_probe_admission(10, current.token);
        route.prepare(
            &state(4, 120),
            &[10, 20],
            now + Duration::from_millis(20),
            |_| Duration::from_millis(100),
        );
        route.prepare(
            &state(5, 130),
            &[10, 20],
            now + Duration::from_millis(30),
            |_| panic!("existing successor deadline renewed"),
        );
        assert_eq!(
            route.next_deadline(),
            Some(now + Duration::from_millis(110))
        );
        assert!(route.receive_receipt(current.token, now + Duration::from_millis(100)));
        let successor = route.probe_for(10).unwrap();
        assert_eq!(successor.ack_generation, 5);
        assert_eq!(successor.max_offset, 130);
        assert_eq!(
            route.next_deadline(),
            Some(now + Duration::from_millis(120))
        );
        route.record_probe_admission(10, successor.token);
        assert!(!route.receive_receipt(successor.token, now + Duration::from_millis(120)));
        assert!(route.requires_output(10, true) && route.requires_output(20, true));
    }

    #[test]
    fn shrinking_native_interval_cannot_hide_an_earlier_successor_deadline() {
        let now = Instant::now();
        let mut route = StreamFeedbackRoute::default();
        route.prepare(&state(2, 100), &[10, 20], now, |_| Duration::from_secs(1));
        let token = route.probe_for(10).unwrap().token;
        route.record_probe_admission(10, token);
        assert!(route.receive_receipt(token, now));
        route.prepare(
            &state(3, 110),
            &[10, 20],
            now + Duration::from_millis(10),
            |_| Duration::from_secs(1),
        );
        let old = route.probe_for(10).unwrap().token;
        route.record_probe_admission(10, old);
        route.prepare(
            &state(4, 120),
            &[10, 20],
            now + Duration::from_millis(20),
            |_| Duration::from_millis(100),
        );
        assert_eq!(
            route.next_deadline(),
            Some(now + Duration::from_millis(120))
        );
        route.prepare(
            &state(4, 120),
            &[10, 20],
            now + Duration::from_millis(120),
            |_| Duration::from_secs(1),
        );
        assert!(route.requires_output(10, true) && route.requires_output(20, true));
        assert!(!route.receive_receipt(old, now + Duration::from_millis(120)));
    }

    #[test]
    fn blocked_probe_expires_and_future_feedback_keeps_full_fanout() {
        let now = Instant::now();
        let mut route = StreamFeedbackRoute::default();
        route.prepare(&state(2, 100), &[10, 20], now, |_| {
            Duration::from_millis(100)
        });
        let token = route.probe_for(10).unwrap().token;
        route.record_probe_admission(10, token);
        assert!(route.receive_receipt(token, now));
        route.prepare(&state(3, 110), &[10, 20], now, |_| {
            Duration::from_millis(100)
        });
        let never_queued = route.probe_for(10).unwrap().token;
        route.prepare(
            &state(4, 120),
            &[10, 20],
            now + Duration::from_millis(101),
            |_| Duration::from_millis(100),
        );
        assert!(route.requires_output(10, true) && route.requires_output(20, true));
        assert!(!route.receive_receipt(never_queued, now + Duration::from_millis(101)));
        route.prepare(
            &state(5, 130),
            &[10, 20],
            now + Duration::from_millis(150),
            |_| Duration::from_millis(100),
        );
        assert!(route.requires_output(10, true) && route.requires_output(20, true));
    }

    #[test]
    fn exact_replacement_and_terminal_drop_proof_authority() {
        let now = Instant::now();
        let mut route = StreamFeedbackRoute::default();
        route.prepare(&state(2, 100), &[(1, 1), (2, 1)], now, |_| {
            Duration::from_secs(1)
        });
        let old = route.probe_for((1, 1)).unwrap().token;
        route.record_probe_admission((1, 1), old);
        route.forget_output((1, 1));
        route.prepare(&state(3, 110), &[(1, 2), (2, 1)], now, |_| {
            Duration::from_secs(1)
        });
        assert!(!route.receive_receipt(old, now));
        route.finish();
        route.prepare(&state(4, 120), &[(1, 2), (2, 1)], now, |_| {
            panic!("terminal probe")
        });
        assert!(route.next_deadline().is_none());
        assert!(route.requires_output((1, 2), true));
        assert!(route.requires_output((2, 1), true));
    }

    #[test]
    fn first_generation_single_output_and_token_exhaustion_preserve_baseline() {
        let now = Instant::now();
        let mut route = StreamFeedbackRoute::default();
        route.prepare(&state(1, 100), &[10, 20], now, |_| panic!("startup probe"));
        route.prepare(&state(2, 100), &[10], now, |_| {
            panic!("single-output probe")
        });
        route.next_token = Some(u64::MAX);
        route.prepare(&state(3, 100), &[10, 20], now, |_| Duration::from_secs(1));
        assert!(route.next_token.is_none());
        assert!(route.next_deadline().is_none());
        assert!(route.requires_output(10, true) && route.requires_output(20, true));
    }
}
