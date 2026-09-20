//! Initial-only submitted-open deadline custody; selection stays in relay/open.
//!
//! One logical owner separates the original lifetime from an alternative-start
//! decision and arbitrates actual first MAX with the next attempt's first poll.
//! Native users retain weak, exact capabilities.

#[cfg(test)]
#[path = "tests_initial_open.rs"]
mod tests;

use crate::model::path::{CarrierPathInstanceId, RelayPathKey};
use crate::protocol::{SessionId, StreamId};
use crate::runtime::error::RuntimeError;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Setup,
    Submitted,
    DecisionDue,
    Admitted,
    Expired,
    Settled,
}

struct Slot {
    key: RelayPathKey,
    // Immutable setup/unretained S, also inherited by later backend generations.
    nominal: Instant,
    // Monotone alternative-start D; Setup and exhausted alternatives mask it.
    decision: Instant,
    budget: Duration,
    admission_budget: Duration,
    may_start_successor: bool,
    generation: u64,
    carrier: Option<CarrierPathInstanceId>,
    phase: Phase,
    terminal_deadline: Option<Instant>,
    retained: bool,
}

#[derive(Clone, Copy)]
struct PhaseObservation {
    ordinal: u8,
    key: RelayPathKey,
    generation: u64,
    carrier: Option<CarrierPathInstanceId>,
    phase: &'static str,
    retained: bool,
    nominal: Instant,
    observed_at: Instant,
}

impl Slot {
    fn expire_at(&mut self, at: Instant) {
        self.phase = Phase::Expired;
        self.terminal_deadline = Some(
            self.terminal_deadline
                .map_or(at, |previous| previous.min(at)),
        );
    }

    fn observation(&self, ordinal: u8, phase: &'static str) -> PhaseObservation {
        #[cfg(feature = "lab-diagnostics")]
        let observed_at = Instant::now();
        #[cfg(not(feature = "lab-diagnostics"))]
        let observed_at = self.nominal;
        PhaseObservation {
            ordinal,
            key: self.key,
            generation: self.generation,
            carrier: self.carrier,
            phase,
            retained: self.retained,
            nominal: self.nominal,
            observed_at,
        }
    }
}

struct State {
    closed: bool,
    admission_epoch: u64,
    slots: Vec<Option<Slot>>,
}

struct Inner {
    session_id: SessionId,
    stream_id: StreamId,
    deadline: Instant,
    state: Mutex<State>,
    changed: Notify,
}

/// This value alone owns acquisition lifetime. It holds no native resources.
pub(in crate::runtime) struct InitialOpenAcquisition(Arc<Inner>);

#[derive(Clone)]
pub(in crate::runtime) struct InitialOpenAttempt {
    inner: Weak<Inner>,
    ordinal: u8,
}

/// A retry gets a new generation even on the same physical carrier.
#[derive(Clone)]
pub(in crate::runtime) struct InitialOpenBackend {
    attempt: InitialOpenAttempt,
    generation: u64,
}

impl Inner {
    fn event(&self, observation: PhaseObservation) {
        #[cfg(feature = "lab-diagnostics")]
        crate::lab_diagnostics::lab_diagnostic(
            "initial_acquisition",
            format_args!(
                "session_id={} stream_id={} ordinal={} underlay={:?} path_index={} generation={} carrier={:?} phase={} retained={} nominal_remaining_us={} logical_remaining_us={} observation_age_us={}",
                self.session_id.0,
                self.stream_id.0,
                observation.ordinal,
                observation.key.underlay,
                observation.key.index,
                observation.generation,
                observation.carrier,
                observation.phase,
                observation.retained,
                observation
                    .nominal
                    .saturating_duration_since(observation.observed_at)
                    .as_micros(),
                self.deadline
                    .saturating_duration_since(observation.observed_at)
                    .as_micros(),
                Instant::now()
                    .saturating_duration_since(observation.observed_at)
                    .as_micros(),
            ),
        );
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = (
            self.session_id,
            self.stream_id,
            observation.ordinal,
            observation.key,
            observation.generation,
            observation.carrier,
            observation.phase,
            observation.retained,
            observation.nominal,
            observation.observed_at,
        );
    }

    fn update_deadline(
        &self,
        ordinal: u8,
        slot: &mut Slot,
        now: Instant,
        observation: &mut Option<PhaseObservation>,
    ) -> Instant {
        if now >= self.deadline {
            slot.expire_at(self.deadline);
        } else if slot.phase == Phase::Setup && now >= slot.nominal {
            // Early retention can precede another backend generation. Its
            // new pair allocation/partial writes still own the original S.
            slot.expire_at(slot.nominal);
            *observation = Some(slot.observation(ordinal, "expired"));
        } else if slot.phase == Phase::Submitted && !slot.retained {
            if slot.may_start_successor && now >= slot.decision {
                slot.phase = Phase::DecisionDue;
                *observation = Some(slot.observation(ordinal, "decision_due"));
            } else if now >= slot.nominal {
                slot.expire_at(slot.nominal);
                *observation = Some(slot.observation(ordinal, "expired"));
            }
        }
        match slot.phase {
            Phase::Expired | Phase::Settled => {
                slot.terminal_deadline.expect("terminal initial deadline")
            }
            Phase::DecisionDue => self.deadline,
            Phase::Submitted | Phase::Admitted if slot.retained => self.deadline,
            Phase::Submitted if slot.may_start_successor => slot.decision,
            _ => slot.nominal,
        }
    }
}

impl InitialOpenAcquisition {
    pub(in crate::runtime) fn new(
        session_id: SessionId,
        stream_id: StreamId,
        deadline: Instant,
        candidate_total: usize,
    ) -> Self {
        Self(Arc::new(Inner {
            session_id,
            stream_id,
            deadline,
            state: Mutex::new(State {
                closed: false,
                admission_epoch: 0,
                slots: (0..candidate_total).map(|_| None).collect(),
            }),
            changed: Notify::new(),
        }))
    }

    pub(in crate::runtime) fn launch_handle(&self) -> InitialOpenLaunch {
        let state = self.0.state.lock().expect("initial open lock");
        InitialOpenLaunch {
            inner: Arc::downgrade(&self.0),
            admission_epoch: state.admission_epoch,
            admitted_at_decision: state
                .slots
                .iter()
                .flatten()
                .any(|slot| slot.phase == Phase::Admitted),
        }
    }

    pub(in crate::runtime) fn changed(&self) -> tokio::sync::futures::Notified<'_> {
        self.0.changed.notified()
    }

    pub(in crate::runtime) fn decision_deadline(&self, ordinal: u8) -> Option<Instant> {
        self.0
            .state
            .lock()
            .expect("initial open lock")
            .slots
            .get(usize::from(ordinal))
            .and_then(Option::as_ref)
            .and_then(|slot| match slot.phase {
                Phase::Setup => Some(slot.nominal),
                Phase::Submitted | Phase::DecisionDue if !slot.retained => {
                    Some(if slot.may_start_successor {
                        slot.decision
                    } else {
                        slot.nominal
                    })
                }
                Phase::Expired => slot.terminal_deadline,
                _ => None,
            })
    }

    pub(in crate::runtime) fn has_admission(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("initial open lock")
            .slots
            .iter()
            .flatten()
            .any(|slot| slot.phase == Phase::Admitted)
    }

    pub(in crate::runtime) fn expire_unsubmitted(&self, ordinal: u8) -> bool {
        let mut state = self.0.state.lock().expect("initial open lock");
        let now = Instant::now();
        let observation = state
            .slots
            .get_mut(usize::from(ordinal))
            .and_then(Option::as_mut)
            .filter(|slot| {
                matches!(slot.phase, Phase::Setup | Phase::Expired) && now >= slot.nominal
            })
            .map(|slot| {
                slot.expire_at(slot.nominal);
                slot.observation(ordinal, "setup_expired")
            });
        drop(state);
        let expired = observation.is_some();
        if let Some(observation) = observation {
            self.0.event(observation);
        }
        if expired {
            self.0.changed.notify_waiters();
        }
        expired
    }

    /// Finite reservation traversal found no actual successor. Consume that
    /// opportunity without shortening the original's unretained lifetime S.
    /// Return true only when S already expired and cancellation is warranted.
    pub(in crate::runtime) fn exhaust_successors(&self, ordinal: u8) -> bool {
        let mut state = self.0.state.lock().expect("initial open lock");
        let mut observation = None;
        let expired = if let Some(slot) = state
            .slots
            .get_mut(usize::from(ordinal))
            .and_then(Option::as_mut)
            && !matches!(slot.phase, Phase::Settled | Phase::Admitted)
        {
            slot.may_start_successor = false;
            if slot.phase == Phase::DecisionDue {
                slot.phase = Phase::Submitted;
            }
            observation = Some(slot.observation(ordinal, "alternatives_exhausted"));
            self.0
                .update_deadline(ordinal, slot, Instant::now(), &mut observation);
            slot.phase == Phase::Expired
        } else {
            false
        };
        drop(state);
        if let Some(observation) = observation {
            self.0.event(observation);
        }
        self.0.changed.notify_waiters();
        expired
    }

    pub(in crate::runtime) fn settle(&self, ordinal: u8) {
        let mut state = self.0.state.lock().expect("initial open lock");
        let observation = if let Some(slot) = state
            .slots
            .get_mut(usize::from(ordinal))
            .and_then(Option::as_mut)
        {
            slot.phase = Phase::Settled;
            let now = Instant::now();
            slot.terminal_deadline = Some(
                slot.terminal_deadline
                    .map_or(now, |previous| previous.min(now)),
            );
            Some(slot.observation(ordinal, "settled"))
        } else {
            None
        };
        drop(state);
        if let Some(observation) = observation {
            self.0.event(observation);
        }
        self.0.changed.notify_waiters();
    }

    /// A reserved successor can fail before its first-poll launch transition.
    /// Such a preparation must not orphan the predecessor's Due decision.
    pub(in crate::runtime) fn unstarted_predecessor(
        &self,
        ordinal: u8,
        predecessor: Option<u8>,
    ) -> Option<u8> {
        let state = self.0.state.lock().expect("initial open lock");
        if state
            .slots
            .get(usize::from(ordinal))
            .is_some_and(Option::is_some)
        {
            return None;
        }
        predecessor.filter(|previous| {
            state
                .slots
                .get(usize::from(*previous))
                .and_then(Option::as_ref)
                .is_some_and(|slot| slot.phase != Phase::Settled)
        })
    }

    pub(in crate::runtime) fn started_ordinals(&self) -> Vec<u8> {
        self.0
            .state
            .lock()
            .expect("initial open lock")
            .slots
            .iter()
            .enumerate()
            .filter_map(|(ordinal, slot)| slot.as_ref().map(|_| ordinal as u8))
            .collect()
    }

    pub(in crate::runtime) fn winner(&self, ordinal: u8) {
        let state = self.0.state.lock().expect("initial open lock");
        let observation = state
            .slots
            .get(usize::from(ordinal))
            .and_then(Option::as_ref)
            .map(|slot| slot.observation(ordinal, "winner"));
        drop(state);
        if let Some(observation) = observation {
            self.0.event(observation);
        }
    }
}

impl Drop for InitialOpenAcquisition {
    fn drop(&mut self) {
        self.0.state.lock().expect("initial open lock").closed = true;
        self.0.changed.notify_waiters();
    }
}

/// Prepared launch authority, used only at the successor future's first poll.
#[derive(Clone)]
pub(in crate::runtime) struct InitialOpenLaunch {
    inner: Weak<Inner>,
    admission_epoch: u64,
    admitted_at_decision: bool,
}

impl InitialOpenLaunch {
    pub(in crate::runtime) fn begin(
        &self,
        ordinal: u8,
        key: RelayPathKey,
        budget: Duration,
        admission_budget: Duration,
        may_start_successor: bool,
        predecessor: Option<u8>,
    ) -> Result<Option<InitialOpenAttempt>, RuntimeError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(RuntimeError::ReliablePathRetired)?;
        let mut state = inner.state.lock().expect("initial open lock");
        let now = Instant::now();
        if state.closed || now >= inner.deadline {
            return Err(RuntimeError::PathOpenTimedOut);
        }
        let live_admission = state
            .slots
            .iter()
            .flatten()
            .any(|slot| slot.phase == Phase::Admitted);
        let intervening_admission = predecessor.is_some()
            && (self.admitted_at_decision || state.admission_epoch != self.admission_epoch);
        if live_admission || intervening_admission {
            // Any actual first MAX since this nominal decision fences its
            // prepared successor, including a retained publisher that settled
            // before this future's first poll. A new decision can retry.
            #[cfg(feature = "lab-diagnostics")]
            let observation = (state.admission_epoch, Instant::now());
            drop(state);
            #[cfg(feature = "lab-diagnostics")]
            crate::lab_diagnostics::lab_diagnostic(
                "initial_acquisition_fenced",
                format_args!(
                    "session_id={} stream_id={} ordinal={} underlay={:?} path_index={} predecessor={:?} decision_admission_epoch={} current_admission_epoch={} live_admission={} observation_age_us={}",
                    inner.session_id.0,
                    inner.stream_id.0,
                    ordinal,
                    key.underlay,
                    key.index,
                    predecessor,
                    self.admission_epoch,
                    observation.0,
                    live_admission,
                    Instant::now()
                        .saturating_duration_since(observation.1)
                        .as_micros(),
                ),
            );
            return Ok(None);
        }
        if state
            .slots
            .get(usize::from(ordinal))
            .is_none_or(Option::is_some)
        {
            return Err(RuntimeError::Protocol("initial ordinal launched twice"));
        }
        let mut previous_observation = None;
        if let Some(previous) = predecessor
            && let Some(slot) = state
                .slots
                .get_mut(usize::from(previous))
                .and_then(Option::as_mut)
        {
            inner.update_deadline(previous, slot, now, &mut previous_observation);
            if slot.phase == Phase::DecisionDue {
                slot.retained = true;
                slot.phase = Phase::Submitted;
                previous_observation = Some(slot.observation(previous, "retained"));
            } else if matches!(slot.phase, Phase::Setup | Phase::Submitted) {
                // A new backend can re-enter Setup before S while a nominal
                // successor is only prepared. Revalidate full submission at
                // actual entry; neither its reservation nor old D promotes it.
                drop(state);
                if let Some(observation) = previous_observation {
                    inner.event(observation);
                    inner.changed.notify_waiters();
                }
                return Ok(None);
            }
        }
        let nominal = (now + budget).min(inner.deadline);
        let slot = Slot {
            key,
            nominal,
            decision: nominal,
            budget,
            admission_budget,
            may_start_successor,
            generation: 0,
            carrier: None,
            phase: Phase::Setup,
            terminal_deadline: None,
            retained: false,
        };
        let observation = slot.observation(ordinal, "launched");
        state.slots[usize::from(ordinal)] = Some(slot);
        drop(state);
        if let Some(previous) = previous_observation {
            inner.event(previous);
        }
        inner.event(observation);
        inner.changed.notify_waiters();
        Ok(Some(InitialOpenAttempt {
            inner: Arc::downgrade(&inner),
            ordinal,
        }))
    }
}

impl InitialOpenAttempt {
    fn deadline(&self) -> Result<Instant, RuntimeError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(RuntimeError::ReliablePathRetired)?;
        let mut state = inner.state.lock().expect("initial open lock");
        if state.closed {
            return Err(RuntimeError::ReliablePathRetired);
        }
        let slot = state
            .slots
            .get_mut(usize::from(self.ordinal))
            .and_then(Option::as_mut)
            .ok_or(RuntimeError::ReliablePathRetired)?;
        let mut observation = None;
        let deadline = inner.update_deadline(self.ordinal, slot, Instant::now(), &mut observation);
        drop(state);
        if let Some(observation) = observation {
            inner.event(observation);
            inner.changed.notify_waiters();
        }
        Ok(deadline)
    }

    async fn expired(&self) -> RuntimeError {
        loop {
            let Some(inner) = self.inner.upgrade() else {
                return RuntimeError::ReliablePathRetired;
            };
            let changed = inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let deadline = match self.deadline() {
                Ok(deadline) => deadline,
                Err(error) => return error,
            };
            if deadline <= Instant::now() {
                return RuntimeError::PathOpenTimedOut;
            }
            tokio::select! {
                biased;
                _ = &mut changed => {},
                _ = tokio::time::sleep_until(deadline) => {},
            }
        }
    }

    /// Covers raw acquisition and physical settlement, ending before the
    /// existing positive-target-credit wait. The caller owns/drop-scopes it.
    pub(in crate::runtime) async fn complete<T>(
        &self,
        mut operation: Pin<&mut impl Future<Output = Result<T, RuntimeError>>>,
    ) -> Result<T, RuntimeError> {
        tokio::select! {
            biased;
            result = &mut operation => result,
            error = self.expired() => Err(error),
        }
    }

    pub(in crate::runtime) fn validate(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
    ) -> Result<(), RuntimeError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(RuntimeError::ReliablePathRetired)?;
        if inner.session_id != session_id || inner.stream_id != stream_id {
            return Err(RuntimeError::Protocol(
                "initial acquisition identity mismatch",
            ));
        }
        Ok(())
    }

    pub(in crate::runtime) fn timing(&self) -> Result<(Instant, Duration), RuntimeError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(RuntimeError::ReliablePathRetired)?;
        let state = inner.state.lock().expect("initial open lock");
        if state.closed {
            return Err(RuntimeError::ReliablePathRetired);
        }
        state
            .slots
            .get(usize::from(self.ordinal))
            .and_then(Option::as_ref)
            .map(|slot| (slot.nominal, slot.budget))
            .ok_or(RuntimeError::ReliablePathRetired)
    }

    pub(in crate::runtime) fn begin_backend(&self) -> Result<InitialOpenBackend, RuntimeError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(RuntimeError::ReliablePathRetired)?;
        let mut state = inner.state.lock().expect("initial open lock");
        if state.closed {
            return Err(RuntimeError::ReliablePathRetired);
        }
        let slot = state
            .slots
            .get_mut(usize::from(self.ordinal))
            .and_then(Option::as_mut)
            .ok_or(RuntimeError::ReliablePathRetired)?;
        if Instant::now() >= slot.nominal || matches!(slot.phase, Phase::Expired | Phase::Settled) {
            return Err(RuntimeError::PathOpenTimedOut);
        }
        slot.generation = slot
            .generation
            .checked_add(1)
            .ok_or(RuntimeError::Protocol(
                "initial backend generation exhausted",
            ))?;
        let deadline_changed = slot.phase != Phase::Setup;
        slot.carrier = None;
        slot.phase = Phase::Setup;
        slot.terminal_deadline = None;
        let generation = slot.generation;
        drop(state);
        if deadline_changed {
            inner.changed.notify_waiters();
        }
        Ok(InitialOpenBackend {
            attempt: self.clone(),
            generation,
        })
    }
}

impl InitialOpenBackend {
    fn with_slot<T>(
        &self,
        update: impl FnOnce(
            &Inner,
            &mut Slot,
            &mut u64,
            &mut Option<PhaseObservation>,
        ) -> Result<T, RuntimeError>,
    ) -> Result<T, RuntimeError> {
        let inner = self
            .attempt
            .inner
            .upgrade()
            .ok_or(RuntimeError::ReliablePathRetired)?;
        let mut state = inner.state.lock().expect("initial open lock");
        if state.closed {
            return Err(RuntimeError::ReliablePathRetired);
        }
        let State {
            slots,
            admission_epoch,
            ..
        } = &mut *state;
        let slot = slots
            .get_mut(usize::from(self.attempt.ordinal))
            .and_then(Option::as_mut)
            .filter(|slot| slot.generation == self.generation)
            .ok_or(RuntimeError::ReliablePathRetired)?;
        let mut observation = None;
        let result = update(&inner, slot, admission_epoch, &mut observation);
        drop(state);
        if let Some(observation) = observation {
            inner.event(observation);
            inner.changed.notify_waiters();
        }
        result
    }

    pub(in crate::runtime) fn bind(
        &self,
        carrier: CarrierPathInstanceId,
    ) -> Result<(), RuntimeError> {
        self.with_slot(|_, slot, _, _| {
            if slot.carrier.is_some_and(|bound| bound != carrier) {
                return Err(RuntimeError::ReliablePathRetired);
            }
            if Instant::now() >= slot.nominal || slot.phase != Phase::Setup {
                return Err(RuntimeError::PathOpenTimedOut);
            }
            slot.carrier = Some(carrier);
            Ok(())
        })
    }

    pub(in crate::runtime) fn submitted(&self) -> Result<(), RuntimeError> {
        self.with_slot(|_inner, slot, _, observation| {
            let now = Instant::now();
            if now >= slot.nominal || slot.phase != Phase::Setup || slot.carrier.is_none() {
                return Err(RuntimeError::PathOpenTimedOut);
            }
            // Transport/path setup and both local OPEN/MAX writes are complete.
            // Price only the remaining admission exchange when another frozen
            // candidate can compete. D contracts, while the original setup and
            // unretained lifetime S remains fixed. A later backend cannot
            // restart the stored decision, and Setup masks it until submission.
            if slot.may_start_successor {
                slot.decision = now
                    .checked_add(slot.admission_budget)
                    .map_or(slot.decision, |deadline| slot.decision.min(deadline));
            }
            slot.phase = Phase::Submitted;
            *observation = Some(slot.observation(self.attempt.ordinal, "submitted"));
            Ok(())
        })
    }

    pub(in crate::runtime) fn admission(&self) -> Result<Instant, RuntimeError> {
        self.with_slot(|inner, slot, admission_epoch, observation| {
            let now = Instant::now();
            if matches!(slot.phase, Phase::Expired | Phase::Settled) || now >= inner.deadline {
                return Err(RuntimeError::PathOpenTimedOut);
            }
            if !matches!(slot.phase, Phase::Submitted | Phase::DecisionDue) {
                return Err(RuntimeError::Protocol(
                    "first MAX before submitted initial open",
                ));
            }
            *admission_epoch = admission_epoch
                .checked_add(1)
                .ok_or(RuntimeError::Protocol(
                    "initial admission observation exhausted",
                ))?;
            slot.phase = Phase::Admitted;
            *observation = Some(slot.observation(self.attempt.ordinal, "first_max"));
            // During DecisionDue native input remains real, but coordinator
            // delay alone cannot grant an unpromoted attempt extra success time.
            if !slot.retained && now > slot.nominal {
                return Err(RuntimeError::PathOpenTimedOut);
            }
            Ok(if slot.retained {
                inner.deadline
            } else {
                slot.nominal
            })
        })
    }

    pub(in crate::runtime) fn deadline(&self) -> Result<Instant, RuntimeError> {
        self.with_slot(|inner, slot, _, observation| {
            Ok(inner.update_deadline(self.attempt.ordinal, slot, Instant::now(), observation))
        })
    }

    pub(in crate::runtime) async fn expired(&self) -> RuntimeError {
        loop {
            let Some(inner) = self.attempt.inner.upgrade() else {
                return RuntimeError::ReliablePathRetired;
            };
            let changed = inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let deadline = match self.deadline() {
                Ok(deadline) => deadline,
                Err(error) => return error,
            };
            if deadline <= Instant::now() {
                return RuntimeError::PathOpenTimedOut;
            }
            tokio::select! {
                biased;
                _ = &mut changed => {},
                _ = tokio::time::sleep_until(deadline) => {},
            }
        }
    }

    /// The caller keeps/drop-scopes the native operation; no owned nesting.
    pub(in crate::runtime) async fn complete<T>(
        &self,
        mut operation: Pin<&mut impl Future<Output = Result<T, RuntimeError>>>,
    ) -> Result<T, RuntimeError> {
        tokio::select! {
            biased;
            result = &mut operation => result,
            error = self.expired() => Err(error),
        }
    }
}
