//! Receive-driven liveness for one authenticated TCP carrier.
//!
//! The actor, authenticated reader, and outer lifecycle guard share one owner.
//! The actor serializes PING through the ordinary writer; this owner only
//! decides when it is due and when the exact carrier has missed its budget.

use crate::protocol::Frame;
use crate::runtime::error::RuntimeError;
use crate::runtime::path::peer_round_trip::{PeerRoundTrip, PeerRoundTripSource};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::runtime::path::tcp) enum TcpCarrierHeartbeatFailure {
    SendProgressTimeout,
    ReplyTimeout,
    ProtocolViolation,
    RandomSourceFailure,
}

impl TcpCarrierHeartbeatFailure {
    pub(in crate::runtime::path::tcp) const fn reason(self) -> &'static str {
        match self {
            Self::SendProgressTimeout => "heartbeat_send_progress_timeout",
            Self::ReplyTimeout => "heartbeat_reply_timeout",
            Self::ProtocolViolation => "heartbeat_protocol_error",
            Self::RandomSourceFailure => "heartbeat_random_source_error",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::runtime::path::tcp) enum TcpCarrierHeartbeatFrameDisposition {
    Forward,
    Consume,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::runtime::path::tcp) struct TcpCarrierHeartbeatPing {
    pub nonce: u64,
    pub due_at: tokio::time::Instant,
    pub deadline: tokio::time::Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::runtime::path::tcp) enum TcpCarrierHeartbeatClaim {
    NotDue,
    Draining,
    Claimed(TcpCarrierHeartbeatPing),
    Failed(TcpCarrierHeartbeatFailure),
}

#[derive(Clone, Copy)]
enum SendPhase {
    Sending,
    AwaitingPong,
}

#[derive(Clone, Copy)]
enum HeartbeatState {
    Idle {
        due_at: tokio::time::Instant,
    },
    Pending {
        nonce: u64,
        due_at: tokio::time::Instant,
        deadline: tokio::time::Instant,
        phase: SendPhase,
        sent_at: tokio::time::Instant,
    },
    Draining {
        tombstone: Option<u64>,
    },
    Failed(TcpCarrierHeartbeatFailure),
}

struct Inner {
    sample_due_at: Option<tokio::time::Instant>,
    state: HeartbeatState,
    delay: Duration,
    random_error: Option<getrandom::Error>,
}

/// Shared liveness state for exactly one authenticated TCP carrier.
pub(in crate::runtime::path::tcp) struct TcpCarrierHeartbeat {
    interval: Duration,
    timeout: Duration,
    peer_timing: Option<Arc<PeerRoundTrip>>,
    inner: Mutex<Inner>,
    schedule_tx: watch::Sender<u64>,
}

impl TcpCarrierHeartbeat {
    pub(in crate::runtime::path::tcp) fn new(
        interval: Duration,
        timeout: Duration,
        started_at: tokio::time::Instant,
        initial_random_sample: u64,
    ) -> Self {
        let delay = heartbeat_renewal_delay(interval, initial_random_sample);
        let (schedule_tx, _) = watch::channel(0);
        Self {
            interval,
            timeout,
            peer_timing: None,
            inner: Mutex::new(Inner {
                sample_due_at: None,
                state: HeartbeatState::Idle {
                    due_at: started_at + delay,
                },
                delay,
                random_error: None,
            }),
            schedule_tx,
        }
    }

    /// Reuse the existing challenge for peer timing even under continuous
    /// receive traffic. The server has no readiness RTT, so it requests an
    /// initial exchange; the client already has authenticated readiness.
    pub(in crate::runtime::path::tcp) fn with_peer_timing(
        mut self,
        timing: Arc<PeerRoundTrip>,
        probe_immediately: bool,
    ) -> Self {
        let inner = self.inner.get_mut().expect("heartbeat lock");
        if let HeartbeatState::Idle { due_at } = inner.state {
            inner.sample_due_at = Some(if probe_immediately {
                tokio::time::Instant::now()
            } else {
                due_at
            });
        }
        self.peer_timing = Some(timing);
        self
    }

    /// The next actor action is due only while idle. The independent failure
    /// future owns the immutable due-plus-timeout bound while a send is pending.
    pub(in crate::runtime::path::tcp) fn next_due_at(&self) -> Option<tokio::time::Instant> {
        let inner = self.lock();
        match inner.state {
            HeartbeatState::Idle { due_at } => Some(
                inner
                    .sample_due_at
                    .map_or(due_at, |sample| sample.min(due_at)),
            ),
            HeartbeatState::Pending { .. }
            | HeartbeatState::Draining { .. }
            | HeartbeatState::Failed(_) => None,
        }
    }

    /// Waits until the current idle due instant, re-reading after successful
    /// PONG/drain/failure transitions. Subscribe-before-read avoids losing a
    /// PONG that resets the due instant while the actor rebuilds its select.
    pub(in crate::runtime::path::tcp) async fn wait_until_due(&self) {
        let mut schedule_rx = self.schedule_tx.subscribe();
        loop {
            let _ = *schedule_rx.borrow_and_update();
            let due_at = self.next_due_at();
            if let Some(due_at) = due_at {
                if tokio::time::Instant::now() >= due_at {
                    return;
                }
                tokio::select! {
                    biased;
                    changed = schedule_rx.changed() => {
                        if changed.is_err() {
                            std::future::pending::<()>().await;
                        }
                    }
                    () = tokio::time::sleep_until(due_at) => return,
                }
            } else if schedule_rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    /// Claims a due idle probe. `nonce_source` runs only after the owner has
    /// established that this exact idle schedule is due and still live.
    pub(in crate::runtime::path::tcp) fn claim_due_ping(
        &self,
        now: tokio::time::Instant,
        nonce_source: impl FnOnce() -> Result<u64, getrandom::Error>,
    ) -> TcpCarrierHeartbeatClaim {
        let mut inner = self.lock();
        match inner.state {
            HeartbeatState::Idle { due_at }
                if now
                    < inner
                        .sample_due_at
                        .map_or(due_at, |sample| sample.min(due_at)) =>
            {
                TcpCarrierHeartbeatClaim::NotDue
            }
            HeartbeatState::Idle { due_at } => {
                let deadline = due_at.min(now) + self.timeout;
                if now >= deadline {
                    TcpCarrierHeartbeatClaim::Failed(
                        self.fail_locked(
                            &mut inner,
                            TcpCarrierHeartbeatFailure::SendProgressTimeout,
                        ),
                    )
                } else {
                    match nonce_source() {
                        Ok(nonce) => {
                            inner.state = HeartbeatState::Pending {
                                nonce,
                                due_at,
                                deadline,
                                phase: SendPhase::Sending,
                                sent_at: now,
                            };
                            // A timing challenge can precede the idle deadline.
                            // Both actor and failure waiters must reread this
                            // owner's state, including while the writer blocks.
                            self.notify_schedule_change();
                            TcpCarrierHeartbeatClaim::Claimed(TcpCarrierHeartbeatPing {
                                nonce,
                                due_at,
                                deadline,
                            })
                        }
                        Err(error) => {
                            inner.random_error = Some(error);
                            TcpCarrierHeartbeatClaim::Failed(self.fail_locked(
                                &mut inner,
                                TcpCarrierHeartbeatFailure::RandomSourceFailure,
                            ))
                        }
                    }
                }
            }
            HeartbeatState::Draining { .. } => TcpCarrierHeartbeatClaim::Draining,
            HeartbeatState::Failed(failure) => TcpCarrierHeartbeatClaim::Failed(failure),
            HeartbeatState::Pending { .. } => TcpCarrierHeartbeatClaim::NotDue,
        }
    }

    pub(in crate::runtime::path::tcp) fn current_failure(
        &self,
    ) -> Option<TcpCarrierHeartbeatFailure> {
        match self.lock().state {
            HeartbeatState::Failed(failure) => Some(failure),
            _ => None,
        }
    }

    pub(in crate::runtime::path::tcp) fn runtime_error(
        &self,
        failure: TcpCarrierHeartbeatFailure,
    ) -> RuntimeError {
        match failure {
            TcpCarrierHeartbeatFailure::ProtocolViolation => {
                RuntimeError::Protocol("unexpected TCP path heartbeat response")
            }
            TcpCarrierHeartbeatFailure::SendProgressTimeout
            | TcpCarrierHeartbeatFailure::ReplyTimeout => RuntimeError::PathHeartbeatTimeout,
            TcpCarrierHeartbeatFailure::RandomSourceFailure => self
                .lock()
                .random_error
                .take()
                .map(RuntimeError::Random)
                .unwrap_or(RuntimeError::Protocol(
                    "TCP path heartbeat random source failed",
                )),
        }
    }

    /// Records local completion without extending the peer-response deadline.
    /// A PONG may already have completed while the actor awaited its flush.
    pub(in crate::runtime::path::tcp) fn mark_ping_flushed(
        &self,
        nonce: u64,
        flushed_at: tokio::time::Instant,
    ) -> Result<(), TcpCarrierHeartbeatFailure> {
        let mut inner = self.lock();
        match inner.state {
            HeartbeatState::Pending {
                nonce: pending_nonce,
                due_at,
                deadline,
                phase: SendPhase::Sending,
                sent_at,
            } if pending_nonce == nonce => {
                if flushed_at >= deadline {
                    let failure = self
                        .fail_locked(&mut inner, TcpCarrierHeartbeatFailure::SendProgressTimeout);
                    Err(failure)
                } else {
                    inner.state = HeartbeatState::Pending {
                        nonce,
                        due_at,
                        deadline,
                        phase: SendPhase::AwaitingPong,
                        sent_at,
                    };
                    Ok(())
                }
            }
            HeartbeatState::Pending { nonce: pending, .. } if pending != nonce => {
                self.fail_locked(&mut inner, TcpCarrierHeartbeatFailure::ProtocolViolation);
                Err(TcpCarrierHeartbeatFailure::ProtocolViolation)
            }
            HeartbeatState::Failed(failure) => Err(failure),
            HeartbeatState::Idle { .. } | HeartbeatState::Draining { .. } => Ok(()),
            HeartbeatState::Pending { .. } => Ok(()),
        }
    }

    /// Applies one authenticated frame to this carrier before actor queueing.
    /// The owner samples time under its lock: receipt, drain, and expiry have
    /// one ordering, and an already terminal carrier cannot be revived by a
    /// timestamp captured before a reader was descheduled.
    /// `renewal_source` runs only for an exact, timely active PONG.
    pub(in crate::runtime::path::tcp) fn observe_authenticated_frame(
        &self,
        frame: &Frame,
        renewal_source: impl FnOnce() -> Result<u64, getrandom::Error>,
    ) -> TcpCarrierHeartbeatFrameDisposition {
        self.observe_authenticated_frame_with_clock(
            frame,
            tokio::time::Instant::now,
            renewal_source,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime::path::tcp) fn observe_authenticated_frame_at(
        &self,
        frame: &Frame,
        observed_at: tokio::time::Instant,
        renewal_source: impl FnOnce() -> Result<u64, getrandom::Error>,
    ) -> TcpCarrierHeartbeatFrameDisposition {
        self.observe_authenticated_frame_with_clock(frame, || observed_at, renewal_source)
    }

    fn observe_authenticated_frame_with_clock(
        &self,
        frame: &Frame,
        clock: impl FnOnce() -> tokio::time::Instant,
        renewal_source: impl FnOnce() -> Result<u64, getrandom::Error>,
    ) -> TcpCarrierHeartbeatFrameDisposition {
        let mut inner = self.lock();
        let observed_at = clock();
        match frame {
            Frame::Pong { nonce } => {
                match inner.state {
                    HeartbeatState::Pending {
                        nonce: pending_nonce,
                        due_at: _,
                        deadline,
                        phase,
                        sent_at,
                    } => {
                        if observed_at >= deadline {
                            let failure = match phase {
                                SendPhase::Sending => {
                                    TcpCarrierHeartbeatFailure::SendProgressTimeout
                                }
                                SendPhase::AwaitingPong => TcpCarrierHeartbeatFailure::ReplyTimeout,
                            };
                            self.fail_locked(&mut inner, failure);
                        } else if *nonce != pending_nonce {
                            self.fail_locked(
                                &mut inner,
                                TcpCarrierHeartbeatFailure::ProtocolViolation,
                            );
                        } else {
                            match renewal_source() {
                                Ok(sample) => {
                                    let delay = heartbeat_renewal_delay(self.interval, sample);
                                    if let Some(timing) = &self.peer_timing {
                                        timing.record(
                                            observed_at.saturating_duration_since(sent_at),
                                            observed_at.into_std(),
                                            PeerRoundTripSource::Heartbeat,
                                        );
                                        inner.sample_due_at = Some(observed_at + delay);
                                    }
                                    inner.delay = delay;
                                    inner.state = HeartbeatState::Idle {
                                        due_at: observed_at + delay,
                                    };
                                    self.notify_schedule_change();
                                }
                                Err(error) => {
                                    inner.random_error = Some(error);
                                    self.fail_locked(
                                        &mut inner,
                                        TcpCarrierHeartbeatFailure::RandomSourceFailure,
                                    );
                                }
                            }
                        }
                    }
                    HeartbeatState::Draining { tombstone } => {
                        if tombstone == Some(*nonce) {
                            inner.state = HeartbeatState::Draining { tombstone: None };
                        } else {
                            self.fail_locked(
                                &mut inner,
                                TcpCarrierHeartbeatFailure::ProtocolViolation,
                            );
                        }
                    }
                    HeartbeatState::Failed(_) => {}
                    HeartbeatState::Idle { due_at } => {
                        if observed_at >= due_at + self.timeout {
                            self.fail_locked(
                                &mut inner,
                                TcpCarrierHeartbeatFailure::SendProgressTimeout,
                            );
                        } else {
                            self.fail_locked(
                                &mut inner,
                                TcpCarrierHeartbeatFailure::ProtocolViolation,
                            );
                        }
                    }
                }
                TcpCarrierHeartbeatFrameDisposition::Consume
            }
            _ => {
                match inner.state {
                    HeartbeatState::Idle { due_at } => {
                        if observed_at >= due_at + self.timeout {
                            self.fail_locked(
                                &mut inner,
                                TcpCarrierHeartbeatFailure::SendProgressTimeout,
                            );
                        } else {
                            inner.state = HeartbeatState::Idle {
                                due_at: observed_at + inner.delay,
                            };
                        }
                    }
                    HeartbeatState::Pending {
                        deadline, phase, ..
                    } if observed_at >= deadline => {
                        let failure = match phase {
                            SendPhase::Sending => TcpCarrierHeartbeatFailure::SendProgressTimeout,
                            SendPhase::AwaitingPong => TcpCarrierHeartbeatFailure::ReplyTimeout,
                        };
                        self.fail_locked(&mut inner, failure);
                    }
                    HeartbeatState::Pending { .. }
                    | HeartbeatState::Draining { .. }
                    | HeartbeatState::Failed(_) => {}
                }
                TcpCarrierHeartbeatFrameDisposition::Forward
            }
        }
    }

    /// Stops new probes. If a probe is outstanding, retain its one nonce so a
    /// late exact PONG remains consumable through graceful retirement.
    pub(in crate::runtime::path::tcp) fn begin_drain(&self) {
        let mut inner = self.lock();
        self.begin_drain_locked(&mut inner, tokio::time::Instant::now());
    }

    /// Deterministic clock seam for drain boundary tests.
    #[cfg(test)]
    pub(in crate::runtime::path::tcp) fn begin_drain_at(&self, observed_at: tokio::time::Instant) {
        let mut inner = self.lock();
        self.begin_drain_locked(&mut inner, observed_at);
    }

    pub(in crate::runtime::path::tcp) async fn failure_future(
        self: std::sync::Arc<Self>,
    ) -> TcpCarrierHeartbeatFailure {
        self.wait_for_failure().await
    }

    async fn wait_for_failure(&self) -> TcpCarrierHeartbeatFailure {
        let mut schedule_rx = self.schedule_tx.subscribe();
        loop {
            let _ = *schedule_rx.borrow_and_update();
            if let Some(failure) = self.current_failure() {
                return failure;
            }
            let deadline = self.failure_deadline();
            if let Some(deadline) = deadline {
                tokio::select! {
                    biased;
                    changed = schedule_rx.changed() => {
                        if changed.is_err() {
                            std::future::pending::<()>().await;
                        }
                    }
                    () = tokio::time::sleep_until(deadline) => {
                        self.expire(tokio::time::Instant::now());
                    }
                }
            } else if schedule_rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    fn failure_deadline(&self) -> Option<tokio::time::Instant> {
        match self.lock().state {
            HeartbeatState::Idle { due_at } => Some(due_at + self.timeout),
            HeartbeatState::Pending { deadline, .. } => Some(deadline),
            HeartbeatState::Draining { .. } | HeartbeatState::Failed(_) => None,
        }
    }

    fn expire(&self, now: tokio::time::Instant) {
        let mut inner = self.lock();
        let failure = match inner.state {
            HeartbeatState::Idle { due_at } if now >= due_at + self.timeout => {
                Some(TcpCarrierHeartbeatFailure::SendProgressTimeout)
            }
            HeartbeatState::Pending {
                deadline, phase, ..
            } if now >= deadline => Some(match phase {
                SendPhase::Sending => TcpCarrierHeartbeatFailure::SendProgressTimeout,
                SendPhase::AwaitingPong => TcpCarrierHeartbeatFailure::ReplyTimeout,
            }),
            _ => None,
        };
        if let Some(failure) = failure {
            self.fail_locked(&mut inner, failure);
        }
    }

    fn begin_drain_locked(&self, inner: &mut Inner, at: tokio::time::Instant) {
        let tombstone = match inner.state {
            HeartbeatState::Idle { due_at } => {
                if at >= due_at + self.timeout {
                    self.fail_locked(inner, TcpCarrierHeartbeatFailure::SendProgressTimeout);
                    return;
                }
                None
            }
            HeartbeatState::Pending {
                nonce,
                deadline,
                phase,
                ..
            } => {
                if at >= deadline {
                    let failure = match phase {
                        SendPhase::Sending => TcpCarrierHeartbeatFailure::SendProgressTimeout,
                        SendPhase::AwaitingPong => TcpCarrierHeartbeatFailure::ReplyTimeout,
                    };
                    self.fail_locked(inner, failure);
                    return;
                }
                Some(nonce)
            }
            HeartbeatState::Draining { .. } => return,
            HeartbeatState::Failed(_) => return,
        };
        inner.state = HeartbeatState::Draining { tombstone };
        self.notify_schedule_change();
    }

    fn fail_locked(
        &self,
        inner: &mut Inner,
        failure: TcpCarrierHeartbeatFailure,
    ) -> TcpCarrierHeartbeatFailure {
        if let HeartbeatState::Failed(existing) = inner.state {
            return existing;
        }
        inner.state = HeartbeatState::Failed(failure);
        self.notify_schedule_change();
        failure
    }

    fn notify_schedule_change(&self) {
        self.schedule_tx.send_modify(|revision| {
            *revision = revision.wrapping_add(1);
        });
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .expect("TCP carrier heartbeat state lock poisoned")
    }
}

/// Maps one uniform `u64` sample to the existing RFC heartbeat renewal window.
pub(in crate::runtime::path::tcp) fn heartbeat_renewal_delay(
    maximum: Duration,
    sample: u64,
) -> Duration {
    let maximum_nanos = maximum.as_nanos();
    let minimum_nanos = maximum_nanos.saturating_mul(4) / 5;
    let span = maximum_nanos.saturating_sub(minimum_nanos);
    let offset = (u128::from(sample).saturating_mul(span.saturating_add(1))) >> 64;
    duration_from_nanos(minimum_nanos.saturating_add(offset).min(maximum_nanos))
}

fn duration_from_nanos(nanos: u128) -> Duration {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds = nanos / NANOS_PER_SECOND;
    let subsecond_nanos = (nanos % NANOS_PER_SECOND) as u32;
    Duration::new(u64::try_from(seconds).unwrap_or(u64::MAX), subsecond_nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn timed_owner(
        start: tokio::time::Instant,
        immediate: bool,
    ) -> (TcpCarrierHeartbeat, Arc<PeerRoundTrip>) {
        let timing = Arc::new(PeerRoundTrip::new(
            Duration::from_secs(10),
            Duration::from_secs(30),
            crate::model::timing::PeerTiming::new(333.0, 0.0),
        ));
        let owner =
            TcpCarrierHeartbeat::new(Duration::from_secs(10), Duration::from_secs(30), start, 0)
                .with_peer_timing(timing.clone(), immediate);
        (owner, timing)
    }

    #[tokio::test(start_paused = true)]
    async fn busy_carrier_samples_from_send_attempt_and_accepts_pong_before_flush() {
        let start = tokio::time::Instant::now();
        let (heartbeat, timing) = timed_owner(start, false);
        for seconds in 1..=9 {
            heartbeat.observe_authenticated_frame_at(
                &Frame::Ping { nonce: 11 },
                start + Duration::from_secs(seconds),
                || Ok(0),
            );
        }
        // Receive activity defers idle expiry, but not peer measurement. A late
        // actor wake must not include the missed scheduling time in the RTT.
        assert_eq!(
            heartbeat.next_due_at(),
            Some(start + Duration::from_secs(8))
        );
        let sent = start + Duration::from_secs(10);
        let TcpCarrierHeartbeatClaim::Claimed(ping) = heartbeat.claim_due_ping(sent, || Ok(42))
        else {
            panic!("due sample");
        };
        assert_eq!(ping.deadline, sent + Duration::from_secs(30));
        let received = sent + Duration::from_millis(160);
        heartbeat.observe_authenticated_frame_at(&Frame::Pong { nonce: 42 }, received, || Ok(0));
        heartbeat
            .mark_ping_flushed(42, received + Duration::from_millis(10))
            .unwrap();
        assert_eq!(timing.last().unwrap().elapsed, Duration::from_millis(160));
        assert_eq!(timing.last().unwrap().observed_at, received.into_std());
        assert_eq!(
            heartbeat.next_due_at(),
            Some(received + Duration::from_secs(8))
        );
        assert!(
            timing
                .timing_at((received + Duration::from_secs(41)).into_std())
                .is_none()
        );
        assert!(
            timing.last().is_some(),
            "stale evidence remains diagnostic, never renewed by reads"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn server_bootstrap_and_invalid_replies_do_not_create_peer_evidence() {
        let start = tokio::time::Instant::now();
        for invalid in ["nonce", "expiry", "drain"] {
            let (heartbeat, timing) = timed_owner(start, true);
            let TcpCarrierHeartbeatClaim::Claimed(ping) = heartbeat.claim_due_ping(start, || Ok(7))
            else {
                panic!("initial server sample");
            };
            if invalid == "drain" {
                heartbeat.begin_drain_at(start);
            }
            heartbeat.observe_authenticated_frame_at(
                &Frame::Pong {
                    nonce: if invalid == "nonce" { 8 } else { 7 },
                },
                if invalid == "expiry" {
                    ping.deadline
                } else {
                    start + Duration::from_millis(20)
                },
                || Ok(0),
            );
            assert!(
                timing.last().is_none(),
                "{invalid} is not an accepted peer exchange"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_probe_rearms_an_already_waiting_failure_guard() {
        for flushed in [false, true] {
            let start = tokio::time::Instant::now();
            let (heartbeat, timing) = timed_owner(start, true);
            let heartbeat = Arc::new(heartbeat);
            let failure = heartbeat.clone().failure_future();
            tokio::pin!(failure);
            // Start waiting on the later idle deadline before publication of
            // the initial timing challenge shortens the response deadline.
            tokio::select! {
                biased;
                result = &mut failure => panic!("healthy carrier failed: {result:?}"),
                () = tokio::task::yield_now() => {}
            }
            let TcpCarrierHeartbeatClaim::Claimed(ping) = heartbeat.claim_due_ping(start, || Ok(9))
            else {
                panic!("initial peer challenge");
            };
            if flushed {
                heartbeat.mark_ping_flushed(ping.nonce, start).unwrap();
            }
            tokio::time::advance(ping.deadline.duration_since(start)).await;
            assert_eq!(
                tokio::time::timeout(Duration::from_millis(1), &mut failure)
                    .await
                    .expect("pending challenge must enforce its own deadline"),
                if flushed {
                    TcpCarrierHeartbeatFailure::ReplyTimeout
                } else {
                    TcpCarrierHeartbeatFailure::SendProgressTimeout
                }
            );
            assert!(timing.last().is_none());
        }
    }

    fn owner_at(start: tokio::time::Instant) -> Arc<TcpCarrierHeartbeat> {
        Arc::new(TcpCarrierHeartbeat::new(
            Duration::from_secs(10),
            Duration::from_secs(30),
            start,
            0,
        ))
    }

    #[test]
    fn failure_reasons_are_stable_webhook_contracts() {
        assert_eq!(
            TcpCarrierHeartbeatFailure::SendProgressTimeout.reason(),
            "heartbeat_send_progress_timeout"
        );
        assert_eq!(
            TcpCarrierHeartbeatFailure::ReplyTimeout.reason(),
            "heartbeat_reply_timeout"
        );
        assert_eq!(
            TcpCarrierHeartbeatFailure::ProtocolViolation.reason(),
            "heartbeat_protocol_error"
        );
        assert_eq!(
            TcpCarrierHeartbeatFailure::RandomSourceFailure.reason(),
            "heartbeat_random_source_error"
        );
    }

    #[test]
    fn receive_only_idle_refresh_stops_at_challenge_claim() {
        let start = tokio::time::Instant::now();
        let heartbeat = owner_at(start);
        let initial_due = start + Duration::from_secs(8);
        assert_eq!(heartbeat.next_due_at(), Some(initial_due));

        heartbeat.observe_authenticated_frame_at(
            &Frame::Ping { nonce: 1 },
            start + Duration::from_secs(7),
            || Ok(0),
        );
        let renewed_due = start + Duration::from_secs(15);
        assert_eq!(heartbeat.next_due_at(), Some(renewed_due));

        assert!(matches!(
            heartbeat.claim_due_ping(renewed_due, || Ok(10)),
            TcpCarrierHeartbeatClaim::Claimed(_)
        ));
        heartbeat.observe_authenticated_frame_at(
            &Frame::Ping { nonce: 2 },
            renewed_due + Duration::from_secs(1),
            || Ok(0),
        );
        assert_eq!(heartbeat.next_due_at(), None);
    }

    #[test]
    fn timely_pong_during_send_completes_and_rearms_actor_deadline() {
        let start = tokio::time::Instant::now();
        let heartbeat = owner_at(start);
        let due_at = start + Duration::from_secs(8);
        let TcpCarrierHeartbeatClaim::Claimed(ping) = heartbeat.claim_due_ping(due_at, || Ok(44))
        else {
            panic!("heartbeat due claim");
        };
        let observed_at = due_at + Duration::from_secs(1);

        assert_eq!(
            heartbeat.observe_authenticated_frame_at(
                &Frame::Pong { nonce: 44 },
                observed_at,
                || Ok(0),
            ),
            TcpCarrierHeartbeatFrameDisposition::Consume
        );
        assert_eq!(
            heartbeat.next_due_at(),
            Some(observed_at + Duration::from_secs(8))
        );
        assert!(heartbeat.mark_ping_flushed(44, observed_at).is_ok());
        assert!(ping.deadline > observed_at);
    }

    #[test]
    fn non_pong_cannot_extend_flushed_ping_reply_deadline() {
        let start = tokio::time::Instant::now();
        let heartbeat = owner_at(start);
        let due_at = start + Duration::from_secs(8);
        let TcpCarrierHeartbeatClaim::Claimed(ping) = heartbeat.claim_due_ping(due_at, || Ok(44))
        else {
            panic!("heartbeat due claim");
        };
        assert_eq!(ping.deadline, due_at + Duration::from_secs(30));
        heartbeat
            .mark_ping_flushed(ping.nonce, due_at + Duration::from_secs(1))
            .expect("PING flush before the immutable response deadline");

        heartbeat.observe_authenticated_frame_at(
            &Frame::Ping { nonce: 12 },
            ping.deadline - Duration::from_nanos(1),
            || Ok(0),
        );
        assert_eq!(heartbeat.current_failure(), None);
        assert_eq!(heartbeat.failure_deadline(), Some(ping.deadline));
        heartbeat
            .observe_authenticated_frame_at(&Frame::Pong { nonce: 44 }, ping.deadline, || Ok(0));
        assert_eq!(
            heartbeat.current_failure(),
            Some(TcpCarrierHeartbeatFailure::ReplyTimeout)
        );

        let other = owner_at(start);
        let TcpCarrierHeartbeatClaim::Claimed(other_ping) = other.claim_due_ping(due_at, || Ok(45))
        else {
            panic!("second heartbeat due claim");
        };
        other.observe_authenticated_frame_at(
            &Frame::Pong { nonce: 46 },
            due_at + Duration::from_secs(1),
            || Ok(0),
        );
        assert_eq!(
            other.current_failure(),
            Some(TcpCarrierHeartbeatFailure::ProtocolViolation)
        );
        assert!(other_ping.deadline > due_at);
    }

    #[test]
    fn serialized_receipt_cannot_revive_expired_carrier_from_earlier_timestamp() {
        let start = tokio::time::Instant::now();
        let heartbeat = owner_at(start);
        let due_at = start + Duration::from_secs(8);
        let TcpCarrierHeartbeatClaim::Claimed(ping) = heartbeat.claim_due_ping(due_at, || Ok(57))
        else {
            panic!("heartbeat due claim");
        };
        heartbeat.mark_ping_flushed(57, due_at).unwrap();
        heartbeat.expire(ping.deadline);
        let earlier = ping.deadline - Duration::from_nanos(1);
        heartbeat.observe_authenticated_frame_at(&Frame::Pong { nonce: 57 }, earlier, || {
            panic!("terminal receipt must not sample renewal entropy")
        });
        heartbeat.begin_drain_at(earlier);
        assert_eq!(
            heartbeat.current_failure(),
            Some(TcpCarrierHeartbeatFailure::ReplyTimeout)
        );
        assert_eq!(heartbeat.next_due_at(), None);
    }

    #[test]
    fn receipt_clock_and_expiry_use_one_serialized_owner_boundary() {
        let start = tokio::time::Instant::now();
        let heartbeat = owner_at(start);
        let due_at = start + Duration::from_secs(8);
        let TcpCarrierHeartbeatClaim::Claimed(ping) = heartbeat.claim_due_ping(due_at, || Ok(58))
        else {
            panic!("heartbeat due claim");
        };
        heartbeat.mark_ping_flushed(58, due_at).unwrap();
        heartbeat.observe_authenticated_frame_with_clock(
            &Frame::Pong { nonce: 58 },
            || {
                assert!(matches!(
                    heartbeat.inner.try_lock(),
                    Err(std::sync::TryLockError::WouldBlock)
                ));
                ping.deadline - Duration::from_nanos(1)
            },
            || Ok(0),
        );
        heartbeat.expire(ping.deadline);
        assert_eq!(heartbeat.current_failure(), None);
        assert!(
            heartbeat
                .next_due_at()
                .is_some_and(|due| due > ping.deadline)
        );
    }

    #[test]
    fn accepted_drain_consumes_one_exact_pending_pong_nonce() {
        let start = tokio::time::Instant::now();
        let heartbeat = owner_at(start);
        let due_at = start + Duration::from_secs(8);
        assert!(matches!(
            heartbeat.claim_due_ping(due_at, || Ok(57)),
            TcpCarrierHeartbeatClaim::Claimed(_)
        ));
        heartbeat.begin_drain_at(due_at + Duration::from_secs(1));
        heartbeat.expire(due_at + Duration::from_secs(31));
        assert_eq!(heartbeat.current_failure(), None);
        assert_eq!(
            heartbeat.observe_authenticated_frame_at(
                &Frame::Pong { nonce: 57 },
                due_at + Duration::from_secs(2),
                || Ok(0),
            ),
            TcpCarrierHeartbeatFrameDisposition::Consume
        );
        assert_eq!(heartbeat.current_failure(), None);
        heartbeat.observe_authenticated_frame_at(
            &Frame::Pong { nonce: 57 },
            due_at + Duration::from_secs(3),
            || Ok(0),
        );
        assert_eq!(
            heartbeat.current_failure(),
            Some(TcpCarrierHeartbeatFailure::ProtocolViolation)
        );
    }

    #[test]
    fn drain_before_deadline_suppresses_expiry_but_drain_at_deadline_does_not() {
        let start = tokio::time::Instant::now();
        let due_at = start + Duration::from_secs(8);
        let accepted = owner_at(start);
        let TcpCarrierHeartbeatClaim::Claimed(ping) = accepted.claim_due_ping(due_at, || Ok(61))
        else {
            panic!("heartbeat due claim");
        };
        accepted
            .mark_ping_flushed(ping.nonce, due_at + Duration::from_secs(1))
            .expect("PING flush before drain");
        accepted.begin_drain_at(ping.deadline - Duration::from_nanos(1));
        accepted.expire(ping.deadline + Duration::from_secs(1));
        assert_eq!(accepted.current_failure(), None);

        let expired = owner_at(start);
        let TcpCarrierHeartbeatClaim::Claimed(expired_ping) =
            expired.claim_due_ping(due_at, || Ok(62))
        else {
            panic!("second heartbeat due claim");
        };
        expired
            .mark_ping_flushed(expired_ping.nonce, due_at + Duration::from_secs(1))
            .expect("second PING flush before deadline");
        expired.begin_drain_at(expired_ping.deadline);
        assert_eq!(
            expired.current_failure(),
            Some(TcpCarrierHeartbeatFailure::ReplyTimeout)
        );
    }

    #[test]
    fn draining_without_a_nonce_rejects_pong_and_wrong_tombstone_is_terminal() {
        let start = tokio::time::Instant::now();
        let absent = owner_at(start);
        absent.begin_drain_at(start + Duration::from_secs(1));
        let mut sample_calls = 0;
        absent.observe_authenticated_frame_at(
            &Frame::Pong { nonce: 71 },
            start + Duration::from_secs(2),
            || {
                sample_calls += 1;
                Ok(0)
            },
        );
        assert_eq!(sample_calls, 0, "unsolicited PONG sampled renewal entropy");
        assert_eq!(
            absent.current_failure(),
            Some(TcpCarrierHeartbeatFailure::ProtocolViolation)
        );

        let with_tombstone = owner_at(start);
        let due_at = start + Duration::from_secs(8);
        assert!(matches!(
            with_tombstone.claim_due_ping(due_at, || Ok(72)),
            TcpCarrierHeartbeatClaim::Claimed(_)
        ));
        with_tombstone.begin_drain_at(due_at + Duration::from_secs(1));
        with_tombstone.observe_authenticated_frame_at(
            &Frame::Pong { nonce: 73 },
            due_at + Duration::from_secs(2),
            || {
                sample_calls += 1;
                Ok(0)
            },
        );
        assert_eq!(sample_calls, 0, "wrong tombstone sampled renewal entropy");
        assert_eq!(
            with_tombstone.current_failure(),
            Some(TcpCarrierHeartbeatFailure::ProtocolViolation)
        );
    }

    #[test]
    fn entropy_callbacks_are_lazy_and_failures_belong_to_the_carrier() {
        let start = tokio::time::Instant::now();
        let mut nonce_calls = 0;
        let not_due = owner_at(start);
        assert_eq!(
            not_due.claim_due_ping(start, || {
                nonce_calls += 1;
                Ok(1)
            }),
            TcpCarrierHeartbeatClaim::NotDue
        );
        not_due.begin_drain_at(start + Duration::from_secs(1));
        assert_eq!(
            not_due.claim_due_ping(start + Duration::from_secs(9), || {
                nonce_calls += 1;
                Ok(2)
            }),
            TcpCarrierHeartbeatClaim::Draining
        );
        assert_eq!(nonce_calls, 0, "nonce entropy was sampled without a claim");

        let expired = owner_at(start - Duration::from_secs(40));
        assert_eq!(
            expired.claim_due_ping(start, || {
                nonce_calls += 1;
                Ok(3)
            }),
            TcpCarrierHeartbeatClaim::Failed(TcpCarrierHeartbeatFailure::SendProgressTimeout)
        );
        assert_eq!(nonce_calls, 0, "expired claim sampled an unusable nonce");

        let failed_nonce = owner_at(start);
        assert_eq!(
            failed_nonce.claim_due_ping(start + Duration::from_secs(8), || {
                nonce_calls += 1;
                Err(getrandom::Error::UNSUPPORTED)
            }),
            TcpCarrierHeartbeatClaim::Failed(TcpCarrierHeartbeatFailure::RandomSourceFailure)
        );
        assert_eq!(nonce_calls, 1);
        assert!(matches!(
            failed_nonce.runtime_error(TcpCarrierHeartbeatFailure::RandomSourceFailure),
            RuntimeError::Random(error) if error == getrandom::Error::UNSUPPORTED
        ));

        let wrong = owner_at(start);
        let due_at = start + Duration::from_secs(8);
        assert!(matches!(
            wrong.claim_due_ping(due_at, || Ok(81)),
            TcpCarrierHeartbeatClaim::Claimed(_)
        ));
        let mut renewal_calls = 0;
        wrong.observe_authenticated_frame_at(
            &Frame::Pong { nonce: 82 },
            due_at + Duration::from_secs(1),
            || {
                renewal_calls += 1;
                Ok(0)
            },
        );
        assert_eq!(renewal_calls, 0, "wrong PONG sampled a renewal delay");
        assert_eq!(
            wrong.current_failure(),
            Some(TcpCarrierHeartbeatFailure::ProtocolViolation)
        );

        let late = owner_at(start);
        let TcpCarrierHeartbeatClaim::Claimed(late_ping) = late.claim_due_ping(due_at, || Ok(83))
        else {
            panic!("late-PONG fixture claim");
        };
        late.mark_ping_flushed(late_ping.nonce, due_at + Duration::from_secs(1))
            .expect("late-PONG fixture flush");
        late.observe_authenticated_frame_at(
            &Frame::Pong {
                nonce: late_ping.nonce,
            },
            late_ping.deadline,
            || {
                renewal_calls += 1;
                Ok(0)
            },
        );
        assert_eq!(renewal_calls, 0, "late PONG sampled a renewal delay");
        assert_eq!(
            late.current_failure(),
            Some(TcpCarrierHeartbeatFailure::ReplyTimeout)
        );

        let draining = owner_at(start);
        assert!(matches!(
            draining.claim_due_ping(due_at, || Ok(84)),
            TcpCarrierHeartbeatClaim::Claimed(_)
        ));
        draining.begin_drain_at(due_at + Duration::from_secs(1));
        draining.observe_authenticated_frame_at(
            &Frame::Pong { nonce: 84 },
            due_at + Duration::from_secs(2),
            || {
                renewal_calls += 1;
                Ok(0)
            },
        );
        assert_eq!(renewal_calls, 0, "drain tombstone sampled renewal entropy");
        assert_eq!(draining.current_failure(), None);

        let failed_renewal = owner_at(start);
        assert!(matches!(
            failed_renewal.claim_due_ping(due_at, || Ok(85)),
            TcpCarrierHeartbeatClaim::Claimed(_)
        ));
        failed_renewal.observe_authenticated_frame_at(
            &Frame::Pong { nonce: 85 },
            due_at + Duration::from_secs(1),
            || {
                renewal_calls += 1;
                Err(getrandom::Error::UNSUPPORTED)
            },
        );
        assert_eq!(renewal_calls, 1);
        assert_eq!(
            failed_renewal.current_failure(),
            Some(TcpCarrierHeartbeatFailure::RandomSourceFailure)
        );
        assert!(matches!(
            failed_renewal.runtime_error(TcpCarrierHeartbeatFailure::RandomSourceFailure),
            RuntimeError::Random(error) if error == getrandom::Error::UNSUPPORTED
        ));
    }

    #[tokio::test]
    async fn failure_guard_expires_before_actor_claim_without_restarting_budget() {
        let start = tokio::time::Instant::now() - Duration::from_secs(40);
        let heartbeat = owner_at(start);
        let result = tokio::time::timeout(Duration::from_millis(100), heartbeat.failure_future())
            .await
            .expect("due-plus-timeout guard did not fire");
        assert_eq!(result, TcpCarrierHeartbeatFailure::SendProgressTimeout);
    }

    #[tokio::test(start_paused = true)]
    async fn actor_deadline_wait_rearms_after_decode_consumes_pong() {
        let now = tokio::time::Instant::now();
        let heartbeat = Arc::new(TcpCarrierHeartbeat::new(
            Duration::from_millis(10),
            Duration::from_millis(30),
            now - Duration::from_millis(10),
            0,
        ));
        assert!(matches!(
            heartbeat.claim_due_ping(now, || Ok(81)),
            TcpCarrierHeartbeatClaim::Claimed(_)
        ));
        let waiting_owner = heartbeat.clone();
        let due_wait = tokio::spawn(async move { waiting_owner.wait_until_due().await });
        tokio::task::yield_now().await;

        let observed_at = tokio::time::Instant::now();
        heartbeat.observe_authenticated_frame_at(&Frame::Pong { nonce: 81 }, observed_at, || Ok(0));
        tokio::task::yield_now().await;
        assert!(
            !due_wait.is_finished(),
            "old due fired after PONG reset the schedule"
        );

        // The fixture's interval is 10 ms, so sample zero renews at 8 ms.
        let renewed_delay = heartbeat_renewal_delay(Duration::from_millis(10), 0);
        tokio::time::advance(renewed_delay - Duration::from_nanos(1)).await;
        tokio::task::yield_now().await;
        assert!(
            !due_wait.is_finished(),
            "actor deadline fired before renewed due"
        );
        tokio::time::advance(Duration::from_nanos(1)).await;
        due_wait.await.expect("actor deadline task panicked");
    }
}
