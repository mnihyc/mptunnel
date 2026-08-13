//! Product balancer selection and flow-lifetime accounting.
//!
//! This state is consulted only when a Product flow opens or completes. It
//! never enters carrier selection, payload forwarding, or reinjection.

use crate::config::GatewayBalancerConfig;
use crate::product::{
    GatewayBalancer, GatewayEntropy, GatewayFreshnessStatus, GatewayHealthStatus, GatewayInstant,
    GatewayLoad, GatewayMemberCounters, GatewayMemberHandle, GatewayMemberMode,
    GatewayObservationSource, GatewayOutcome, GatewayProbePolicy, GatewaySelectionReason,
    GatewayStrategy, Network, NetworkSet, OutboundId, PrincipalId, ProtocolTarget,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::random_u64;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(in crate::runtime) struct ClientGatewayRuntime {
    inner: Arc<ClientGatewayInner>,
    members: Arc<[OutboundId]>,
    generation: u64,
    probe: Option<GatewayProbePolicy>,
}

struct ClientGatewayInner {
    origin: Instant,
    state: Mutex<ClientGatewayState>,
}

struct ClientGatewayState {
    balancer: GatewayBalancer,
    entropy: GatewayRuntimeEntropy,
}

struct GatewayRuntimeEntropy {
    state: u64,
}

impl GatewayEntropy for GatewayRuntimeEntropy {
    fn next_u64(&mut self) -> u64 {
        // SplitMix64 is a small, deterministic per-generation selector PRNG.
        // Its seed comes from the OS once; gateway weighting is not a
        // cryptographic use and therefore needs no per-flow syscall.
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}

pub(in crate::runtime) struct GatewaySelectionBinding {
    pub(in crate::runtime) handle: GatewayMemberHandle,
    pub(in crate::runtime) lease: GatewayFlowLease,
}

pub(in crate::runtime) struct GatewayFlowLease {
    runtime: ClientGatewayRuntime,
    handle: GatewayMemberHandle,
    selected_at: Instant,
    recovery_probe: bool,
    phase: GatewayFlowPhase,
}

pub(in crate::runtime) struct GatewayProbeLease {
    runtime: ClientGatewayRuntime,
    handle: GatewayMemberHandle,
    started_at: Instant,
    complete: bool,
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct GatewayRuntimeSnapshot {
    pub(in crate::runtime) now: GatewayInstant,
    pub(in crate::runtime) generation: u64,
    pub(in crate::runtime) strategy: GatewayStrategy,
    pub(in crate::runtime) manual_member: Option<OutboundId>,
    pub(in crate::runtime) probe: Option<GatewayProbePolicy>,
    pub(in crate::runtime) members: Vec<GatewayRuntimeMemberSnapshot>,
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct GatewayRuntimeMemberSnapshot {
    pub(in crate::runtime) member: OutboundId,
    pub(in crate::runtime) networks: NetworkSet,
    pub(in crate::runtime) mode: GatewayMemberMode,
    pub(in crate::runtime) health: GatewayHealthStatus,
    pub(in crate::runtime) freshness: GatewayFreshnessStatus,
    pub(in crate::runtime) probe_in_flight: bool,
    pub(in crate::runtime) consecutive_failures: u32,
    pub(in crate::runtime) recovery_successes: u32,
    pub(in crate::runtime) latency_ewma: Option<Duration>,
    pub(in crate::runtime) last_latency_observation: Option<GatewayInstant>,
    pub(in crate::runtime) last_latency_observation_source: Option<GatewayObservationSource>,
    pub(in crate::runtime) load: GatewayLoad,
    pub(in crate::runtime) last_observation: Option<GatewayInstant>,
    pub(in crate::runtime) last_observation_source: Option<GatewayObservationSource>,
    pub(in crate::runtime) last_error: Option<String>,
    pub(in crate::runtime) last_error_at: Option<GatewayInstant>,
    pub(in crate::runtime) last_selection_reason: Option<GatewaySelectionReason>,
    pub(in crate::runtime) last_selected_at: Option<GatewayInstant>,
    pub(in crate::runtime) counters: GatewayMemberCounters,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GatewayFlowPhase {
    Pending,
    Active,
    Complete,
}

impl ClientGatewayRuntime {
    pub(in crate::runtime) fn compile(
        config: &GatewayBalancerConfig,
    ) -> Result<Self, RuntimeError> {
        let balancer = GatewayBalancer::compile(config.generation, config.spec.clone())
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
        Ok(Self {
            generation: config.generation,
            probe: config.spec.probe.clone(),
            members: config
                .spec
                .members
                .iter()
                .map(|member| member.id.clone())
                .collect(),
            inner: Arc::new(ClientGatewayInner {
                origin: Instant::now(),
                state: Mutex::new(ClientGatewayState {
                    balancer,
                    entropy: GatewayRuntimeEntropy {
                        state: random_u64()?,
                    },
                }),
            }),
        })
    }

    pub(in crate::runtime) fn members(&self) -> &[OutboundId] {
        &self.members
    }

    pub(in crate::runtime) fn member_count(&self) -> usize {
        self.members.len()
    }

    pub(in crate::runtime) fn member_id(
        &self,
        handle: GatewayMemberHandle,
    ) -> Result<&OutboundId, RuntimeError> {
        if handle.generation() != self.generation {
            return Err(RuntimeError::ProductPolicy(
                "balancer selection handle belongs to another runtime generation".to_string(),
            ));
        }
        self.members.get(usize::from(handle.slot())).ok_or_else(|| {
            RuntimeError::ProductPolicy(
                "balancer selection handle has no runtime member".to_string(),
            )
        })
    }

    pub(in crate::runtime) fn member_handle(
        &self,
        member: &OutboundId,
    ) -> Result<GatewayMemberHandle, RuntimeError> {
        self.lock()?.balancer.handle_for(member).ok_or_else(|| {
            RuntimeError::ProductPolicy(
                "balancer runtime member has no selection handle".to_string(),
            )
        })
    }

    pub(in crate::runtime) const fn probe_policy(&self) -> Option<&GatewayProbePolicy> {
        self.probe.as_ref()
    }

    #[cfg(test)]
    pub(in crate::runtime) fn select(
        &self,
        network: Network,
        destination: &ProtocolTarget,
        excluded: &[GatewayMemberHandle],
    ) -> Result<GatewaySelectionBinding, RuntimeError> {
        self.select_for_principal(network, destination, None, excluded)
    }

    pub(in crate::runtime) fn select_for_principal(
        &self,
        network: Network,
        destination: &ProtocolTarget,
        principal: Option<&PrincipalId>,
        excluded: &[GatewayMemberHandle],
    ) -> Result<GatewaySelectionBinding, RuntimeError> {
        let now = self.now();
        let mut state = self.lock()?;
        let ClientGatewayState { balancer, entropy } = &mut *state;
        let selection = balancer
            .select_with_principal(
                now,
                network,
                Some(destination),
                principal,
                excluded,
                entropy,
            )
            .map_err(|error| RuntimeError::GatewayUnavailable(error.to_string()))?;
        let handle = selection.handle();
        let reason = selection.reason();
        if !selection.may_attempt() {
            return Err(RuntimeError::GatewayUnavailable(format!("{reason:?}")));
        }
        balancer
            .record_open_attempt(handle)
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
        adjust_pending(balancer, handle, now, true)?;
        drop(state);
        Ok(GatewaySelectionBinding {
            handle,
            lease: GatewayFlowLease {
                runtime: self.clone(),
                handle,
                selected_at: Instant::now(),
                recovery_probe: matches!(reason, GatewaySelectionReason::AllUnhealthyRecoveryProbe),
                phase: GatewayFlowPhase::Pending,
            },
        })
    }

    pub(in crate::runtime) fn set_member_mode(
        &self,
        member: &OutboundId,
        mode: GatewayMemberMode,
    ) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        let handle = state.balancer.handle_for(member).ok_or_else(|| {
            RuntimeError::GatewayUnavailable(format!(
                "balancer member {} is not configured",
                member.as_str()
            ))
        })?;
        state
            .balancer
            .set_member_mode(handle, mode)
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))
    }

    pub(in crate::runtime) fn set_manual_member(
        &self,
        member: Option<&OutboundId>,
    ) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        let handle = member
            .map(|member| {
                state.balancer.handle_for(member).ok_or_else(|| {
                    RuntimeError::GatewayUnavailable(format!(
                        "balancer member {} is not configured",
                        member.as_str()
                    ))
                })
            })
            .transpose()?;
        state
            .balancer
            .set_manual_override(handle)
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))
    }

    pub(in crate::runtime) fn begin_active_probe(
        &self,
        member: &OutboundId,
    ) -> Result<Option<GatewayProbeLease>, RuntimeError> {
        let now = self.now();
        let mut state = self.lock()?;
        let handle = state.balancer.handle_for(member).ok_or_else(|| {
            RuntimeError::GatewayUnavailable(format!(
                "balancer member {} is not configured",
                member.as_str()
            ))
        })?;
        if !state
            .balancer
            .claim_active_probe(handle, now)
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?
        {
            return Ok(None);
        }
        Ok(Some(GatewayProbeLease {
            runtime: self.clone(),
            handle,
            started_at: Instant::now(),
            complete: false,
        }))
    }

    pub(in crate::runtime) fn snapshot(&self) -> Result<GatewayRuntimeSnapshot, RuntimeError> {
        let now = self.now();
        let state = self.lock()?;
        let manual_member = state
            .balancer
            .manual_override()
            .map(|handle| state.balancer.member_id(handle).cloned())
            .transpose()
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
        let mut members = Vec::with_capacity(self.members.len());
        for member in self.members.iter() {
            let handle = state
                .balancer
                .handle_for(member)
                .expect("runtime member inventory matches Product balancer");
            let status = state
                .balancer
                .member_status(handle, now)
                .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
            members.push(GatewayRuntimeMemberSnapshot {
                member: status.member.clone(),
                networks: status.networks,
                mode: status.mode,
                health: status.health,
                freshness: status.freshness,
                probe_in_flight: status.probe_in_flight,
                consecutive_failures: status.consecutive_failures,
                recovery_successes: status.recovery_successes,
                latency_ewma: status.latency_ewma,
                last_latency_observation: status.last_latency_observation,
                last_latency_observation_source: status.last_latency_observation_source,
                load: status.load,
                last_observation: status.last_observation,
                last_observation_source: status.last_observation_source,
                last_error: status.last_error.map(str::to_string),
                last_error_at: status.last_error_at,
                last_selection_reason: status.last_selection_reason,
                last_selected_at: status.last_selected_at,
                counters: status.counters,
            });
        }
        Ok(GatewayRuntimeSnapshot {
            now,
            generation: state.balancer.generation(),
            strategy: state.balancer.strategy(),
            manual_member,
            probe: state.balancer.probe_policy().cloned(),
            members,
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, ClientGatewayState>, RuntimeError> {
        self.inner
            .state
            .lock()
            .map_err(|_| RuntimeError::GatewayStatePoisoned)
    }

    fn now(&self) -> GatewayInstant {
        GatewayInstant::from_millis(bounded_millis(self.inner.origin.elapsed()))
    }
}

impl GatewayFlowLease {
    pub(in crate::runtime) fn is_pending(&self) -> bool {
        self.phase == GatewayFlowPhase::Pending
    }

    pub(in crate::runtime) fn opened(&mut self) -> Result<(), RuntimeError> {
        if self.phase != GatewayFlowPhase::Pending {
            return Ok(());
        }
        let now = self.runtime.now();
        let latency = Duration::from_millis(bounded_millis(self.selected_at.elapsed()));
        let mut state = self.runtime.lock()?;
        let mut load = state
            .balancer
            .member_status(self.handle, now)
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?
            .load;
        load.pending_flows = adjust_counter(load.pending_flows, false)?;
        load.active_flows = adjust_counter(load.active_flows, true)?;
        state
            .balancer
            .set_load(self.handle, load)
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
        // Set the ownership phase before passive observation so even an
        // impossible foreign-handle error cannot double-decrement pending.
        self.phase = GatewayFlowPhase::Active;
        state
            .balancer
            .observe_outcome(
                self.handle,
                now,
                GatewayObservationSource::PassiveOpen,
                GatewayOutcome::Success {
                    latency: Some(latency),
                },
                None,
            )
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
        Ok(())
    }

    pub(in crate::runtime) fn failed(
        &mut self,
        error: impl Into<String>,
    ) -> Result<(), RuntimeError> {
        if self.phase != GatewayFlowPhase::Pending {
            return Ok(());
        }
        let now = self.runtime.now();
        let mut state = self.runtime.lock()?;
        adjust_pending(&mut state.balancer, self.handle, now, false)?;
        self.phase = GatewayFlowPhase::Complete;
        state
            .balancer
            .observe_outcome(
                self.handle,
                now,
                GatewayObservationSource::PassiveOpen,
                GatewayOutcome::Failure,
                Some(error.into()),
            )
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
        Ok(())
    }

    pub(in crate::runtime) fn completed(
        &mut self,
        error: Option<String>,
    ) -> Result<(), RuntimeError> {
        if self.phase != GatewayFlowPhase::Active {
            return Ok(());
        }
        let now = self.runtime.now();
        let mut state = self.runtime.lock()?;
        adjust_active(&mut state.balancer, self.handle, now, false)?;
        self.phase = GatewayFlowPhase::Complete;
        let outcome = if error.is_some() {
            GatewayOutcome::Failure
        } else {
            GatewayOutcome::Success { latency: None }
        };
        state
            .balancer
            .observe_outcome(
                self.handle,
                now,
                GatewayObservationSource::PassiveFlow,
                outcome,
                error,
            )
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))
    }
}

impl GatewayProbeLease {
    pub(in crate::runtime) fn succeeded(&mut self) -> Result<(), RuntimeError> {
        self.finish(Ok(()))
    }

    pub(in crate::runtime) fn failed(
        &mut self,
        error: impl Into<String>,
    ) -> Result<(), RuntimeError> {
        self.finish(Err(error.into()))
    }

    fn finish(&mut self, outcome: Result<(), String>) -> Result<(), RuntimeError> {
        if self.complete {
            return Ok(());
        }
        let now = self.runtime.now();
        let latency = Duration::from_millis(bounded_millis(self.started_at.elapsed()));
        let mut state = self.runtime.lock()?;
        let (outcome, error) = match outcome {
            Ok(()) => (
                GatewayOutcome::Success {
                    latency: Some(latency),
                },
                None,
            ),
            Err(error) => (GatewayOutcome::Failure, Some(error)),
        };
        self.complete = true;
        state
            .balancer
            .observe_outcome(
                self.handle,
                now,
                GatewayObservationSource::ActiveProbe,
                outcome,
                error,
            )
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))
    }
}

impl Drop for GatewayProbeLease {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        let Ok(mut state) = self.runtime.inner.state.lock() else {
            return;
        };
        let _ = state.balancer.cancel_active_probe(self.handle);
    }
}

impl Drop for GatewayFlowLease {
    fn drop(&mut self) {
        let now = self.runtime.now();
        let Ok(mut state) = self.runtime.inner.state.lock() else {
            return;
        };
        match self.phase {
            GatewayFlowPhase::Pending => {
                let _ = adjust_pending(&mut state.balancer, self.handle, now, false);
                if self.recovery_probe {
                    let _ = state.balancer.cancel_recovery_probe(self.handle);
                }
            }
            GatewayFlowPhase::Active => {
                let _ = adjust_active(&mut state.balancer, self.handle, now, false);
            }
            GatewayFlowPhase::Complete => {}
        }
        self.phase = GatewayFlowPhase::Complete;
    }
}

fn adjust_pending(
    balancer: &mut GatewayBalancer,
    handle: GatewayMemberHandle,
    now: GatewayInstant,
    increment: bool,
) -> Result<(), RuntimeError> {
    let mut load = balancer
        .member_status(handle, now)
        .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?
        .load;
    load.pending_flows = adjust_counter(load.pending_flows, increment)?;
    balancer
        .set_load(handle, load)
        .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))
}

fn adjust_active(
    balancer: &mut GatewayBalancer,
    handle: GatewayMemberHandle,
    now: GatewayInstant,
    increment: bool,
) -> Result<(), RuntimeError> {
    let mut load = balancer
        .member_status(handle, now)
        .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?
        .load;
    load.active_flows = adjust_counter(load.active_flows, increment)?;
    balancer
        .set_load(handle, load)
        .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))
}

fn adjust_counter(value: u32, increment: bool) -> Result<u32, RuntimeError> {
    if increment {
        value.checked_add(1).ok_or_else(|| {
            RuntimeError::ProductPolicy("balancer flow counter overflow".to_string())
        })
    } else {
        value.checked_sub(1).ok_or_else(|| {
            RuntimeError::ProductPolicy("balancer flow counter underflow".to_string())
        })
    }
}

fn bounded_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
#[path = "tests_gateway.rs"]
mod tests;
